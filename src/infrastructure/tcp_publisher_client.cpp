#include "nexstream/publisher/infrastructure/tcp_publisher_client.hpp"
#include <cmath>
#include <unordered_set>
#include <utility>
#include "nexstream/publisher/infrastructure/frame_codec.hpp"

namespace nexstream::publisher::infrastructure {
namespace transport = nexstream::transport::v1;

namespace {
std::vector<std::string> ids_of(const domain::Batch& batch) {
    std::vector<std::string> ids; ids.reserve(batch.records.size());
    for (const auto& record : batch.records) ids.push_back(record.message_id);
    return ids;
}
application::PublishOutcome protocol_failure(const domain::Batch& batch, std::string detail) {
    auto result = application::retry_all(ids_of(batch), std::move(detail));
    result.status = application::TransactionStatus::kProtocolFailure;
    return result;
}
transport::Frame auth_frame(const domain::Batch& batch) {
    transport::Frame frame; frame.set_protocol_version(1);
    auto* auth = frame.mutable_auth_request();
    auth->set_agent_id(batch.agent_id); auth->set_host_id(batch.host_id);
    auth->set_key_id(batch.key_id); auth->set_secret(batch.secret); return frame;
}
}  // namespace

TcpPublisherClient::TcpPublisherClient(PublisherConfig config) : config_(std::move(config)) {}

application::PublishOutcome TcpPublisherClient::validate_batch(const domain::Batch& batch) const {
    const auto ids = ids_of(batch);
    auto invalid = [&](std::string detail) { return protocol_failure(batch, std::move(detail)); };
    if (batch.records.empty() || batch.records.size() > config_.max_samples)
        return invalid("batch sample count outside bounds");
    if (batch.agent_id.empty() || batch.host_id.empty() || batch.key_id.empty() ||
        batch.secret.empty() || batch.batch_id.empty()) return invalid("required value missing");
    if (batch.agent_id.size() > config_.max_identifier_bytes ||
        batch.host_id.size() > config_.max_identifier_bytes ||
        batch.key_id.size() > config_.max_identifier_bytes ||
        batch.batch_id.size() > config_.max_identifier_bytes ||
        batch.secret.size() > config_.max_secret_bytes) return invalid("identifier/secret too long");
    std::unordered_set<std::string> unique;
    for (const auto& record : batch.records) {
        if (record.message_id.empty() || record.message_id.size() > config_.max_identifier_bytes ||
            !unique.insert(record.message_id).second || record.sample_timestamp_ms <= 0 ||
            !std::isfinite(record.cpu_usage_percent) || record.cpu_usage_percent < 0.0 ||
            record.cpu_usage_percent > 100.0 ||
            record.memory_used_bytes > record.memory_total_bytes)
            return invalid("invalid telemetry record");
    }
    application::PublishOutcome valid;
    valid.status = application::TransactionStatus::kSuccess;
    valid.detail = "valid";
    for (const auto& id : ids) valid.records.push_back({id, transport::UNKNOWN, true});
    return valid;
}

application::PublishOutcome TcpPublisherClient::publish(const domain::Batch& batch) const {
    const auto ids = ids_of(batch);
    const auto validation = validate_batch(batch);
    if (validation.detail != "valid") return validation;
    const auto deadline = std::chrono::steady_clock::now() + config_.transaction_timeout;
    TransportError transport_error = TransportError::kNone;
    auto stream = NonblockingStream::connect_ipv4(config_.address, config_.port, deadline,
                                                   transport_error, config_.stream);
    if (!stream) return application::retry_all(ids, "connect failed or timed out");
    if (config_.tls_enabled) {
        transport_error = stream->enable_tls(config_.tls_server_hostname, config_.tls_ca_file, deadline);
        if (transport_error != TransportError::kNone)
            return application::retry_all(ids, "TLS verification/handshake failed or timed out");
    }

    auto auth_wire = encode_frame(auth_frame(batch), config_.max_frame_bytes);
    transport_error = stream->send_all(auth_wire, deadline);
    if (transport_error != TransportError::kNone)
        return application::retry_all(ids, "AuthRequest send unresolved");
    auto received = stream->receive_frame(deadline);
    if (!received.frame) return application::retry_all(ids, "AuthResult receive unresolved");
    if (received.frame->protocol_version() != 1 || !received.frame->has_auth_result())
        return protocol_failure(batch, "valid v1 AuthResult required");
    const auto auth_code = received.frame->auth_result().code();
    if (!transport::AuthCode_IsValid(static_cast<int>(auth_code)) ||
        auth_code == transport::AUTH_CODE_UNSPECIFIED)
        return protocol_failure(batch, "invalid AuthResult code");
    if (auth_code != transport::AUTHENTICATED) {
        auto result = application::retry_all(ids, "authentication failed");
        if (auth_code == transport::INVALID_CREDENTIALS || auth_code == transport::HOST_NOT_AUTHORIZED) {
            result.status = application::TransactionStatus::kPermanentFailure;
            for (auto& record : result.records) { record.retry = false; record.code = transport::PERMANENT_FAILURE; }
        }
        return result;
    }

    std::vector<std::byte> batch_wire;
    batch_wire.reserve(std::min<std::size_t>(config_.max_batch_bytes, 16'384));
    auto append = [&](const transport::Frame& frame) -> bool {
        auto wire = encode_frame(frame, config_.max_frame_bytes);
        if (wire.size() > config_.max_batch_bytes - batch_wire.size()) return false;
        batch_wire.insert(batch_wire.end(), wire.begin(), wire.end());
        return true;
    };
    transport::Frame start; start.set_protocol_version(1);
    auto* start_value = start.mutable_batch_start(); start_value->set_batch_id(batch.batch_id);
    start_value->set_host_id(batch.host_id);
    start_value->set_expected_count(batch.expected_count_unspecified
        ? 0U : static_cast<std::uint32_t>(batch.records.size()));
    if (!append(start)) return protocol_failure(batch, "batch exceeds byte limit");
    for (const auto& record : batch.records) {
        transport::Frame frame; frame.set_protocol_version(1);
        auto* snapshot = frame.mutable_telemetry()->mutable_snapshot();
        snapshot->set_host_id(batch.host_id); snapshot->set_message_id(record.message_id);
        snapshot->set_sample_timestamp_ms(record.sample_timestamp_ms);
        snapshot->set_cpu_usage_percent(record.cpu_usage_percent);
        snapshot->set_memory_used_bytes(record.memory_used_bytes);
        snapshot->set_memory_total_bytes(record.memory_total_bytes);
        if (!append(frame)) return protocol_failure(batch, "batch exceeds byte limit");
    }
    transport::Frame end; end.set_protocol_version(1);
    end.mutable_batch_end()->set_batch_id(batch.batch_id);
    end.mutable_batch_end()->set_sent_count(static_cast<std::uint32_t>(batch.records.size()));
    if (!append(end)) return protocol_failure(batch, "batch exceeds byte limit");

    transport_error = stream->send_all(batch_wire, deadline);
    if (transport_error != TransportError::kNone)
        return application::retry_all(ids, "batch send unresolved");
    received = stream->receive_frame(deadline);
    if (!received.frame) return application::retry_all(ids, "BatchAck timeout/disconnect");
    if (received.frame->protocol_version() != 1)
        return protocol_failure(batch, "invalid response protocol version");
    if (received.frame->has_protocol_error()) {
        auto result = application::retry_all(ids, received.frame->protocol_error().detail());
        if (received.frame->protocol_error().code() == transport::PERMANENT_FAILURE) {
            result.status = application::TransactionStatus::kPermanentFailure;
            for (auto& record : result.records) { record.retry = false; record.code = transport::PERMANENT_FAILURE; }
        }
        return result;
    }
    if (!received.frame->has_batch_ack()) return protocol_failure(batch, "BatchAck required");
    return application::interpret_ack(received.frame->batch_ack(), batch.batch_id, ids);
}
}  // namespace nexstream::publisher::infrastructure
