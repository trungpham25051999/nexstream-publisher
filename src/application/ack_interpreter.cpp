#include "nexstream/publisher/application/publish_outcome.hpp"
#include <unordered_map>
#include <unordered_set>

namespace nexstream::publisher::application {
namespace transport = nexstream::transport::v1;

PublishOutcome retry_all(const std::vector<std::string>& ids, std::string detail) {
    PublishOutcome outcome; outcome.status = TransactionStatus::kRetry;
    outcome.detail = std::move(detail);
    for (const auto& id : ids) outcome.records.push_back({id, transport::UNKNOWN, true});
    return outcome;
}

PublishOutcome interpret_ack(const transport::BatchAck& ack, const std::string& expected_batch_id,
                             const std::vector<std::string>& expected_ids) {
    if (ack.batch_id() != expected_batch_id ||
        ack.message_results_size() != static_cast<int>(expected_ids.size()) ||
        !transport::ResultCode_IsValid(static_cast<int>(ack.overall_result())) ||
        ack.overall_result() == transport::RESULT_CODE_UNSPECIFIED) {
        auto result = retry_all(expected_ids, "invalid or mismatched BatchAck");
        result.status = TransactionStatus::kProtocolFailure;
        return result;
    }
    std::unordered_set<std::string> expected(expected_ids.begin(), expected_ids.end());
    std::unordered_map<std::string, transport::ResultCode> received;
    for (const auto& item : ack.message_results()) {
        if (!expected.contains(item.message_id()) || received.contains(item.message_id()) ||
            !transport::ResultCode_IsValid(static_cast<int>(item.code())) ||
            item.code() == transport::RESULT_CODE_UNSPECIFIED) {
            auto result = retry_all(expected_ids, "invalid message result in BatchAck");
            result.status = TransactionStatus::kProtocolFailure;
            return result;
        }
        received.emplace(item.message_id(), item.code());
    }

    PublishOutcome outcome;
    bool any_retry = false;
    bool any_permanent = false;
    bool all_success = true;
    const bool aggregate_uncertain = ack.overall_result() == transport::UNKNOWN ||
        ack.overall_result() == transport::RETRYABLE_FAILURE ||
        ack.overall_result() == transport::RETRY_LATER;
    for (const auto& id : expected_ids) {
        const auto code = received.at(id);
        const bool retry = aggregate_uncertain || code == transport::UNKNOWN ||
            code == transport::RETRYABLE_FAILURE || code == transport::RETRY_LATER;
        any_retry = any_retry || retry;
        any_permanent = any_permanent || code == transport::PERMANENT_FAILURE;
        all_success = all_success && code == transport::SUCCESS;
        outcome.records.push_back({id, code, retry});
    }
    if (ack.overall_result() == transport::SUCCESS && !all_success) {
        auto result = retry_all(expected_ids, "SUCCESS aggregate conflicts with record results");
        result.status = TransactionStatus::kProtocolFailure;
        return result;
    }
    if (all_success && ack.overall_result() == transport::SUCCESS) {
        outcome.status = TransactionStatus::kSuccess;
        outcome.detail = "delivery success";
    } else if (any_retry) {
        outcome.status = TransactionStatus::kRetry;
        outcome.detail = "delivery unresolved or retryable";
    } else if (any_permanent || ack.overall_result() == transport::PERMANENT_FAILURE) {
        outcome.status = TransactionStatus::kPermanentFailure;
        outcome.detail = "permanent delivery failure";
    } else {
        outcome.status = TransactionStatus::kProtocolFailure;
        outcome.detail = "inconsistent BatchAck";
    }
    return outcome;
}
}  // namespace nexstream::publisher::application
