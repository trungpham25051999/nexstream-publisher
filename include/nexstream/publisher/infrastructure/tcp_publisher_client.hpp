#pragma once
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include "nexstream/publisher/application/publish_outcome.hpp"
#include "nexstream/publisher/application/agent_ports.hpp"
#include "nexstream/publisher/domain/batch.hpp"
#include "nexstream/publisher/infrastructure/nonblocking_stream.hpp"

namespace nexstream::publisher::infrastructure {
struct PublisherConfig {
    std::string address{"127.0.0.1"};
    std::uint16_t port{9000};
    std::chrono::milliseconds transaction_timeout{10'000};
    std::size_t max_frame_bytes{65'536};
    std::size_t max_batch_bytes{1'048'576};
    std::size_t max_samples{50};
    std::size_t max_identifier_bytes{128};
    std::size_t max_secret_bytes{512};
    StreamConfig stream{};
    bool tls_enabled{false};
    std::string tls_server_hostname;
    std::string tls_ca_file;
};

class TcpPublisherClient final : public application::IBatchPublisher {
  public:
    explicit TcpPublisherClient(PublisherConfig config);
    [[nodiscard]] application::PublishOutcome publish(const domain::Batch& batch) const override;
  private:
    [[nodiscard]] application::PublishOutcome validate_batch(const domain::Batch& batch) const;
    PublisherConfig config_;
};
}  // namespace nexstream::publisher::infrastructure
