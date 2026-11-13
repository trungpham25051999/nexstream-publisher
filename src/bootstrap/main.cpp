#include <charconv>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string_view>
#include "nexstream/publisher/infrastructure/tcp_publisher_client.hpp"

int main(int argc, char** argv) {
    std::string address = "127.0.0.1";
    std::uint16_t port = 9000;
    if (argc == 5 && std::string_view(argv[1]) == "--host" &&
        std::string_view(argv[3]) == "--port") {
        address = argv[2];
        unsigned int parsed = 0;
        const std::string_view value(argv[4]);
        const auto conversion = std::from_chars(value.data(), value.data() + value.size(), parsed);
        if (conversion.ec != std::errc{} || conversion.ptr != value.data() + value.size() ||
            parsed > 65'535) {
            std::cerr << "invalid port\n"; return 2;
        }
        port = static_cast<std::uint16_t>(parsed);
    } else if (argc != 1) {
        std::cerr << "usage: nexstream-publisher [--host IPv4 --port PORT]\n"; return 2;
    }

    nexstream::publisher::domain::Batch batch;
    batch.agent_id = "publisher-cli-fixture"; batch.host_id = "publisher-cli-host";
    batch.key_id = "test-key"; batch.secret = "test-secret"; batch.batch_id = "cli-batch-01";
    const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    batch.records.push_back({"cli-message-01", now, 12.5, 4'096, 8'192});

    nexstream::publisher::infrastructure::PublisherConfig config;
    config.address = address; config.port = port;
    nexstream::publisher::infrastructure::TcpPublisherClient client(config);
    const auto outcome = client.publish(batch);
    std::cout << outcome.detail << '\n';
    using Status = nexstream::publisher::application::TransactionStatus;
    if (outcome.status == Status::kSuccess) return 0;
    if (outcome.status == Status::kRetry) return 3;
    if (outcome.status == Status::kPermanentFailure) return 4;
    return 5;
}
