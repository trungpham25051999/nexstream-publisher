#include <arpa/inet.h>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include "nexstream/publisher/infrastructure/tcp_publisher_client.hpp"
#include "test_harness.hpp"

using namespace std::chrono_literals;
namespace app = nexstream::publisher::application;
namespace domain = nexstream::publisher::domain;
namespace infra = nexstream::publisher::infrastructure;

namespace {
std::uint16_t reserve_port() {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    sockaddr_in endpoint{}; endpoint.sin_family = AF_INET;
    endpoint.sin_addr.s_addr = htonl(INADDR_LOOPBACK); endpoint.sin_port = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&endpoint), sizeof(endpoint)) != 0) return 0;
    socklen_t size = sizeof(endpoint);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&endpoint), &size);
    const auto port = ntohs(endpoint.sin_port); ::close(fd); return port;
}
bool wait_for_gateway(std::uint16_t port) {
    for (int attempt = 0; attempt < 100; ++attempt) {
        const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        sockaddr_in endpoint{}; endpoint.sin_family = AF_INET; endpoint.sin_port = htons(port);
        endpoint.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (::connect(fd, reinterpret_cast<sockaddr*>(&endpoint), sizeof(endpoint)) == 0) {
            ::close(fd); return true;
        }
        ::close(fd); std::this_thread::sleep_for(10ms);
    }
    return false;
}
class ChildProcess {
  public:
    explicit ChildProcess(std::uint16_t port) {
        pid_ = ::fork();
        if (pid_ == 0) {
            const auto port_value = std::to_string(port);
            ::execl(NEXSTREAM_INGEST_BINARY, NEXSTREAM_INGEST_BINARY, "--port",
                    port_value.c_str(), static_cast<char*>(nullptr));
            _exit(127);
        }
    }
    ~ChildProcess() {
        if (pid_ > 0) { ::kill(pid_, SIGTERM); int status = 0; ::waitpid(pid_, &status, 0); }
    }
    bool valid() const { return pid_ > 0; }
  private: pid_t pid_{-1};
};
}  // namespace

int main() {
    TestHarness test;
    const auto port = reserve_port();
    test.expect(port != 0, "ephemeral integration port selected");
    ChildProcess gateway(port);
    test.expect(gateway.valid() && wait_for_gateway(port), "Sprint 02 Gateway started");

    domain::Batch batch;
    batch.agent_id = "publisher-integration"; batch.host_id = "integration-host";
    batch.key_id = "test-key"; batch.secret = "test-secret"; batch.batch_id = "batch-s03";
    batch.expected_count_unspecified = true;
    batch.records.push_back({"stable-message-01", 1'800'000'000'000, 30.0, 1'000, 2'000});
    batch.records.push_back({"stable-message-02", 1'800'000'001'000, 31.0, 1'100, 2'000});

    infra::PublisherConfig config; config.port = port; config.transaction_timeout = 2s;
    config.stream.test_write_chunk_bytes = 3;
    infra::TcpPublisherClient client(config);
    const auto outcome = client.publish(batch);
    test.expect(outcome.status == app::TransactionStatus::kRetry,
                "Gateway Sprint 02 UNKNOWN is not delivery success");
    test.expect(outcome.records.size() == 2 && outcome.records[0].retry && outcome.records[1].retry,
                "UNKNOWN retains both records");
    test.expect(outcome.records[0].message_id == "stable-message-01" &&
                    outcome.records[1].message_id == "stable-message-02",
                "retry preserves stable message identities");

    auto tiny_config = config; tiny_config.max_batch_bytes = 1;
    infra::TcpPublisherClient bounded_client(tiny_config);
    const auto bounded_outcome = bounded_client.publish(batch);
    test.expect(bounded_outcome.status == app::TransactionStatus::kProtocolFailure &&
                    bounded_outcome.records.size() == 2,
                "aggregate batch byte limit rejects without losing identities");

    const auto timeout_port = reserve_port();
    const int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    int one = 1; ::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in endpoint{}; endpoint.sin_family = AF_INET; endpoint.sin_port = htons(timeout_port);
    endpoint.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    test.expect(::bind(listener, reinterpret_cast<sockaddr*>(&endpoint), sizeof(endpoint)) == 0 &&
                    ::listen(listener, 1) == 0,
                "timeout fixture listening");
    std::thread hanging_server([&] {
        const int client_fd = ::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC);
        if (client_fd >= 0) { std::this_thread::sleep_for(250ms); ::close(client_fd); }
    });
    config.port = timeout_port; config.transaction_timeout = 100ms;
    infra::TcpPublisherClient timeout_client(config);
    const auto timeout_outcome = timeout_client.publish(batch);
    hanging_server.join(); ::close(listener);
    test.expect(timeout_outcome.status == app::TransactionStatus::kRetry &&
                    timeout_outcome.records.size() == 2,
                "transaction timeout retains records for retry");
    test.expect(timeout_outcome.records[0].message_id == "stable-message-01" &&
                    timeout_outcome.records[1].message_id == "stable-message-02",
                "timeout preserves original message identities");
    return test.finish();
}
