#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <pthread.h>
#include <sys/socket.h>
#include <thread>
#include <vector>
#include "nexstream/publisher/infrastructure/frame_codec.hpp"
#include "nexstream/publisher/infrastructure/nonblocking_stream.hpp"
#include "test_harness.hpp"

using namespace std::chrono_literals;
using nexstream::publisher::infrastructure::Deadline;
using nexstream::publisher::infrastructure::NonblockingStream;
using nexstream::publisher::infrastructure::TransportError;
using nexstream::publisher::infrastructure::UniqueFd;
using nexstream::publisher::infrastructure::encode_frame;

namespace {
void noop_handler(int) {}
std::pair<UniqueFd, UniqueFd> socket_pair() {
    int values[2]{};
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, values) != 0)
        return {};
    return {UniqueFd(values[0]), UniqueFd(values[1])};
}
nexstream::transport::v1::Frame auth_result() {
    nexstream::transport::v1::Frame frame; frame.set_protocol_version(1);
    frame.mutable_auth_result()->set_code(nexstream::transport::v1::AUTHENTICATED); return frame;
}
}  // namespace

int main() {
    TestHarness test;
    struct sigaction action{}; action.sa_handler = noop_handler; sigemptyset(&action.sa_mask);
    sigaction(SIGUSR1, &action, nullptr);

    {
        auto [sender_fd, reader_fd] = socket_pair();
        test.expect(static_cast<bool>(sender_fd) && static_cast<bool>(reader_fd), "socketpair created");
        NonblockingStream stream(std::move(sender_fd));
        std::array<std::byte, 4096> filler{};
        std::size_t filled = 0;
        while (true) {
            const auto count = ::send(stream.fd(), filler.data(), filler.size(), MSG_NOSIGNAL);
            if (count > 0) { filled += static_cast<std::size_t>(count); continue; }
            if (count < 0 && errno == EINTR) continue;
            test.expect(count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK),
                        "real send EAGAIN reached");
            break;
        }
        std::vector<std::byte> payload(16'384, std::byte{0x5a});
        const auto target = filled + payload.size();
        const auto main_thread = pthread_self();
        std::atomic_size_t received{0};
        std::thread reader([&] {
            std::this_thread::sleep_for(10ms);
            pthread_kill(main_thread, SIGUSR1);
            std::this_thread::sleep_for(40ms);
            std::array<std::byte, 8192> buffer{};
            while (received.load() < target) {
                const auto count = ::recv(reader_fd.get(), buffer.data(), buffer.size(), 0);
                if (count > 0) received.fetch_add(static_cast<std::size_t>(count));
                else if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
                    std::this_thread::yield();
                else if (count < 0 && errno == EINTR) continue;
                else break;
            }
        });
        const auto status = stream.send_all(payload, std::chrono::steady_clock::now() + 2s);
        reader.join();
        test.expect(status == TransportError::kNone && received.load() == target,
                    "partial writes and EINTR/EAGAIN preserve all bytes");
        test.expect(stream.stats().send_eagain > 0 && stream.stats().send_calls > 1,
                    "EAGAIN and multiple send calls observed");
    }

    {
        auto [receiver_fd, peer_fd] = socket_pair();
        NonblockingStream stream(std::move(receiver_fd));
        const auto wire = encode_frame(auth_result());
        std::thread writer([&] {
            for (const auto byte : wire) {
                while (::send(peer_fd.get(), &byte, 1, MSG_NOSIGNAL) < 0 && errno == EINTR) {}
                std::this_thread::sleep_for(1ms);
            }
        });
        const auto result = stream.receive_frame(std::chrono::steady_clock::now() + 2s);
        writer.join();
        test.expect(result.frame && result.frame->has_auth_result(),
                    "fragmented receive reconstructs one frame");
        test.expect(stream.stats().recv_eagain > 0 && stream.stats().recv_calls > 1,
                    "nonblocking receive EAGAIN observed");
    }

    {
        auto [sender_fd, peer_fd] = socket_pair();
        NonblockingStream stream(std::move(sender_fd));
        std::array<std::byte, 4096> filler{};
        while (::send(stream.fd(), filler.data(), filler.size(), MSG_NOSIGNAL) > 0) {}
        const std::array<std::byte, 1> payload{std::byte{1}};
        const auto status = stream.send_all(payload, std::chrono::steady_clock::now() + 50ms);
        test.expect(status == TransportError::kTimeout, "transaction I/O deadline is enforced");
    }

    {
        auto [receiver_fd, peer_fd] = socket_pair();
        NonblockingStream stream(std::move(receiver_fd));
        peer_fd.reset();
        const auto result = stream.receive_frame(std::chrono::steady_clock::now() + 1s);
        test.expect(result.error == TransportError::kDisconnected, "server disconnect is reported");
    }
    {
        auto [receiver_fd, peer_fd] = socket_pair();
        NonblockingStream stream(std::move(receiver_fd));
        const std::array<std::byte, 4> malformed{};
        test.expect(::send(peer_fd.get(), malformed.data(), malformed.size(), MSG_NOSIGNAL) == 4,
                    "malformed server prefix sent");
        const auto result = stream.receive_frame(std::chrono::steady_clock::now() + 1s);
        test.expect(result.error == TransportError::kProtocol,
                    "malformed server framing fails closed");
    }
    return test.finish();
}
