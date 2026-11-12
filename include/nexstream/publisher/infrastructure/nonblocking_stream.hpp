#pragma once
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <span>
#include <string>
#include "nexstream/publisher/infrastructure/frame_codec.hpp"
#include "nexstream/publisher/infrastructure/unique_fd.hpp"

struct ssl_ctx_st;
struct ssl_st;

namespace nexstream::publisher::infrastructure {
using Deadline = std::chrono::steady_clock::time_point;
enum class TransportError { kNone, kTimeout, kConnect, kDisconnected, kIo, kProtocol };
struct IoStats { std::size_t send_calls{0}; std::size_t recv_calls{0};
                 std::size_t send_eagain{0}; std::size_t recv_eagain{0}; };
struct ReceiveResult {
    std::optional<nexstream::transport::v1::Frame> frame;
    TransportError error{TransportError::kNone};
};
struct StreamConfig {
    std::size_t max_frame_bytes{65'536};
    std::size_t max_receive_buffer_bytes{65'540};
    std::size_t max_write_bytes{1'052'000};
    std::size_t test_write_chunk_bytes{0};
    int test_socket_send_buffer_bytes{0};
};

class NonblockingStream {
  public:
    NonblockingStream(UniqueFd fd, StreamConfig config = {});
    ~NonblockingStream();
    NonblockingStream(NonblockingStream&& other) noexcept;
    NonblockingStream& operator=(NonblockingStream&& other) noexcept;
    NonblockingStream(const NonblockingStream&) = delete;
    NonblockingStream& operator=(const NonblockingStream&) = delete;
    static std::optional<NonblockingStream> connect_ipv4(
        const std::string& address, std::uint16_t port, Deadline deadline,
        TransportError& error, StreamConfig config = {});
    [[nodiscard]] TransportError send_all(std::span<const std::byte> bytes, Deadline deadline);
    [[nodiscard]] ReceiveResult receive_frame(Deadline deadline);
    [[nodiscard]] TransportError enable_tls(const std::string& server_hostname,
                                            const std::string& ca_file, Deadline deadline);
    [[nodiscard]] const IoStats& stats() const noexcept { return stats_; }
    [[nodiscard]] int fd() const noexcept { return fd_.get(); }
  private:
    [[nodiscard]] TransportError wait(short events, Deadline deadline);
    UniqueFd fd_;
    StreamConfig config_;
    FrameDecoder decoder_;
    std::deque<nexstream::transport::v1::Frame> pending_frames_;
    IoStats stats_;
    ssl_ctx_st* ssl_context_{nullptr};
    ssl_st* ssl_{nullptr};
};
}  // namespace nexstream::publisher::infrastructure
