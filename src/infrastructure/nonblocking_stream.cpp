#include "nexstream/publisher/infrastructure/nonblocking_stream.hpp"
#include <algorithm>
#include <array>
#include <arpa/inet.h>
#include <cerrno>
#include <climits>
#include <poll.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <sys/socket.h>

namespace nexstream::publisher::infrastructure {
NonblockingStream::NonblockingStream(UniqueFd fd, StreamConfig config)
    : fd_(std::move(fd)), config_(config),
      decoder_(config.max_frame_bytes, config.max_receive_buffer_bytes) {}

NonblockingStream::~NonblockingStream() {
    if (ssl_) SSL_free(reinterpret_cast<SSL*>(ssl_));
    if (ssl_context_) SSL_CTX_free(reinterpret_cast<SSL_CTX*>(ssl_context_));
}
NonblockingStream::NonblockingStream(NonblockingStream&& other) noexcept
    : fd_(std::move(other.fd_)), config_(other.config_), decoder_(std::move(other.decoder_)),
      pending_frames_(std::move(other.pending_frames_)), stats_(other.stats_),
      ssl_context_(other.ssl_context_), ssl_(other.ssl_) {
    other.ssl_context_ = nullptr; other.ssl_ = nullptr;
}
NonblockingStream& NonblockingStream::operator=(NonblockingStream&& other) noexcept {
    if (this == &other) return *this;
    if (ssl_) SSL_free(reinterpret_cast<SSL*>(ssl_));
    if (ssl_context_) SSL_CTX_free(reinterpret_cast<SSL_CTX*>(ssl_context_));
    fd_ = std::move(other.fd_); config_ = other.config_; decoder_ = std::move(other.decoder_);
    pending_frames_ = std::move(other.pending_frames_); stats_ = other.stats_;
    ssl_context_ = other.ssl_context_; ssl_ = other.ssl_;
    other.ssl_context_ = nullptr; other.ssl_ = nullptr; return *this;
}

TransportError NonblockingStream::enable_tls(const std::string& hostname, const std::string& ca_file,
                                             Deadline deadline) {
    if (hostname.empty() || ca_file.empty() || ssl_ || ssl_context_) return TransportError::kProtocol;
    SSL_CTX* context = SSL_CTX_new(TLS_client_method());
    if (!context) return TransportError::kIo;
    ssl_context_ = reinterpret_cast<ssl_ctx_st*>(context);
    SSL_CTX_set_min_proto_version(context, TLS1_3_VERSION);
    SSL_CTX_set_max_proto_version(context, TLS1_3_VERSION);
    SSL_CTX_set_verify(context, SSL_VERIFY_PEER, nullptr);
    if (SSL_CTX_load_verify_locations(context, ca_file.c_str(), nullptr) != 1) return TransportError::kConnect;
    SSL* tls = SSL_new(context); if (!tls) return TransportError::kIo;
    ssl_ = reinterpret_cast<ssl_st*>(tls);
    if (SSL_set_fd(tls, fd_.get()) != 1 || SSL_set_tlsext_host_name(tls, hostname.c_str()) != 1 ||
        SSL_set1_host(tls, hostname.c_str()) != 1) return TransportError::kConnect;
    while (std::chrono::steady_clock::now() < deadline) {
        const int result = SSL_connect(tls);
        if (result == 1) return SSL_get_verify_result(tls) == X509_V_OK
            ? TransportError::kNone : TransportError::kConnect;
        const int error = SSL_get_error(tls, result);
        if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) {
            const auto wait_result = wait(error == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT, deadline);
            if (wait_result != TransportError::kNone) return wait_result;
            continue;
        }
        return TransportError::kConnect;
    }
    return TransportError::kTimeout;
}

TransportError NonblockingStream::wait(short events, Deadline deadline) {
    while (true) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) return TransportError::kTimeout;
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        const auto timeout = static_cast<int>(std::min<long long>(
            std::max<long long>(1, remaining.count()), INT_MAX));
        pollfd descriptor{fd_.get(), events, 0};
        const int result = ::poll(&descriptor, 1, timeout);
        if (result > 0) {
            if ((descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
                if ((descriptor.revents & events) == 0) return TransportError::kDisconnected;
            }
            if ((descriptor.revents & events) != 0) return TransportError::kNone;
            continue;
        }
        if (result == 0) return TransportError::kTimeout;
        if (errno == EINTR) continue;
        return TransportError::kIo;
    }
}

std::optional<NonblockingStream> NonblockingStream::connect_ipv4(
    const std::string& address, std::uint16_t port, Deadline deadline,
    TransportError& error, StreamConfig config) {
    UniqueFd fd(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!fd) { error = TransportError::kConnect; return std::nullopt; }
    if (config.test_socket_send_buffer_bytes > 0) {
        const int size = config.test_socket_send_buffer_bytes;
        ::setsockopt(fd.get(), SOL_SOCKET, SO_SNDBUF, &size, sizeof(size));
    }
    sockaddr_in endpoint{}; endpoint.sin_family = AF_INET; endpoint.sin_port = htons(port);
    if (::inet_pton(AF_INET, address.c_str(), &endpoint.sin_addr) != 1) {
        error = TransportError::kConnect; return std::nullopt;
    }
    const int result = ::connect(fd.get(), reinterpret_cast<sockaddr*>(&endpoint), sizeof(endpoint));
    NonblockingStream stream(std::move(fd), config);
    if (result == 0) { error = TransportError::kNone; return stream; }
    if (errno != EINPROGRESS) { error = TransportError::kConnect; return std::nullopt; }
    error = stream.wait(POLLOUT, deadline);
    if (error != TransportError::kNone) return std::nullopt;
    int socket_error = 0; socklen_t size = sizeof(socket_error);
    if (::getsockopt(stream.fd(), SOL_SOCKET, SO_ERROR, &socket_error, &size) < 0 ||
        socket_error != 0) {
        error = TransportError::kConnect; return std::nullopt;
    }
    return stream;
}

TransportError NonblockingStream::send_all(std::span<const std::byte> bytes, Deadline deadline) {
    if (bytes.size() > config_.max_write_bytes) return TransportError::kProtocol;
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        if (std::chrono::steady_clock::now() >= deadline) return TransportError::kTimeout;
        auto request = bytes.size() - offset;
        if (config_.test_write_chunk_bytes > 0)
            request = std::min(request, config_.test_write_chunk_bytes);
        ++stats_.send_calls;
        ssize_t count = 0;
        if (ssl_) {
            std::size_t written = 0; const int result = SSL_write_ex(reinterpret_cast<SSL*>(ssl_),
                bytes.data() + offset, request, &written);
            if (result == 1) count = static_cast<ssize_t>(written);
            else {
                const int error = SSL_get_error(reinterpret_cast<SSL*>(ssl_), result);
                if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) {
                    ++stats_.send_eagain;
                    const auto status = wait(error == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT, deadline);
                    if (status != TransportError::kNone) return status;
                    continue;
                }
                return error == SSL_ERROR_ZERO_RETURN ? TransportError::kDisconnected : TransportError::kIo;
            }
        } else count = ::send(fd_.get(), bytes.data() + offset, request, MSG_NOSIGNAL);
        if (count > 0) { offset += static_cast<std::size_t>(count); continue; }
        if (count < 0 && errno == EINTR) continue;
        if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            ++stats_.send_eagain;
            const auto status = wait(POLLOUT, deadline);
            if (status != TransportError::kNone) return status;
            continue;
        }
        return count == 0 ? TransportError::kDisconnected : TransportError::kIo;
    }
    return TransportError::kNone;
}

ReceiveResult NonblockingStream::receive_frame(Deadline deadline) {
    if (!pending_frames_.empty()) {
        auto frame = std::move(pending_frames_.front()); pending_frames_.pop_front();
        return {std::move(frame), TransportError::kNone};
    }
    std::array<std::byte, 16'384> buffer{};
    while (true) {
        if (std::chrono::steady_clock::now() >= deadline) return {{}, TransportError::kTimeout};
        ++stats_.recv_calls;
        ssize_t count = 0;
        if (ssl_) {
            std::size_t received = 0; const int result = SSL_read_ex(reinterpret_cast<SSL*>(ssl_),
                buffer.data(), buffer.size(), &received);
            if (result == 1) count = static_cast<ssize_t>(received);
            else {
                const int error = SSL_get_error(reinterpret_cast<SSL*>(ssl_), result);
                if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) {
                    ++stats_.recv_eagain;
                    const auto status = wait(error == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT, deadline);
                    if (status != TransportError::kNone) return {{}, status};
                    continue;
                }
                return {{}, error == SSL_ERROR_ZERO_RETURN ? TransportError::kDisconnected
                                                            : TransportError::kIo};
            }
        } else count = ::recv(fd_.get(), buffer.data(), buffer.size(), 0);
        if (count > 0) {
            auto result = decoder_.feed(std::span<const std::byte>(
                buffer.data(), static_cast<std::size_t>(count)));
            if (result.error != FrameError::kNone) return {{}, TransportError::kProtocol};
            for (auto& frame : result.frames) pending_frames_.push_back(std::move(frame));
            if (!pending_frames_.empty()) {
                auto frame = std::move(pending_frames_.front()); pending_frames_.pop_front();
                return {std::move(frame), TransportError::kNone};
            }
            continue;
        }
        if (count == 0) return {{}, TransportError::kDisconnected};
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            ++stats_.recv_eagain;
            const auto status = wait(POLLIN, deadline);
            if (status != TransportError::kNone) return {{}, status};
            continue;
        }
        return {{}, TransportError::kIo};
    }
}
}  // namespace nexstream::publisher::infrastructure
