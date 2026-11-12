#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>
#include "nexstream/transport/v1/transport.pb.h"

namespace nexstream::publisher::infrastructure {
enum class FrameError { kNone, kInvalidLength, kMalformedProtobuf, kBufferLimit };
struct FrameFeedResult {
    std::vector<nexstream::transport::v1::Frame> frames;
    FrameError error{FrameError::kNone};
};
class FrameDecoder {
  public:
    FrameDecoder(std::size_t max_frame_bytes = 65'536, std::size_t max_buffer_bytes = 65'540);
    [[nodiscard]] FrameFeedResult feed(std::span<const std::byte> bytes);
  private:
    std::size_t max_frame_bytes_;
    std::size_t max_buffer_bytes_;
    std::array<std::byte, 4> header_{};
    std::size_t header_size_{0};
    std::uint32_t expected_{0};
    std::vector<std::byte> payload_;
    bool failed_{false};
};
[[nodiscard]] std::vector<std::byte> encode_frame(
    const nexstream::transport::v1::Frame& frame, std::size_t max_frame_bytes = 65'536);
}  // namespace nexstream::publisher::infrastructure
