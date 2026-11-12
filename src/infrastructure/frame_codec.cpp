#include "nexstream/publisher/infrastructure/frame_codec.hpp"
#include <algorithm>
#include <stdexcept>

namespace nexstream::publisher::infrastructure {
FrameDecoder::FrameDecoder(std::size_t max_frame, std::size_t max_buffer)
    : max_frame_bytes_(max_frame), max_buffer_bytes_(max_buffer) {
    if (max_frame == 0 || max_buffer < max_frame + 4) throw std::invalid_argument("invalid limits");
}
FrameFeedResult FrameDecoder::feed(std::span<const std::byte> bytes) {
    FrameFeedResult result;
    if (failed_) { result.error = FrameError::kMalformedProtobuf; return result; }
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        if (expected_ == 0) {
            const auto count = std::min<std::size_t>(4 - header_size_, bytes.size() - offset);
            std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(offset), count,
                        header_.begin() + static_cast<std::ptrdiff_t>(header_size_));
            header_size_ += count; offset += count;
            if (header_size_ < 4) break;
            expected_ = (std::to_integer<std::uint32_t>(header_[0]) << 24U) |
                        (std::to_integer<std::uint32_t>(header_[1]) << 16U) |
                        (std::to_integer<std::uint32_t>(header_[2]) << 8U) |
                        std::to_integer<std::uint32_t>(header_[3]);
            if (expected_ == 0 || expected_ > max_frame_bytes_) {
                failed_ = true; result.error = FrameError::kInvalidLength; return result;
            }
            if (static_cast<std::size_t>(expected_) + 4 > max_buffer_bytes_) {
                failed_ = true; result.error = FrameError::kBufferLimit; return result;
            }
            payload_.clear(); payload_.reserve(expected_);
        }
        const auto count = std::min<std::size_t>(expected_ - payload_.size(), bytes.size() - offset);
        payload_.insert(payload_.end(), bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                        bytes.begin() + static_cast<std::ptrdiff_t>(offset + count));
        offset += count;
        if (payload_.size() != expected_) continue;
        nexstream::transport::v1::Frame frame;
        if (!frame.ParseFromArray(payload_.data(), static_cast<int>(payload_.size()))) {
            failed_ = true; result.error = FrameError::kMalformedProtobuf; return result;
        }
        result.frames.push_back(std::move(frame));
        header_size_ = 0; expected_ = 0; payload_.clear();
    }
    return result;
}
std::vector<std::byte> encode_frame(const nexstream::transport::v1::Frame& frame,
                                    std::size_t max_frame_bytes) {
    const auto size = frame.ByteSizeLong();
    if (size == 0 || size > max_frame_bytes) throw std::length_error("frame exceeds limit");
    std::vector<std::byte> output(size + 4);
    const auto value = static_cast<std::uint32_t>(size);
    output[0] = static_cast<std::byte>(value >> 24U);
    output[1] = static_cast<std::byte>(value >> 16U);
    output[2] = static_cast<std::byte>(value >> 8U);
    output[3] = static_cast<std::byte>(value);
    if (!frame.SerializeToArray(output.data() + 4, static_cast<int>(size)))
        throw std::runtime_error("serialization failed");
    return output;
}
}  // namespace nexstream::publisher::infrastructure
