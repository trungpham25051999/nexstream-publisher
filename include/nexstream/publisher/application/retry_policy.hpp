#pragma once
#include <chrono>
#include <cstdint>
#include <random>

namespace nexstream::publisher::application {
class RetryPolicy {
  public:
    RetryPolicy(std::chrono::milliseconds base = std::chrono::milliseconds(250),
                std::chrono::milliseconds maximum = std::chrono::seconds(30),
                std::uint64_t seed = std::random_device{}());
    [[nodiscard]] std::chrono::milliseconds next_failure_delay();
    [[nodiscard]] std::chrono::milliseconds normal_jitter(std::chrono::milliseconds maximum);
    void reset() noexcept { failures_ = 0; }
  private:
    std::chrono::milliseconds base_;
    std::chrono::milliseconds maximum_;
    std::uint32_t failures_{0};
    std::mt19937_64 random_;
};
}  // namespace nexstream::publisher::application
