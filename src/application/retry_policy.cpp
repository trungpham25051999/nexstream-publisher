#include "nexstream/publisher/application/retry_policy.hpp"
#include <algorithm>
#include <limits>
#include <stdexcept>

namespace nexstream::publisher::application {
RetryPolicy::RetryPolicy(std::chrono::milliseconds base, std::chrono::milliseconds maximum,
                         std::uint64_t seed)
    : base_(base), maximum_(maximum), random_(seed) {
    if (base_.count() <= 0 || maximum_ < base_) throw std::invalid_argument("invalid retry policy");
}
std::chrono::milliseconds RetryPolicy::next_failure_delay() {
    const auto shift = std::min<std::uint32_t>(failures_, 20); ++failures_;
    const auto factor = std::uint64_t{1} << shift;
    const auto capped = std::min<std::uint64_t>(static_cast<std::uint64_t>(maximum_.count()),
        static_cast<std::uint64_t>(base_.count()) * factor);
    std::uniform_int_distribution<std::uint64_t> distribution(capped / 2, capped);
    return std::chrono::milliseconds(distribution(random_));
}
std::chrono::milliseconds RetryPolicy::normal_jitter(std::chrono::milliseconds maximum) {
    if (maximum.count() <= 0) return std::chrono::milliseconds::zero();
    std::uniform_int_distribution<long long> distribution(0, maximum.count());
    return std::chrono::milliseconds(distribution(random_));
}
}  // namespace nexstream::publisher::application
