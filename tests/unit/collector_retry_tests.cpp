#include <chrono>
#include <thread>
#include <algorithm>
#include "nexstream/publisher/application/retry_policy.hpp"
#include "nexstream/publisher/infrastructure/linux_collector.hpp"
#include "test_harness.hpp"

using namespace std::chrono_literals;
int main() {
    TestHarness test; nexstream::publisher::infrastructure::LinuxSystemCollector collector;
    (void)collector.collect(nexstream::publisher::infrastructure::generate_message_id());
    std::this_thread::sleep_for(20ms);
    const auto sample = collector.collect(nexstream::publisher::infrastructure::generate_message_id());
    test.expect(sample.has_value(), "real /proc CPU/RAM sample collected after baseline");
    test.expect(sample && sample->cpu_usage_percent >= 0.0 && sample->cpu_usage_percent <= 100.0 &&
                    sample->memory_used_bytes <= sample->memory_total_bytes,
                "real collector values satisfy contract bounds");
    const auto extensions = collector.extension_status();
    test.expect(extensions.size() == 3 &&
                    extensions[0].status == nexstream::publisher::infrastructure::CollectorStatus::kUnsupported,
                "GPU/Disk/Network adapters report UNSUPPORTED without fake data");

    nexstream::publisher::application::RetryPolicy retry(100ms, 800ms, 42);
    auto previous_upper = 100ms;
    for (int i = 0; i < 6; ++i) {
        const auto delay = retry.next_failure_delay();
        test.expect(delay >= previous_upper / 2 && delay <= std::min(previous_upper, 800ms),
                    "exponential retry delay is jittered and capped");
        previous_upper = std::min(previous_upper * 2, 800ms);
    }
    retry.reset(); test.expect(retry.next_failure_delay() <= 100ms, "success reset returns backoff to base");
    test.expect(retry.normal_jitter(250ms) <= 250ms, "normal publish jitter is bounded");
    return test.finish();
}
