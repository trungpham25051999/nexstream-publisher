#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>
#include <unistd.h>
#include "nexstream/publisher/infrastructure/durable_queue.hpp"
#include "nexstream/publisher/infrastructure/linux_collector.hpp"

int main() {
    using Clock = std::chrono::steady_clock;
    nexstream::publisher::infrastructure::LinuxSystemCollector collector;
    (void)collector.collect("baseline"); std::this_thread::sleep_for(std::chrono::milliseconds(10));
    constexpr int iterations = 1'000; int valid = 0; const auto start = Clock::now();
    for (int i = 0; i < iterations; ++i)
        if (collector.collect("probe-" + std::to_string(i))) ++valid;
    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start);

    const auto path = std::filesystem::temp_directory_path() /
        ("nexstream-s06-measure-" + std::to_string(::getpid()) + ".db");
    nexstream::publisher::infrastructure::DurableQueue queue(path);
    for (int i = 0; i < 1'000; ++i) {
        nexstream::publisher::domain::TelemetryRecord record{"measure-" + std::to_string(i),
            1'800'000'000'000 + i, 10.0, 1024, 2048};
        if (!queue.enqueue("measure-host", record)) return 2;
    }
    const auto stats = queue.stats();
    std::cout << "collector_iterations=" << iterations << " valid_delta_samples=" << valid
              << " collector_total_us=" << elapsed.count()
              << " collector_mean_us=" << static_cast<double>(elapsed.count()) / iterations << '\n';
    std::cout << "sqlite_records=" << stats.pending << " sqlite_disk_bytes=" << stats.disk_bytes << '\n';
    std::error_code error; std::filesystem::remove(path, error);
    std::filesystem::remove(path.string()+"-wal", error); std::filesystem::remove(path.string()+"-shm", error);
    return 0;
}
