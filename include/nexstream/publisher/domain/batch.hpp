#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace nexstream::publisher::domain {
struct TelemetryRecord {
    std::string message_id;
    std::int64_t sample_timestamp_ms{0};
    double cpu_usage_percent{0.0};
    std::uint64_t memory_used_bytes{0};
    std::uint64_t memory_total_bytes{0};
};
struct Batch {
    std::string agent_id;
    std::string host_id;
    std::string key_id;
    std::string secret;
    std::string batch_id;
    std::vector<TelemetryRecord> records;
    bool expected_count_unspecified{false};
};
}  // namespace nexstream::publisher::domain
