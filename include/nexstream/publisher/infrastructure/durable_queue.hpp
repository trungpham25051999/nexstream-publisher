#pragma once
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <string>
#include <vector>
#include "nexstream/publisher/application/publish_outcome.hpp"
#include "nexstream/publisher/application/agent_ports.hpp"
#include "nexstream/publisher/domain/batch.hpp"

struct sqlite3;
namespace nexstream::publisher::infrastructure {
struct DurableQueueConfig {
    std::uint64_t disk_quota_bytes{256 * 1024 * 1024};
    std::uint64_t headroom_bytes{16 * 1024 * 1024};
    std::chrono::hours max_retention{24};
};
struct QueueStats { std::size_t pending{0}; std::size_t quarantined{0}; std::uint64_t disk_bytes{0}; };

class DurableQueue final : public application::IRecordQueue {
  public:
    DurableQueue(std::filesystem::path path, DurableQueueConfig config = {});
    ~DurableQueue();
    DurableQueue(const DurableQueue&) = delete;
    DurableQueue& operator=(const DurableQueue&) = delete;
    [[nodiscard]] bool enqueue(const std::string& host_id, const domain::TelemetryRecord& record) override;
    [[nodiscard]] std::vector<domain::TelemetryRecord> fetch_pending(std::size_t limit) override;
    void apply_outcome(const application::PublishOutcome& outcome) override;
    void enforce_retention(std::int64_t now_ms) override;
    [[nodiscard]] QueueStats stats() const;
    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }
  private:
    void execute(const char* sql) const;
    [[nodiscard]] std::uint64_t disk_usage() const;
    void enforce_quota(std::uint64_t incoming_bytes);
    std::filesystem::path path_;
    DurableQueueConfig config_;
    sqlite3* database_{nullptr};
    mutable std::mutex mutex_;
};
}  // namespace nexstream::publisher::infrastructure
