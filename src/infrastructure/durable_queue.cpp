#include "nexstream/publisher/infrastructure/durable_queue.hpp"
#include <sqlite3.h>
#include <stdexcept>

namespace nexstream::publisher::infrastructure {
namespace {
class Statement {
  public:
    Statement(sqlite3* database, const char* sql) {
        if (sqlite3_prepare_v2(database, sql, -1, &value_, nullptr) != SQLITE_OK)
            throw std::runtime_error("sqlite prepare failed");
    }
    ~Statement() { sqlite3_finalize(value_); }
    sqlite3_stmt* get() const { return value_; }
  private: sqlite3_stmt* value_{nullptr};
};
void bind_text(sqlite3_stmt* statement, int index, const std::string& value) {
    if (sqlite3_bind_text(statement, index, value.data(), static_cast<int>(value.size()), SQLITE_TRANSIENT) != SQLITE_OK)
        throw std::runtime_error("sqlite bind failed");
}
}  // namespace

DurableQueue::DurableQueue(std::filesystem::path path, DurableQueueConfig config)
    : path_(std::move(path)), config_(config) {
    if (config_.disk_quota_bytes == 0 || config_.headroom_bytes >= config_.disk_quota_bytes ||
        config_.max_retention <= std::chrono::hours::zero() || config_.max_retention > std::chrono::hours(24))
        throw std::invalid_argument("invalid queue bounds");
    if (sqlite3_open_v2(path_.c_str(), &database_, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr) != SQLITE_OK) {
        if (database_) { sqlite3_close(database_); database_ = nullptr; }
        throw std::runtime_error("sqlite open failed");
    }
    try {
        sqlite3_busy_timeout(database_, 2'000);
        execute("PRAGMA journal_mode=WAL"); execute("PRAGMA synchronous=FULL");
        execute("PRAGMA foreign_keys=ON");
        execute("CREATE TABLE IF NOT EXISTS records("
                "message_id TEXT PRIMARY KEY,host_id TEXT NOT NULL,enqueued_ms INTEGER NOT NULL,"
                "sample_timestamp_ms INTEGER NOT NULL,cpu_usage REAL NOT NULL,memory_used INTEGER NOT NULL,"
                "memory_total INTEGER NOT NULL,state TEXT NOT NULL CHECK(state IN('pending','quarantined')),"
                "quarantine_reason TEXT NOT NULL DEFAULT '',estimated_bytes INTEGER NOT NULL)");
        execute("CREATE INDEX IF NOT EXISTS records_pending_order ON records(state,enqueued_ms,message_id)");
    } catch (...) { sqlite3_close(database_); database_ = nullptr; throw; }
}
DurableQueue::~DurableQueue() { if (database_) sqlite3_close(database_); }
void DurableQueue::execute(const char* sql) const {
    char* error = nullptr; if (sqlite3_exec(database_, sql, nullptr, nullptr, &error) != SQLITE_OK) {
        const std::string detail = error ? error : "sqlite execute failed"; sqlite3_free(error);
        throw std::runtime_error(detail);
    }
}
std::uint64_t DurableQueue::disk_usage() const {
    std::uint64_t total = 0; std::error_code error;
    for (const auto suffix : {"", "-wal", "-shm"}) {
        const auto size = std::filesystem::file_size(path_.string() + suffix, error);
        if (!error) total += size;
        error.clear();
    }
    return total;
}
void DurableQueue::enforce_quota(std::uint64_t incoming) {
    const auto usable = config_.disk_quota_bytes - config_.headroom_bytes;
    if (incoming > usable) throw std::runtime_error("record exceeds queue quota");
    const auto parent = path_.has_parent_path() ? path_.parent_path() : std::filesystem::current_path();
    std::error_code space_error;
    const auto available = std::filesystem::space(parent, space_error).available;
    if (!space_error && available < config_.headroom_bytes + incoming)
        throw std::runtime_error("filesystem headroom exhausted");
    auto logical_usage = [&] {
        Statement total(database_, "SELECT COALESCE(sum(estimated_bytes),0) FROM records");
        if (sqlite3_step(total.get()) != SQLITE_ROW) throw std::runtime_error("quota query failed");
        return static_cast<std::uint64_t>(sqlite3_column_int64(total.get(), 0));
    };
    while (logical_usage() + incoming > usable) {
        Statement remove(database_, "DELETE FROM records WHERE message_id=(SELECT message_id FROM records WHERE state='pending' ORDER BY enqueued_ms,message_id LIMIT 1)");
        if (sqlite3_step(remove.get()) != SQLITE_DONE) throw std::runtime_error("quota eviction failed");
        if (sqlite3_changes(database_) == 0) throw std::runtime_error("queue quota exhausted by non-evictable data");
    }
}
bool DurableQueue::enqueue(const std::string& host_id, const domain::TelemetryRecord& record) {
    std::lock_guard lock(mutex_);
    const auto estimated = static_cast<std::uint64_t>(host_id.size() + record.message_id.size() + 128);
    enforce_quota(estimated); execute("BEGIN IMMEDIATE");
    try {
        Statement statement(database_, "INSERT OR IGNORE INTO records(message_id,host_id,enqueued_ms,sample_timestamp_ms,cpu_usage,memory_used,memory_total,state,estimated_bytes) VALUES(?,?,?,?,?,?,?, 'pending',?)");
        bind_text(statement.get(), 1, record.message_id); bind_text(statement.get(), 2, host_id);
        const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        sqlite3_bind_int64(statement.get(), 3, now_ms);
        sqlite3_bind_int64(statement.get(), 4, record.sample_timestamp_ms);
        sqlite3_bind_double(statement.get(), 5, record.cpu_usage_percent);
        sqlite3_bind_int64(statement.get(), 6, static_cast<sqlite3_int64>(record.memory_used_bytes));
        sqlite3_bind_int64(statement.get(), 7, static_cast<sqlite3_int64>(record.memory_total_bytes));
        sqlite3_bind_int64(statement.get(), 8, static_cast<sqlite3_int64>(estimated));
        if (sqlite3_step(statement.get()) != SQLITE_DONE) throw std::runtime_error("sqlite insert failed");
        const bool inserted = sqlite3_changes(database_) == 1; execute("COMMIT"); return inserted;
    } catch (...) { try { execute("ROLLBACK"); } catch (...) {} throw; }
}
std::vector<domain::TelemetryRecord> DurableQueue::fetch_pending(std::size_t limit) {
    std::lock_guard lock(mutex_); std::vector<domain::TelemetryRecord> output;
    Statement statement(database_, "SELECT message_id,sample_timestamp_ms,cpu_usage,memory_used,memory_total FROM records WHERE state='pending' ORDER BY enqueued_ms,message_id LIMIT ?");
    sqlite3_bind_int64(statement.get(), 1, static_cast<sqlite3_int64>(limit));
    while (sqlite3_step(statement.get()) == SQLITE_ROW) {
        domain::TelemetryRecord record;
        record.message_id = reinterpret_cast<const char*>(sqlite3_column_text(statement.get(), 0));
        record.sample_timestamp_ms = sqlite3_column_int64(statement.get(), 1);
        record.cpu_usage_percent = sqlite3_column_double(statement.get(), 2);
        record.memory_used_bytes = static_cast<std::uint64_t>(sqlite3_column_int64(statement.get(), 3));
        record.memory_total_bytes = static_cast<std::uint64_t>(sqlite3_column_int64(statement.get(), 4));
        output.push_back(std::move(record));
    }
    return output;
}
void DurableQueue::apply_outcome(const application::PublishOutcome& outcome) {
    std::lock_guard lock(mutex_); execute("BEGIN IMMEDIATE");
    try {
        for (const auto& record : outcome.records) {
            if (record.code == nexstream::transport::v1::SUCCESS && !record.retry) {
                Statement statement(database_, "DELETE FROM records WHERE message_id=? AND state='pending'");
                bind_text(statement.get(), 1, record.message_id); if (sqlite3_step(statement.get()) != SQLITE_DONE) throw std::runtime_error("delete failed");
            } else if ((record.code == nexstream::transport::v1::PERMANENT_FAILURE ||
                        outcome.status == application::TransactionStatus::kPermanentFailure) && !record.retry) {
                Statement statement(database_, "UPDATE records SET state='quarantined',quarantine_reason='PERMANENT_FAILURE' WHERE message_id=? AND state='pending'");
                bind_text(statement.get(), 1, record.message_id); if (sqlite3_step(statement.get()) != SQLITE_DONE) throw std::runtime_error("quarantine failed");
            }
        }
        execute("COMMIT");
    } catch (...) { try { execute("ROLLBACK"); } catch (...) {} throw; }
}
void DurableQueue::enforce_retention(std::int64_t now_ms) {
    std::lock_guard lock(mutex_); const auto cutoff = now_ms -
        std::chrono::duration_cast<std::chrono::milliseconds>(config_.max_retention).count();
    Statement statement(database_, "DELETE FROM records WHERE state='pending' AND enqueued_ms<?");
    sqlite3_bind_int64(statement.get(), 1, cutoff); if (sqlite3_step(statement.get()) != SQLITE_DONE)
        throw std::runtime_error("retention cleanup failed");
}
QueueStats DurableQueue::stats() const {
    std::lock_guard lock(mutex_); QueueStats result; Statement statement(database_,
        "SELECT state,count(*) FROM records GROUP BY state");
    while (sqlite3_step(statement.get()) == SQLITE_ROW) {
        const std::string_view state(reinterpret_cast<const char*>(sqlite3_column_text(statement.get(), 0)));
        const auto count = static_cast<std::size_t>(sqlite3_column_int64(statement.get(), 1));
        if (state == "pending") result.pending = count; else result.quarantined = count;
    }
    result.disk_bytes = disk_usage(); return result;
}
}  // namespace nexstream::publisher::infrastructure
