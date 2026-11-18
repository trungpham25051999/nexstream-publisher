#include <chrono>
#include <filesystem>
#include <sqlite3.h>
#include <unistd.h>
#include <stdexcept>
#include <sys/wait.h>
#include <csignal>
#include "nexstream/publisher/infrastructure/durable_queue.hpp"
#include "test_harness.hpp"

namespace app = nexstream::publisher::application;
namespace domain = nexstream::publisher::domain;
namespace infra = nexstream::publisher::infrastructure;
namespace transport = nexstream::transport::v1;
namespace {
std::filesystem::path temporary_path(const char* name) {
    return std::filesystem::temp_directory_path() /
        (std::string("nexstream-s06-") + name + "-" + std::to_string(::getpid()) + ".db");
}
domain::TelemetryRecord record(std::string id, std::int64_t timestamp = 1'800'000'000'000) {
    return {std::move(id), timestamp, 25.0, 1024, 2048};
}
void remove_database(const std::filesystem::path& path) {
    std::error_code error; std::filesystem::remove(path, error);
    std::filesystem::remove(path.string() + "-wal", error); std::filesystem::remove(path.string() + "-shm", error);
}
}  // namespace
int main() {
    TestHarness test; const auto path = temporary_path("queue"); remove_database(path);
    {
        infra::DurableQueue queue(path);
        test.expect(queue.enqueue("host", record("m1")), "new stable identity enqueued");
        test.expect(!queue.enqueue("host", record("m1")), "duplicate message identity is idempotent");
        test.expect(queue.enqueue("host", record("m2")) && queue.enqueue("host", record("m3")), "three records durable");
        app::PublishOutcome mixed; mixed.status = app::TransactionStatus::kRetry;
        mixed.records = {{"m1", transport::SUCCESS, false}, {"m2", transport::UNKNOWN, true},
                         {"m3", transport::PERMANENT_FAILURE, false}};
        queue.apply_outcome(mixed); const auto stats = queue.stats();
        test.expect(stats.pending == 1 && stats.quarantined == 1, "only SUCCESS deleted; permanent quarantined; UNKNOWN retained");
        test.expect(queue.fetch_pending(10)[0].message_id == "m2", "retry preserves original identity");
    }
    {
        infra::DurableQueue recovered(path);
        const auto stats = recovered.stats();
        test.expect(stats.pending == 1 && stats.quarantined == 1, "WAL queue recovers after close/reopen");
    }

    const auto crash = temporary_path("crash"); remove_database(crash);
    { infra::DurableQueue queue(crash);
      test.expect(queue.enqueue("host", record("committed")), "pre-crash record committed"); }
    int ready[2]{}; ::pipe(ready); const auto child = ::fork();
    if (child == 0) {
        ::close(ready[0]); sqlite3* database = nullptr; sqlite3_open(crash.c_str(), &database);
        sqlite3_exec(database, "BEGIN IMMEDIATE; INSERT INTO records(message_id,host_id,enqueued_ms,sample_timestamp_ms,cpu_usage,memory_used,memory_total,state,estimated_bytes) VALUES('uncommitted','host',1,1,1,1,2,'pending',128);", nullptr, nullptr, nullptr);
        const char value = '1'; (void)::write(ready[1], &value, 1); ::pause(); _exit(0);
    }
    ::close(ready[1]); char signal_value{}; (void)::read(ready[0], &signal_value, 1); ::close(ready[0]);
    ::kill(child, SIGKILL); int child_status = 0; ::waitpid(child, &child_status, 0);
    { infra::DurableQueue recovered(crash); const auto values = recovered.fetch_pending(10);
      test.expect(values.size() == 1 && values[0].message_id == "committed",
                  "WAL crash recovery preserves commit and rolls back partial transaction"); }

    sqlite3* raw = nullptr; sqlite3_open(path.c_str(), &raw);
    sqlite3_exec(raw, "UPDATE records SET enqueued_ms=0 WHERE state='pending'", nullptr, nullptr, nullptr); sqlite3_close(raw);
    { infra::DurableQueue retained(path); retained.enforce_retention(1'800'000'000'000);
      test.expect(retained.stats().pending == 0, "24-hour retention removes oldest pending record"); }

    const auto tiny = temporary_path("tiny"); remove_database(tiny);
    bool quota_rejected = false;
    try {
        infra::DurableQueueConfig config; config.disk_quota_bytes = 200; config.headroom_bytes = 100;
        infra::DurableQueue queue(tiny, config); (void)queue.enqueue("host", record("too-large-for-database"));
    } catch (const std::exception&) { quota_rejected = true; }
    test.expect(quota_rejected, "disk quota/headroom rejects when SQLite footprint cannot fit");
    bool enospc_rejected = false;
    if (std::filesystem::exists("/dev/full")) {
        try { infra::DurableQueue full("/dev/full"); } catch (const std::exception&) { enospc_rejected = true; }
        test.expect(enospc_rejected, "real ENOSPC-style /dev/full database failure is fail-safe");
    }

    const auto bounded = temporary_path("bounded"); remove_database(bounded);
    { infra::DurableQueueConfig config; config.disk_quota_bytes = 600; config.headroom_bytes = 100;
      infra::DurableQueue queue(bounded, config);
      test.expect(queue.enqueue("host", record("oldest")) && queue.enqueue("host", record("middle")) &&
                      queue.enqueue("host", record("newest")) && queue.enqueue("host", record("latest")),
                  "bounded queue admits records by evicting when necessary");
      const auto values = queue.fetch_pending(10);
      test.expect(values.size() < 4 && values.front().message_id != "oldest",
                  "quota pressure drops oldest pending record first"); }
    remove_database(path); remove_database(crash); remove_database(tiny); remove_database(bounded); return test.finish();
}
