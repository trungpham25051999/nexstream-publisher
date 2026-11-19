#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>
#include "nexstream/publisher/application/durable_agent.hpp"
#include "test_harness.hpp"

namespace app = nexstream::publisher::application;
namespace domain = nexstream::publisher::domain;
namespace transport = nexstream::transport::v1;
using namespace std::chrono_literals;
namespace {
class Queue final : public app::IRecordQueue {
  public:
    bool enqueue(const std::string&, const domain::TelemetryRecord& record) override {
        std::lock_guard lock(mutex); records.push_back(record); ++enqueued; return true;
    }
    std::vector<domain::TelemetryRecord> fetch_pending(std::size_t limit) override {
        std::lock_guard lock(mutex); return {records.begin(), records.begin() +
            static_cast<std::ptrdiff_t>(std::min(limit, records.size()))};
    }
    void apply_outcome(const app::PublishOutcome& outcome) override {
        std::lock_guard lock(mutex);
        for (const auto& result : outcome.records) if (!result.retry && result.code == transport::SUCCESS)
            std::erase_if(records, [&](const auto& value) { return value.message_id == result.message_id; });
    }
    void enforce_retention(std::int64_t) override { ++retention_calls; }
    std::mutex mutex; std::vector<domain::TelemetryRecord> records;
    std::atomic_size_t enqueued{0}, retention_calls{0};
};
class Collector final : public app::ISystemCollector {
  public: std::optional<domain::TelemetryRecord> collect(const std::string& id) override {
      return domain::TelemetryRecord{id, 1'800'000'000'000, 10, 1, 2}; }
};
class Identity final : public app::IIdentityGenerator {
  public: std::string next() override { return "id-" + std::to_string(++value); } std::atomic_uint64_t value{0};
};
class SlowPublisher final : public app::IBatchPublisher {
  public:
    app::PublishOutcome publish(const domain::Batch& batch) const override {
        const auto now = ++active; auto observed = max_active.load();
        while (now > observed && !max_active.compare_exchange_weak(observed, now)) {}
        ++calls; std::this_thread::sleep_for(1200ms); --active;
        return app::retry_all([&] { std::vector<std::string> ids; for (const auto& r : batch.records) ids.push_back(r.message_id); return ids; }(), "network unavailable");
    }
    mutable std::atomic_int active{0}, max_active{0}; mutable std::atomic_size_t calls{0};
};
}  // namespace
int main() {
    TestHarness test; Queue queue; Collector collector; Identity identity; SlowPublisher publisher;
    app::AgentConfig config; config.agent_id="agent"; config.host_id="host"; config.key_id="key";
    config.secret="fixture"; config.batch_size=10; config.sampling_interval=1s;
    config.minimum_drain_interval=50ms; config.normal_publish_jitter=50ms;
    app::DurableAgent agent(config, queue, collector, publisher, identity);
    agent.run(); std::this_thread::sleep_for(2250ms); agent.request_stop();
    test.expect(queue.enqueued.load() >= 2, "sampling continues while publisher call is blocked");
    test.expect(queue.retention_calls.load() >= 2, "production sampling path enforces retention");
    test.expect(publisher.max_active.load() == 1, "only one active batch per Agent");
    test.expect(!queue.records.empty(), "network failure retains stable records for retry");
    return test.finish();
}
