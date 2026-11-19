#include "nexstream/publisher/application/durable_agent.hpp"
#include <algorithm>
#include <stdexcept>

namespace nexstream::publisher::application {
DurableAgent::DurableAgent(AgentConfig config, IRecordQueue& queue,
                           ISystemCollector& collector, IBatchPublisher& publisher,
                           IIdentityGenerator& identities)
    : config_(std::move(config)), queue_(queue), collector_(collector), publisher_(publisher),
      identities_(identities) {
    if (config_.agent_id.empty() || config_.host_id.empty() || config_.key_id.empty() ||
        config_.secret.empty() || config_.batch_size == 0 || config_.batch_size > 50 ||
        config_.sampling_interval <= std::chrono::seconds::zero())
        throw std::invalid_argument("invalid agent config");
}
DurableAgent::~DurableAgent() { request_stop(); }
void DurableAgent::run() {
    if (running_.exchange(true)) return;
    sampling_thread_ = std::thread([this] { sampling_loop(); });
    publishing_thread_ = std::thread([this] { publishing_loop(); });
}
void DurableAgent::request_stop() {
    running_.store(false);
    if (sampling_thread_.joinable()) sampling_thread_.join();
    if (publishing_thread_.joinable()) publishing_thread_.join();
}
bool DurableAgent::sample_once() {
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    queue_.enforce_retention(now_ms);
    const auto id = identities_.next();
    auto record = collector_.collect(id); return record && queue_.enqueue(config_.host_id, *record);
}
PublishOutcome DurableAgent::publish_once() {
    const auto records = queue_.fetch_pending(config_.batch_size);
    if (records.empty()) { PublishOutcome empty; empty.status = TransactionStatus::kSuccess; empty.detail = "queue empty"; return empty; }
    domain::Batch batch; batch.agent_id = config_.agent_id; batch.host_id = config_.host_id;
    batch.key_id = config_.key_id; batch.secret = config_.secret;
    batch.batch_id = identities_.next(); batch.records = records;
    const auto result = publisher_.publish(batch); queue_.apply_outcome(result); return result;
}
void DurableAgent::sampling_loop() {
    auto next = std::chrono::steady_clock::now();
    while (running_.load()) {
        next += config_.sampling_interval; try { (void)sample_once(); } catch (...) {}
        std::this_thread::sleep_until(next);
        if (std::chrono::steady_clock::now() > next + config_.sampling_interval) next = std::chrono::steady_clock::now();
    }
}
void DurableAgent::publishing_loop() {
    while (running_.load()) {
        std::chrono::milliseconds delay = config_.minimum_drain_interval;
        try {
            const auto outcome = publish_once();
            if (outcome.detail == "queue empty") delay = config_.normal_publish_jitter;
            else if (outcome.status == TransactionStatus::kSuccess) {
                retry_.reset(); delay += retry_.normal_jitter(config_.normal_publish_jitter);
            }
            else delay = retry_.next_failure_delay();
        } catch (...) { delay = retry_.next_failure_delay(); }
        std::this_thread::sleep_for(std::max(delay, config_.minimum_drain_interval));
    }
}
}  // namespace nexstream::publisher::application
