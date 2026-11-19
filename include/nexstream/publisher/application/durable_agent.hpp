#pragma once
#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include "nexstream/publisher/application/retry_policy.hpp"
#include "nexstream/publisher/application/agent_ports.hpp"

namespace nexstream::publisher::application {
struct AgentConfig {
    std::string agent_id;
    std::string host_id;
    std::string key_id;
    std::string secret;
    std::size_t batch_size{50};
    std::chrono::seconds sampling_interval{1};
    std::chrono::milliseconds normal_publish_jitter{250};
    std::chrono::milliseconds minimum_drain_interval{100};
};
class DurableAgent {
  public:
    DurableAgent(AgentConfig config, IRecordQueue& queue,
                 ISystemCollector& collector, IBatchPublisher& publisher,
                 IIdentityGenerator& identities);
    ~DurableAgent();
    void run();
    void request_stop();
    [[nodiscard]] bool sample_once();
    [[nodiscard]] PublishOutcome publish_once();
  private:
    void sampling_loop();
    void publishing_loop();
    AgentConfig config_;
    IRecordQueue& queue_;
    ISystemCollector& collector_;
    IBatchPublisher& publisher_;
    IIdentityGenerator& identities_;
    std::atomic_bool running_{false};
    std::thread sampling_thread_;
    std::thread publishing_thread_;
    RetryPolicy retry_;
};
}  // namespace nexstream::publisher::application
