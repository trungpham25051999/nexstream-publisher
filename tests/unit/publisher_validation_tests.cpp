#include <chrono>
#include "nexstream/publisher/infrastructure/tcp_publisher_client.hpp"
#include "test_harness.hpp"

namespace app = nexstream::publisher::application;
namespace domain = nexstream::publisher::domain;
namespace infra = nexstream::publisher::infrastructure;

domain::Batch valid_batch() {
    domain::Batch batch;
    batch.agent_id = "agent"; batch.host_id = "host"; batch.key_id = "key";
    batch.secret = "test-secret"; batch.batch_id = "batch";
    batch.records.push_back({"message", 1, 10.0, 1, 2}); return batch;
}

int main() {
    TestHarness test;
    infra::PublisherConfig config; config.port = 1;
    test.expect(config.transaction_timeout == std::chrono::seconds(10),
                "default transaction deadline is 10 seconds");
    infra::TcpPublisherClient client(config);

    auto batch = valid_batch();
    for (int i = 1; i < 51; ++i)
        batch.records.push_back({"message-" + std::to_string(i), i + 1, 10.0, 1, 2});
    auto outcome = client.publish(batch);
    test.expect(outcome.status == app::TransactionStatus::kProtocolFailure &&
                    outcome.records.size() == 51,
                "sample count rejected before connect without losing identities");

    batch = valid_batch();
    batch.records.push_back(batch.records.front());
    outcome = client.publish(batch);
    test.expect(outcome.status == app::TransactionStatus::kProtocolFailure,
                "duplicate message identity rejected");

    batch = valid_batch(); batch.records[0].message_id = std::string(129, 'x');
    outcome = client.publish(batch);
    test.expect(outcome.status == app::TransactionStatus::kProtocolFailure,
                "oversized identity rejected");
    return test.finish();
}
