#include <string>
#include <vector>
#include "nexstream/publisher/application/publish_outcome.hpp"
#include "test_harness.hpp"

namespace transport = nexstream::transport::v1;
using nexstream::publisher::application::TransactionStatus;
using nexstream::publisher::application::interpret_ack;

transport::BatchAck ack(transport::ResultCode overall, transport::ResultCode record) {
    transport::BatchAck value; value.set_batch_id("batch"); value.set_overall_result(overall);
    auto* item = value.add_message_results(); item->set_message_id("message"); item->set_code(record);
    return value;
}

int main() {
    TestHarness test;
    const std::vector<std::string> ids{"message"};
    auto result = interpret_ack(ack(transport::SUCCESS, transport::SUCCESS), "batch", ids);
    test.expect(result.status == TransactionStatus::kSuccess && !result.records[0].retry,
                "SUCCESS releases confirmed record");
    result = interpret_ack(ack(transport::UNKNOWN, transport::UNKNOWN), "batch", ids);
    test.expect(result.status == TransactionStatus::kRetry && result.records[0].retry &&
                    result.records[0].message_id == "message",
                "UNKNOWN retains stable identity for retry");
    result = interpret_ack(ack(transport::RETRYABLE_FAILURE, transport::RETRYABLE_FAILURE),
                           "batch", ids);
    test.expect(result.status == TransactionStatus::kRetry && result.records[0].retry,
                "RETRYABLE_FAILURE retries");
    result = interpret_ack(ack(transport::RETRY_LATER, transport::RETRY_LATER), "batch", ids);
    test.expect(result.status == TransactionStatus::kRetry && result.records[0].retry,
                "RETRY_LATER retries");
    result = interpret_ack(ack(transport::PERMANENT_FAILURE, transport::PERMANENT_FAILURE),
                           "batch", ids);
    test.expect(result.status == TransactionStatus::kPermanentFailure && !result.records[0].retry,
                "PERMANENT_FAILURE does not auto-retry");
    result = interpret_ack(ack(transport::SUCCESS, transport::UNKNOWN), "batch", ids);
    test.expect(result.status == TransactionStatus::kProtocolFailure && result.records[0].retry,
                "conflicting SUCCESS is rejected safely");
    auto mismatch = ack(transport::SUCCESS, transport::SUCCESS); mismatch.set_batch_id("other");
    result = interpret_ack(mismatch, "batch", ids);
    test.expect(result.status == TransactionStatus::kProtocolFailure && result.records[0].retry,
                "mismatched batch identity retains record");
    return test.finish();
}
