#pragma once
#include <string>
#include <vector>
#include "nexstream/transport/v1/transport.pb.h"

namespace nexstream::publisher::application {
enum class TransactionStatus { kSuccess, kRetry, kPermanentFailure, kProtocolFailure };
struct RecordOutcome {
    std::string message_id;
    nexstream::transport::v1::ResultCode code{nexstream::transport::v1::UNKNOWN};
    bool retry{true};
};
struct PublishOutcome {
    TransactionStatus status{TransactionStatus::kRetry};
    std::vector<RecordOutcome> records;
    std::string detail;
};
[[nodiscard]] PublishOutcome interpret_ack(
    const nexstream::transport::v1::BatchAck& ack,
    const std::string& expected_batch_id,
    const std::vector<std::string>& expected_message_ids);
[[nodiscard]] PublishOutcome retry_all(const std::vector<std::string>& message_ids,
                                       std::string detail);
}  // namespace nexstream::publisher::application
