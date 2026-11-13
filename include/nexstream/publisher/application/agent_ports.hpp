#pragma once
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>
#include "nexstream/publisher/application/publish_outcome.hpp"
#include "nexstream/publisher/domain/batch.hpp"

namespace nexstream::publisher::application {
class IRecordQueue {
  public:
    virtual ~IRecordQueue() = default;
    [[nodiscard]] virtual bool enqueue(const std::string&, const domain::TelemetryRecord&) = 0;
    [[nodiscard]] virtual std::vector<domain::TelemetryRecord> fetch_pending(std::size_t limit) = 0;
    virtual void apply_outcome(const PublishOutcome&) = 0;
    virtual void enforce_retention(std::int64_t now_ms) = 0;
};
class ISystemCollector {
  public:
    virtual ~ISystemCollector() = default;
    [[nodiscard]] virtual std::optional<domain::TelemetryRecord> collect(const std::string& message_id) = 0;
};
class IBatchPublisher {
  public:
    virtual ~IBatchPublisher() = default;
    [[nodiscard]] virtual PublishOutcome publish(const domain::Batch&) const = 0;
};
class IIdentityGenerator {
  public:
    virtual ~IIdentityGenerator() = default;
    [[nodiscard]] virtual std::string next() = 0;
};
}  // namespace nexstream::publisher::application
