#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <vector>
#include "nexstream/publisher/domain/batch.hpp"
#include "nexstream/publisher/application/agent_ports.hpp"

namespace nexstream::publisher::infrastructure {
enum class CollectorStatus { kSupported, kUnsupported, kError };
struct AdapterStatus { std::string name; CollectorStatus status; std::string detail; };
class LinuxSystemCollector final : public application::ISystemCollector {
  public:
    [[nodiscard]] std::optional<domain::TelemetryRecord> collect(const std::string& message_id) override;
    [[nodiscard]] static std::vector<AdapterStatus> extension_status();
  private:
    std::uint64_t previous_total_{0};
    std::uint64_t previous_idle_{0};
};
[[nodiscard]] std::string generate_message_id();
class CsprngIdentityGenerator final : public application::IIdentityGenerator {
  public: [[nodiscard]] std::string next() override { return generate_message_id(); }
};
}  // namespace nexstream::publisher::infrastructure
