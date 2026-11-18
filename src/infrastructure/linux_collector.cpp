#include "nexstream/publisher/infrastructure/linux_collector.hpp"
#include <openssl/rand.h>
#include <array>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>

namespace nexstream::publisher::infrastructure {
std::optional<domain::TelemetryRecord> LinuxSystemCollector::collect(const std::string& message_id) {
    std::ifstream cpu("/proc/stat"); std::string label; std::uint64_t user=0,nice=0,system=0,idle=0,iowait=0,irq=0,softirq=0,steal=0;
    if (!(cpu >> label >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal) || label != "cpu") return std::nullopt;
    const auto total = user + nice + system + idle + iowait + irq + softirq + steal;
    const auto idle_total = idle + iowait;
    if (previous_total_ == 0 || total <= previous_total_) { previous_total_ = total; previous_idle_ = idle_total; return std::nullopt; }
    const auto delta_total = total - previous_total_; const auto delta_idle = idle_total - previous_idle_;
    previous_total_ = total; previous_idle_ = idle_total;

    std::ifstream memory("/proc/meminfo"); std::string key, unit; std::uint64_t value=0,total_kib=0,available_kib=0;
    while (memory >> key >> value >> unit) {
        if (key == "MemTotal:") total_kib = value;
        else if (key == "MemAvailable:") available_kib = value;
        if (total_kib && available_kib) break;
    }
    if (total_kib == 0 || available_kib > total_kib) return std::nullopt;
    domain::TelemetryRecord record; record.message_id = message_id;
    record.sample_timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    record.cpu_usage_percent = 100.0 * static_cast<double>(delta_total - delta_idle) /
                               static_cast<double>(delta_total);
    record.memory_total_bytes = total_kib * 1024; record.memory_used_bytes = (total_kib - available_kib) * 1024;
    return record;
}
std::vector<AdapterStatus> LinuxSystemCollector::extension_status() {
    return {{"gpu", CollectorStatus::kUnsupported, "UNSUPPORTED: no GPU field in contracts v1"},
            {"disk", CollectorStatus::kUnsupported, "UNSUPPORTED: no disk field in contracts v1"},
            {"network", CollectorStatus::kUnsupported, "UNSUPPORTED: no network field in contracts v1"}};
}
std::string generate_message_id() {
    std::array<unsigned char, 16> bytes{};
    if (RAND_bytes(bytes.data(), bytes.size()) != 1) throw std::runtime_error("CSPRNG failed");
    std::ostringstream output; output << std::hex << std::setfill('0');
    for (const auto value : bytes) output << std::setw(2) << static_cast<unsigned int>(value);
    return output.str();
}
}  // namespace nexstream::publisher::infrastructure
