#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_map>
#include "nexstream/publisher/application/durable_agent.hpp"
#include "nexstream/publisher/infrastructure/durable_queue.hpp"
#include "nexstream/publisher/infrastructure/linux_collector.hpp"
#include "nexstream/publisher/infrastructure/tcp_publisher_client.hpp"

namespace { std::atomic_bool stop_requested{false}; void stop_handler(int) { stop_requested.store(true); } }
int main(int argc, char** argv) {
    std::unordered_map<std::string, std::string> options;
    for (int index = 1; index + 1 < argc; index += 2) options.emplace(argv[index], argv[index + 1]);
    const char* required[]{"--address","--port","--tls-hostname","--ca-file","--queue-db",
                           "--agent-id","--host-id","--key-id","--secret-file"};
    for (const auto* key : required) if (!options.contains(key)) {
        std::cerr << "usage: nexstream-agent --address IPv4 --port PORT --tls-hostname NAME --ca-file PATH "
                     "--queue-db PATH --agent-id ID --host-id ID --key-id ID --secret-file PATH\n"; return 2;
    }
    unsigned int port = 0; const auto& port_text = options["--port"];
    const auto parsed = std::from_chars(port_text.data(), port_text.data() + port_text.size(), port);
    if (parsed.ec != std::errc{} || parsed.ptr != port_text.data() + port_text.size() || port > 65'535) return 2;
    std::ifstream secret_input(options["--secret-file"]); std::string secret;
    if (!secret_input || !std::getline(secret_input, secret) || secret.empty()) {
        std::cerr << "secret file unavailable or empty\n"; return 2;
    }
    try {
        nexstream::publisher::infrastructure::DurableQueue queue(options["--queue-db"]);
        nexstream::publisher::infrastructure::LinuxSystemCollector collector;
        nexstream::publisher::infrastructure::CsprngIdentityGenerator identities;
        nexstream::publisher::infrastructure::PublisherConfig publisher_config;
        publisher_config.address = options["--address"]; publisher_config.port = static_cast<std::uint16_t>(port);
        publisher_config.tls_enabled = true; publisher_config.tls_server_hostname = options["--tls-hostname"];
        publisher_config.tls_ca_file = options["--ca-file"];
        nexstream::publisher::infrastructure::TcpPublisherClient publisher(publisher_config);
        nexstream::publisher::application::AgentConfig agent_config;
        agent_config.agent_id=options["--agent-id"]; agent_config.host_id=options["--host-id"];
        agent_config.key_id=options["--key-id"]; agent_config.secret=std::move(secret);
        nexstream::publisher::application::DurableAgent agent(agent_config, queue, collector, publisher, identities);
        struct sigaction action{}; action.sa_handler = stop_handler; sigemptyset(&action.sa_mask);
        sigaction(SIGINT, &action, nullptr); sigaction(SIGTERM, &action, nullptr);
        for (const auto& status : collector.extension_status()) std::cerr << status.name << ": " << status.detail << '\n';
        agent.run(); while (!stop_requested.load()) std::this_thread::sleep_for(std::chrono::milliseconds(100));
        agent.request_stop();
    } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
    return 0;
}
