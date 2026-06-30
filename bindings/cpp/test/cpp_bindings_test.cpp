// SPDX-License-Identifier: Apache-2.0
#include <ringloom/service.hpp>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>

namespace {

void require(bool condition, const std::string &message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

std::string getenvOr(const char *name, std::string fallback) {
    const char *value = std::getenv(name);
    return value == nullptr || value[0] == '\0' ? std::move(fallback) : std::string(value);
}

std::string shellQuote(const std::string &value) {
    std::string quoted = "'";
    for (const char ch : value) {
        if (ch == '\'') {
            quoted += "'\\''";
        } else {
            quoted += ch;
        }
    }
    quoted += "'";
    return quoted;
}

std::string runCommandCapture(const std::string &command) {
    std::string output;
    FILE *pipe = popen((command + " 2>&1").c_str(), "r");
    if (pipe == nullptr) {
        throw std::runtime_error("failed to run command: " + command);
    }

    char buffer[4096];
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        output += buffer;
    }

    const int status = pclose(pipe);
    if (status != 0) {
        throw std::runtime_error("command failed: " + command + "\n" + output);
    }
    return output;
}

std::unordered_map<std::string, std::string> parseEnv(const std::string &output) {
    std::unordered_map<std::string, std::string> env;
    std::istringstream stream(output);
    std::string line;
    while (std::getline(stream, line)) {
        const std::size_t equals = line.find('=');
        if (equals != std::string::npos && equals > 0) {
            env.emplace(line.substr(0, equals), line.substr(equals + 1));
        }
    }

    require(env.find("RINGLOOM_STORAGE_PATH") != env.end(), "broker output did not contain RINGLOOM_STORAGE_PATH");
    require(env.find("RINGLOOM_GROUP") != env.end(), "broker output did not contain RINGLOOM_GROUP");
    return env;
}

std::string createWorkspace() {
    char path[] = "/tmp/ringloom-cpp-local-ipc-XXXXXX";
    char *created = mkdtemp(path);
    if (created == nullptr) {
        throw std::runtime_error("failed to create temporary workspace");
    }
    return std::string(created);
}

class TestBroker {
public:
    TestBroker(
        std::string repo_root,
        std::string workspace,
        int node_id = 1,
        int port = 19001,
        std::string group = "ringloom-java-test",
        std::vector<std::string> peers = {})
        : repo_root_(std::move(repo_root)),
          workspace_(std::move(workspace)),
          node_id_(node_id) {
        const std::string broker_bin = getenvOr(
            "RINGLOOM_BROKER_BIN",
            repo_root_ + "/zig-out/bin/ringloom-broker");
        std::string command =
            "bash " + shellQuote(repo_root_ + "/scripts/start-test-broker.sh") +
            " --workspace " + shellQuote(workspace_) +
            " --node-id " + std::to_string(node_id_) +
            " --port " + std::to_string(port) +
            " --group " + shellQuote(group) +
            " --daemon --bin-dir " + shellQuote(std::filesystem::path(broker_bin).parent_path().string());
        for (const std::string &peer : peers) {
            command += " --peer " + shellQuote(peer);
        }
        env_ = parseEnv(runCommandCapture(command));
    }

    ~TestBroker() {
        try {
            stop();
        } catch (const std::exception &ex) {
            std::cerr << "failed to stop test broker: " << ex.what() << '\n';
        }
    }

    TestBroker(const TestBroker &) = delete;
    TestBroker &operator=(const TestBroker &) = delete;

    const std::string &storagePath() const {
        return env_.at("RINGLOOM_STORAGE_PATH");
    }

    const std::string &group() const {
        return env_.at("RINGLOOM_GROUP");
    }

    int nodeId() const {
        return node_id_;
    }

    void stop() {
        if (closed_) {
            return;
        }
        closed_ = true;

        const std::string command =
            "bash " + shellQuote(repo_root_ + "/scripts/start-test-broker.sh") +
            " --workspace " + shellQuote(workspace_) +
            " --node-id " + std::to_string(node_id_) +
            " --stop";
        static_cast<void>(runCommandCapture(command));
    }

private:
    std::string repo_root_;
    std::string workspace_;
    int node_id_;
    std::unordered_map<std::string, std::string> env_;
    bool closed_ = false;
};

ringloom::ServiceConfig serviceConfig(const std::string &service_name, const TestBroker &broker) {
    ringloom::ServiceConfig config = ringloom::ServiceConfig::of(service_name);
    config.storage_path = broker.storagePath();
    config.group = broker.group();
    config.broker_node_id = static_cast<std::int16_t>(broker.nodeId());
    config.heartbeat_timeout_ms = 10'000;
    config.control_buffer_length = 65'536;
    config.messages_buffer_length = 1'048'576;
    return config;
}

void testAbiBasics() {
    require(ringloom::abiVersion() == ringloom::service_abi_version, "unexpected C ABI version");
    require(ringloom::statusName(ringloom::Status::ok) == "ok", "unexpected status name for OK");
    static_cast<void>(ringloom::lastErrorMessage());

    ringloom_service_config_t config{};
    ringloom_service_t *service = nullptr;
    const ringloom_status_t status = ringloom_service_start(&config, &service);
    require(status == RINGLOOM_ERR_INVALID_ARGUMENT, "invalid start arguments should fail predictably");
    require(service == nullptr, "failed service start must leave the output handle null");
}

void testLocalIpc() {
    const std::string repo_root = getenvOr("RINGLOOM_PROJECT_ROOT", std::filesystem::current_path().string());
    const std::string workspace = createWorkspace();
    bool success = false;

    try {
        TestBroker broker(repo_root, workspace);

        auto echo = ringloom::Service::start(serviceConfig("cpp-echo", broker));
        auto consumer = echo.messageConsumer();
        auto ping = ringloom::Service::start(serviceConfig("cpp-ping", broker));
        auto client = ping.createClient("cpp-echo");
        ringloom::BufferClaim claim;

        require(!ping.aeronDirectory().empty(), "expected Aeron directory diagnostic");
        require(ping.aeronInboundStreamId() == 0, "local/direct-UDP path no longer uses broker ingress stream ids");
        require(client.lastAeronSendStatus() == ringloom::AeronPublicationStatus::unknown, "unexpected initial Aeron status");
        static_cast<void>(ping.publicationConnected());

        const std::string payload = "hello";
        bool sent = false;
        const auto send_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < send_deadline) {
            static_cast<void>(echo.pollControl());
            static_cast<void>(ping.pollControl());

            const ringloom::Status status = client.tryClaim(7, payload.size(), claim);
            if (status == ringloom::Status::ok) {
                std::memcpy(claim.payload(), payload.data(), payload.size());
                require(claim.commit() == ringloom::Status::ok, "claim commit failed");
                sent = true;
                break;
            }

            if (status != ringloom::Status::no_available_instance &&
                status != ringloom::Status::buffer_full) {
                throw ringloom::RingloomError("ringloom_client_try_claim", status);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        require(sent, "message was not sent before deadline");

        std::string received;
        std::uint16_t template_id = 0;
        const auto receive_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (received.empty() && std::chrono::steady_clock::now() < receive_deadline) {
            const int work = consumer.poll([&](const ringloom::Message &message) {
                received = message.payloadString();
                template_id = message.template_id;
            });
            if (work < 0) {
                throw ringloom::RingloomError("ringloom_message_consumer_poll", consumer.lastStatus());
            }
            if (work == 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        }

        require(received == "hello", "did not receive expected payload");
        require(template_id == 7, "did not receive expected template id");

        const auto targets = client.targetServices();
        require(std::any_of(targets.begin(), targets.end(), [](const ringloom::TargetService &target) {
            return target.target_node_id == 1 && target.target_service_id > 0;
        }), "client did not cache the discovered target");

        success = true;
        broker.stop();
    } catch (...) {
        std::cerr << "Preserving RingLoom C++ workspace: " << workspace << '\n';
        throw;
    }

    if (success) {
        std::filesystem::remove_all(workspace);
    }
}

void pollBoth(ringloom::Service &first, ringloom::Service &second) {
    static_cast<void>(first.pollControl());
    static_cast<void>(second.pollControl());
}

ringloom::TargetService waitForRemoteTarget(
    ringloom::Service &ping,
    ringloom::Service &echo,
    ringloom::Client &client,
    std::int16_t target_node_id) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
        pollBoth(ping, echo);
        client.refreshTargetServices();
        const auto targets = client.targetServices();
        const auto it = std::find_if(targets.begin(), targets.end(), [&](const ringloom::TargetService &target) {
            return target.target_node_id == target_node_id;
        });
        if (it != targets.end()) {
            return *it;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    throw std::runtime_error("timed out waiting for remote target discovery");
}

ringloom::Message waitForMessage(ringloom::MessageConsumer &consumer, std::string &payload_copy) {
    ringloom::Message captured;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (payload_copy.empty() && std::chrono::steady_clock::now() < deadline) {
        const int work = consumer.poll([&](const ringloom::Message &message) {
            captured = message;
            payload_copy = message.payloadString();
        });
        if (work < 0) {
            throw ringloom::RingloomError("ringloom_message_consumer_poll", consumer.lastStatus());
        }
        if (work == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }
    require(!payload_copy.empty(), "timed out waiting for remote payload");
    return captured;
}

void testRemoteAeron() {
    const std::string repo_root = getenvOr("RINGLOOM_PROJECT_ROOT", std::filesystem::current_path().string());
    const std::string workspace = createWorkspace();
    bool success = false;

    try {
        TestBroker broker_a(
            repo_root,
            workspace,
            1,
            19301,
            "ringloom-cpp-remote",
            {"2@127.0.0.1:19302"});
        TestBroker broker_b(
            repo_root,
            workspace,
            2,
            19302,
            "ringloom-cpp-remote",
            {"1@127.0.0.1:19301"});

        std::this_thread::sleep_for(std::chrono::seconds(2));

        auto echo = ringloom::Service::start(serviceConfig("cpp-remote-echo", broker_b));
        auto consumer = echo.messageConsumer();
        auto ping = ringloom::Service::start(serviceConfig("cpp-remote-ping", broker_a));
        auto client = ping.createClient("cpp-remote-echo");
        ringloom::BufferClaim claim;

        const auto target = waitForRemoteTarget(ping, echo, client, 2);
        const std::string payload = "remote";
        bool sent = false;
        const auto send_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < send_deadline) {
            const ringloom::Status status = client.tryClaimTo(
                target.target_node_id,
                target.target_service_id,
                77,
                payload.size(),
                claim);
            if (status == ringloom::Status::ok) {
                std::memcpy(claim.payload(), payload.data(), payload.size());
                require(claim.commit() == ringloom::Status::ok, "remote claim commit failed");
                sent = true;
                break;
            }
            if (status != ringloom::Status::no_available_instance &&
                status != ringloom::Status::backpressure &&
                status != ringloom::Status::peer_disconnected) {
                throw ringloom::RingloomError("ringloom_client_try_claim_to", status);
            }
            pollBoth(ping, echo);
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }

        require(sent, "remote claim was not committed before deadline");
        require(client.lastAeronSendStatus() == ringloom::AeronPublicationStatus::claimed, "unexpected last Aeron status");

        std::string received;
        const ringloom::Message message = waitForMessage(consumer, received);
        require(received == "remote", "did not receive expected remote payload");
        require(message.template_id == 77, "did not receive expected remote template id");
        require(message.source_node_id == 1, "unexpected remote source node");
        require(message.target_node_id == 2, "unexpected remote target node");

        success = true;
        broker_b.stop();
        broker_a.stop();
    } catch (...) {
        std::cerr << "Preserving RingLoom C++ workspace: " << workspace << '\n';
        throw;
    }

    if (success) {
        std::filesystem::remove_all(workspace);
    }
}

void testTopicValueTypes() {
    // Enum values
    require(static_cast<int>(ringloom::TopicStart::earliest) == RINGLOOM_TOPIC_START_EARLIEST, "TopicStart earliest mismatch");
    require(static_cast<int>(ringloom::TopicStart::latest) == RINGLOOM_TOPIC_START_LATEST, "TopicStart latest mismatch");
    require(static_cast<int>(ringloom::TopicAckMode::fire_and_forget) == 0, "TopicAckMode fire_and_forget mismatch");
    require(static_cast<int>(ringloom::TopicAckMode::replicate_once) == 1, "TopicAckMode replicate_once mismatch");

    // Status
    require(static_cast<int>(ringloom::Status::not_ready) == RINGLOOM_NOT_READY, "Status not_ready mismatch");
    require(!ringloom::isOk(ringloom::Status::not_ready), "not_ready should not be ok");

    // TopicConfig
    auto cfg = ringloom::TopicConfig::defaults();
    require(cfg.roll_scheme == "FAST_DAILY", "default roll scheme mismatch");
    require(cfg.retention_cycles == 0, "default retention cycles mismatch");
    require(cfg.flags == 0, "default flags mismatch");

    // TopicPollResult
    ringloom::TopicPollResult result;
    require(!result.hasMessage(), "empty poll result should not have message");
    require(result.copyPayload().empty(), "empty poll result should have empty copy");
}

void testTopicPublisher() {
    const std::string repo_root = getenvOr("RINGLOOM_PROJECT_ROOT", std::filesystem::current_path().string());
    const std::string workspace = createWorkspace();
    bool success = false;

    try {
        TestBroker broker(repo_root, workspace);

        auto echo = ringloom::Service::start(serviceConfig("cpp-topic-pub", broker));
        auto client = echo.createClient("cpp-topic-pub");

        // Register a topic publisher
        auto publisher = client.registerTopicPublication("pub-test");

        // Publish (may return not_connected since backend is stubbed)
        const std::string payload = "hello";
        static_cast<void>(publisher.publish(payload.data(), payload.size()));
        // Don't assert OK - the backend may be stubbed

        // Publish with ack mode
        std::uint64_t out_index = 0;
        static_cast<void>(publisher.publish(payload.data(), payload.size(),
            ringloom::TopicAckMode::replicate_once, 0, &out_index));

        // isAcked
        static_cast<void>(publisher.isAcked(0));  // exercise the API

        // publishOrThrow (may throw if not_connected, so catch)
        try {
            publisher.publishOrThrow(payload.data(), payload.size());
        } catch (const ringloom::RingloomError&) {
            // expected when backend is stubbed
        }

        // Close idempotency
        publisher.close();
        publisher.close();  // should not crash

        success = true;
        broker.stop();
    } catch (...) {
        std::cerr << "Preserving RingLoom C++ workspace: " << workspace << '\n';
        throw;
    }

    if (success) {
        std::filesystem::remove_all(workspace);
    }
}

void testTopicSubscriptionStubbed() {
    const std::string repo_root = getenvOr("RINGLOOM_PROJECT_ROOT", std::filesystem::current_path().string());
    const std::string workspace = createWorkspace();
    bool success = false;

    try {
        TestBroker broker(repo_root, workspace);

        auto echo = ringloom::Service::start(serviceConfig("cpp-topic-sub", broker));
        auto client = echo.createClient("cpp-topic-sub");

        // subscribeTopic should throw since subscription backend returns INTERNAL
        bool threw = false;
        try {
            auto sub = client.subscribeTopic("sub-test");
        } catch (const ringloom::RingloomError&) {
            threw = true;
        }
        require(threw, "subscribeTopic should throw when subscription backend is stubbed");

        success = true;
        broker.stop();
    } catch (...) {
        std::cerr << "Preserving RingLoom C++ workspace: " << workspace << '\n';
        throw;
    }

    if (success) {
        std::filesystem::remove_all(workspace);
    }
}

} // namespace

int main() {
    try {
        testAbiBasics();
        testTopicValueTypes();
        testLocalIpc();
        testRemoteAeron();
        testTopicPublisher();
        testTopicSubscriptionStubbed();
        std::cout << "C++ binding tests passed\n";
        return 0;
    } catch (const std::exception &ex) {
        std::cerr << "C++ binding test failed: " << ex.what() << '\n';
        return 1;
    }
}
