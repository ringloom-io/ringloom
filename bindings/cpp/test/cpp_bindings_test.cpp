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
    TestBroker(std::string repo_root, std::string workspace)
        : repo_root_(std::move(repo_root)), workspace_(std::move(workspace)) {
        const std::string broker_bin = getenvOr(
            "RINGLOOM_BROKER_BIN",
            repo_root_ + "/zig-out/bin/ringloom-broker");
        const std::string command =
            "bash " + shellQuote(repo_root_ + "/scripts/start-test-broker.sh") +
            " --workspace " + shellQuote(workspace_) +
            " --daemon --bin-dir " + shellQuote(std::filesystem::path(broker_bin).parent_path().string());
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

    void stop() {
        if (closed_) {
            return;
        }
        closed_ = true;

        const std::string command =
            "bash " + shellQuote(repo_root_ + "/scripts/start-test-broker.sh") +
            " --workspace " + shellQuote(workspace_) +
            " --stop";
        static_cast<void>(runCommandCapture(command));
    }

private:
    std::string repo_root_;
    std::string workspace_;
    std::unordered_map<std::string, std::string> env_;
    bool closed_ = false;
};

ringloom::ServiceConfig serviceConfig(const std::string &service_name, const TestBroker &broker) {
    ringloom::ServiceConfig config = ringloom::ServiceConfig::of(service_name);
    config.storage_path = broker.storagePath();
    config.group = broker.group();
    config.broker_node_id = 1;
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

} // namespace

int main() {
    try {
        testAbiBasics();
        testLocalIpc();
        std::cout << "C++ binding tests passed\n";
        return 0;
    } catch (const std::exception &ex) {
        std::cerr << "C++ binding test failed: " << ex.what() << '\n';
        return 1;
    }
}
