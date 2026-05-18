// SPDX-License-Identifier: Apache-2.0
#ifndef RINGLOOM_CPP_SERVICE_HPP
#define RINGLOOM_CPP_SERVICE_HPP

#include <ringloom_service.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ringloom {

inline constexpr std::uint32_t service_abi_version = RINGLOOM_SERVICE_ABI_VERSION;

enum class Status : int {
    ok = RINGLOOM_OK,
    invalid_argument = RINGLOOM_ERR_INVALID_ARGUMENT,
    out_of_memory = RINGLOOM_ERR_OUT_OF_MEMORY,
    broker_not_found = RINGLOOM_ERR_BROKER_NOT_FOUND,
    registration_timeout = RINGLOOM_ERR_REGISTRATION_TIMEOUT,
    buffer_full = RINGLOOM_ERR_BUFFER_FULL,
    no_available_instance = RINGLOOM_ERR_NO_AVAILABLE_INSTANCE,
    backpressure = RINGLOOM_ERR_BACKPRESSURE,
    peer_disconnected = RINGLOOM_ERR_PEER_DISCONNECTED,
    claim_not_active = RINGLOOM_ERR_CLAIM_NOT_ACTIVE,
    message_too_long = RINGLOOM_ERR_MESSAGE_TOO_LONG,
    internal = RINGLOOM_ERR_INTERNAL,
};

enum class AeronPublicationStatus : int {
    unknown = RINGLOOM_AERON_PUBLICATION_UNKNOWN,
    claimed = RINGLOOM_AERON_PUBLICATION_CLAIMED,
    not_connected = RINGLOOM_AERON_PUBLICATION_NOT_CONNECTED,
    back_pressured = RINGLOOM_AERON_PUBLICATION_BACK_PRESSURED,
    admin_action = RINGLOOM_AERON_PUBLICATION_ADMIN_ACTION,
    closed = RINGLOOM_AERON_PUBLICATION_CLOSED,
    max_position_exceeded = RINGLOOM_AERON_PUBLICATION_MAX_POSITION_EXCEEDED,
    failed = RINGLOOM_AERON_PUBLICATION_FAILED,
};

inline Status toStatus(ringloom_status_t status) noexcept {
    return static_cast<Status>(static_cast<int>(status));
}

inline ringloom_status_t toNativeStatus(Status status) noexcept {
    return static_cast<ringloom_status_t>(static_cast<int>(status));
}

inline bool isOk(Status status) noexcept {
    return status == Status::ok;
}

inline std::uint32_t abiVersion() noexcept {
    return ringloom_service_abi_version();
}

inline std::string statusName(Status status) {
    const char *name = ringloom_status_string(toNativeStatus(status));
    return name == nullptr ? std::string{} : std::string{name};
}

inline std::string aeronPublicationStatusName(AeronPublicationStatus status) {
    const char *name = ringloom_aeron_publication_status_string(
        static_cast<ringloom_aeron_publication_status_t>(static_cast<int>(status)));
    return name == nullptr ? std::string{} : std::string{name};
}

inline std::string lastErrorMessage() {
    const char *message = ringloom_last_error_message();
    return message == nullptr ? std::string{} : std::string{message};
}

class RingloomError final : public std::runtime_error {
public:
    RingloomError(std::string operation, Status status)
        : std::runtime_error(makeMessage(operation, status)),
          operation_(std::move(operation)),
          status_(status) {}

    Status status() const noexcept {
        return status_;
    }

    const std::string &operation() const noexcept {
        return operation_;
    }

private:
    static std::string makeMessage(const std::string &operation, Status status) {
        std::string message = operation;
        message += " failed: ";
        message += statusName(status);

        std::string detail = lastErrorMessage();
        if (!detail.empty()) {
            message += ": ";
            message += detail;
        }
        return message;
    }

    std::string operation_;
    Status status_;
};

inline void throwIfNotOk(std::string operation, Status status) {
    if (!isOk(status)) {
        throw RingloomError(std::move(operation), status);
    }
}

struct ServiceConfig {
    static constexpr const char *default_storage_path = "/dev/shm";
    static constexpr const char *default_group = "default";
    static constexpr std::int16_t default_broker_node_id = 1;
    static constexpr std::int32_t default_heartbeat_timeout_ms = 10'000;
    static constexpr std::size_t default_control_buffer_length = 65'536;
    static constexpr std::size_t default_messages_buffer_length = 1'048'576;

    std::string service_name;
    std::string storage_path = default_storage_path;
    std::string group = default_group;
    std::int16_t broker_node_id = default_broker_node_id;
    bool blocking_mode = false;
    std::int32_t heartbeat_timeout_ms = default_heartbeat_timeout_ms;
    std::size_t control_buffer_length = default_control_buffer_length;
    std::size_t messages_buffer_length = default_messages_buffer_length;
    bool leader_election_enabled = false;

    static ServiceConfig of(std::string service_name) {
        ServiceConfig config;
        config.service_name = std::move(service_name);
        return config;
    }
};

struct TargetService {
    std::int32_t target_service_id = 0;
    std::int16_t target_node_id = 0;
    bool is_leader = false;
};

enum class ServiceLifecycleEventType : int {
    available = RINGLOOM_SERVICE_AVAILABLE,
    unavailable = RINGLOOM_SERVICE_UNAVAILABLE,
};

struct ServiceLifecycleEvent {
    ServiceLifecycleEventType type = ServiceLifecycleEventType::available;
    std::string service_name;
    std::int32_t service_id = 0;
    std::int16_t node_id = 0;
    bool is_leader = false;
};

struct Message {
    std::int64_t correlation_id = 0;
    std::int16_t source_node_id = 0;
    std::int16_t source_service_id = 0;
    std::int16_t target_node_id = 0;
    std::int16_t target_service_id = 0;
    std::uint16_t template_id = 0;
    std::uint8_t flags = 0;
    const std::uint8_t *payload = nullptr;
    std::size_t payload_len = 0;

    std::vector<std::uint8_t> copyPayload() const {
        if (payload == nullptr || payload_len == 0) {
            return {};
        }
        return std::vector<std::uint8_t>(payload, payload + payload_len);
    }

    std::string payloadString() const {
        if (payload == nullptr || payload_len == 0) {
            return {};
        }
        return std::string(reinterpret_cast<const char *>(payload), payload_len);
    }
};

class BufferClaim {
public:
    BufferClaim() = default;
    ~BufferClaim() {
        if (active()) {
            static_cast<void>(abort());
        }
    }

    BufferClaim(const BufferClaim &) = delete;
    BufferClaim &operator=(const BufferClaim &) = delete;
    BufferClaim(BufferClaim &&) = delete;
    BufferClaim &operator=(BufferClaim &&) = delete;

    bool active() const noexcept {
        return claim_._active != 0;
    }

    std::uint8_t *payload() const noexcept {
        return claim_.payload;
    }

    std::size_t payloadLength() const noexcept {
        return claim_.payload_len;
    }

    Status commit() noexcept {
        return toStatus(ringloom_buffer_claim_commit(&claim_));
    }

    Status abort() noexcept {
        return toStatus(ringloom_buffer_claim_abort(&claim_));
    }

private:
    friend class Client;

    ringloom_buffer_claim_t *native() noexcept {
        return &claim_;
    }

    ringloom_buffer_claim_t claim_{};
};

class Client {
public:
    using LifecycleHandler = std::function<void(const ServiceLifecycleEvent &)>;

    explicit Client(ringloom_client_t *handle) : handle_(handle) {
        if (handle_ == nullptr) {
            throw std::invalid_argument("client handle must not be null");
        }
        throwIfNotOk("ringloom_client_set_lifecycle_handler", setNativeLifecycleHandler());
        refreshTargetServices();
    }

    ~Client() {
        close();
    }

    Client(const Client &) = delete;
    Client &operator=(const Client &) = delete;
    Client(Client &&) = delete;
    Client &operator=(Client &&) = delete;

    Status tryClaim(std::uint16_t template_id, std::size_t payload_len, BufferClaim &claim) noexcept {
        if (claim.active()) {
            return Status::invalid_argument;
        }
        return toStatus(ringloom_client_try_claim(handle_, template_id, payload_len, claim.native()));
    }

    Status tryClaimTo(
        std::int16_t target_node_id,
        std::int32_t target_service_id,
        std::uint16_t template_id,
        std::size_t payload_len,
        BufferClaim &claim) noexcept {
        if (claim.active()) {
            return Status::invalid_argument;
        }
        return toStatus(ringloom_client_try_claim_to(
            handle_,
            target_node_id,
            target_service_id,
            template_id,
            payload_len,
            claim.native()));
    }

    Status send(const void *payload, std::size_t payload_len) noexcept {
        return toStatus(ringloom_client_send(
            handle_,
            static_cast<const std::uint8_t *>(payload),
            payload_len));
    }

    Status send(std::string_view payload) noexcept {
        return send(payload.data(), payload.size());
    }

    Status send(const std::vector<std::uint8_t> &payload) noexcept {
        return send(payload.data(), payload.size());
    }

    void sendOrThrow(const void *payload, std::size_t payload_len) {
        throwIfNotOk("ringloom_client_send", send(payload, payload_len));
    }

    Status sendTo(
        std::int16_t target_node_id,
        std::int32_t target_service_id,
        const void *payload,
        std::size_t payload_len) noexcept {
        return toStatus(ringloom_client_send_to(
            handle_,
            target_node_id,
            target_service_id,
            static_cast<const std::uint8_t *>(payload),
            payload_len));
    }

    Status sendToLeader(const void *payload, std::size_t payload_len) noexcept {
        return toStatus(ringloom_client_send_to_leader(
            handle_,
            static_cast<const std::uint8_t *>(payload),
            payload_len));
    }

    std::vector<TargetService> targetServices() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return target_services_;
    }

    AeronPublicationStatus lastAeronSendStatus() {
        ringloom_aeron_publication_status_t status = RINGLOOM_AERON_PUBLICATION_UNKNOWN;
        throwIfNotOk(
            "ringloom_client_last_aeron_send_status",
            toStatus(ringloom_client_last_aeron_send_status(handle_, &status)));
        return static_cast<AeronPublicationStatus>(static_cast<int>(status));
    }

    void refreshTargetServices() {
        std::size_t capacity = 0;
        for (;;) {
            std::vector<ringloom_client_target_t> native_targets(capacity);
            std::size_t count = 0;
            const auto status = toStatus(ringloom_client_list_targets(
                handle_,
                native_targets.empty() ? nullptr : native_targets.data(),
                native_targets.size(),
                &count));
            throwIfNotOk("ringloom_client_list_targets", status);

            if (count > native_targets.size()) {
                capacity = count;
                continue;
            }

            std::vector<TargetService> targets;
            targets.reserve(count);
            for (std::size_t i = 0; i < count; ++i) {
                targets.push_back(TargetService{
                    native_targets[i].target_service_id,
                    native_targets[i].target_node_id,
                    native_targets[i].is_leader,
                });
            }

            std::lock_guard<std::mutex> lock(mutex_);
            target_services_ = std::move(targets);
            return;
        }
    }

    void onLifecycle(LifecycleHandler handler) {
        std::lock_guard<std::mutex> lock(mutex_);
        lifecycle_handler_ = std::move(handler);
    }

    void clearLifecycleHandler() {
        std::lock_guard<std::mutex> lock(mutex_);
        lifecycle_handler_ = nullptr;
    }

    void rethrowLifecycleException() {
        std::exception_ptr pending;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            pending = lifecycle_exception_;
            lifecycle_exception_ = nullptr;
        }
        if (pending) {
            std::rethrow_exception(pending);
        }
    }

    void close() noexcept {
        if (handle_ == nullptr) {
            return;
        }
        static_cast<void>(ringloom_client_set_lifecycle_handler(handle_, nullptr, nullptr));
        ringloom_client_destroy(handle_);
        handle_ = nullptr;
    }

private:
    Status setNativeLifecycleHandler() noexcept {
        return toStatus(ringloom_client_set_lifecycle_handler(handle_, &Client::dispatchLifecycle, this));
    }

    static void dispatchLifecycle(void *user_data, const ringloom_service_lifecycle_event_t *native_event) noexcept {
        if (user_data == nullptr || native_event == nullptr) {
            return;
        }

        auto *self = static_cast<Client *>(user_data);
        self->handleLifecycle(*native_event);
    }

    void handleLifecycle(const ringloom_service_lifecycle_event_t &native_event) noexcept {
        try {
            ServiceLifecycleEvent event;
            event.type = native_event.event_type == RINGLOOM_SERVICE_UNAVAILABLE
                ? ServiceLifecycleEventType::unavailable
                : ServiceLifecycleEventType::available;
            event.service_name = native_event.service_name == nullptr
                ? std::string{}
                : std::string(native_event.service_name, native_event.service_name_len);
            event.service_id = native_event.service_id;
            event.node_id = native_event.node_id;
            event.is_leader = native_event.is_leader;

            LifecycleHandler handler;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                updateTargetCache(event);
                handler = lifecycle_handler_;
            }

            if (handler) {
                handler(event);
            }
        } catch (...) {
            std::lock_guard<std::mutex> lock(mutex_);
            lifecycle_exception_ = std::current_exception();
        }
    }

    void updateTargetCache(const ServiceLifecycleEvent &event) {
        const auto matches = [&](const TargetService &target) {
            return target.target_node_id == event.node_id &&
                target.target_service_id == event.service_id;
        };

        const auto it = std::find_if(target_services_.begin(), target_services_.end(), matches);
        if (event.type == ServiceLifecycleEventType::available) {
            const TargetService target{event.service_id, event.node_id, event.is_leader};
            if (it == target_services_.end()) {
                target_services_.push_back(target);
            } else {
                *it = target;
            }
        } else if (it != target_services_.end()) {
            target_services_.erase(it);
        }
    }

    ringloom_client_t *handle_ = nullptr;
    mutable std::mutex mutex_;
    std::vector<TargetService> target_services_;
    LifecycleHandler lifecycle_handler_;
    std::exception_ptr lifecycle_exception_;
};

class MessageConsumer {
public:
    using MessageHandler = std::function<void(const Message &)>;

    explicit MessageConsumer(ringloom_message_consumer_t *handle) : handle_(handle) {
        if (handle_ == nullptr) {
            throw std::invalid_argument("message consumer handle must not be null");
        }
    }

    ~MessageConsumer() {
        close();
    }

    MessageConsumer(const MessageConsumer &) = delete;
    MessageConsumer &operator=(const MessageConsumer &) = delete;
    MessageConsumer(MessageConsumer &&) = delete;
    MessageConsumer &operator=(MessageConsumer &&) = delete;

    int poll(const MessageHandler &handler, std::uint32_t limit = 256) {
        if (!handler) {
            throw std::invalid_argument("message handler must not be empty");
        }

        active_handler_ = &handler;
        callback_exception_ = nullptr;
        std::uint32_t count = 0;
        const Status status = toStatus(ringloom_message_consumer_poll(
            handle_,
            &MessageConsumer::dispatchMessage,
            this,
            limit,
            &count));
        active_handler_ = nullptr;
        last_status_ = status;

        if (callback_exception_) {
            std::rethrow_exception(callback_exception_);
        }
        if (!isOk(status)) {
            return -1;
        }
        return static_cast<int>(count);
    }

    Status lastStatus() const noexcept {
        return last_status_;
    }

    void close() noexcept {
        if (handle_ == nullptr) {
            return;
        }
        ringloom_message_consumer_destroy(handle_);
        handle_ = nullptr;
    }

private:
    static void dispatchMessage(void *user_data, const ringloom_message_t *native_message) noexcept {
        if (user_data == nullptr || native_message == nullptr) {
            return;
        }

        auto *self = static_cast<MessageConsumer *>(user_data);
        self->handleMessage(*native_message);
    }

    void handleMessage(const ringloom_message_t &native_message) noexcept {
        if (active_handler_ == nullptr || callback_exception_) {
            return;
        }

        try {
            Message message;
            message.correlation_id = native_message.correlation_id;
            message.source_node_id = native_message.source_node_id;
            message.source_service_id = native_message.source_service_id;
            message.target_node_id = native_message.target_node_id;
            message.target_service_id = native_message.target_service_id;
            message.template_id = native_message.template_id;
            message.flags = native_message.flags;
            message.payload = native_message.payload;
            message.payload_len = native_message.payload_len;
            (*active_handler_)(message);
        } catch (...) {
            callback_exception_ = std::current_exception();
        }
    }

    ringloom_message_consumer_t *handle_ = nullptr;
    const MessageHandler *active_handler_ = nullptr;
    std::exception_ptr callback_exception_;
    Status last_status_ = Status::ok;
};

class Service {
public:
    static Service start(const ServiceConfig &config) {
        if (config.service_name.empty()) {
            throw std::invalid_argument("service_name must not be empty");
        }

        ringloom_service_config_t native_config{};
        native_config.storage_path = config.storage_path.empty() ? nullptr : config.storage_path.data();
        native_config.storage_path_len = config.storage_path.size();
        native_config.group = config.group.empty() ? nullptr : config.group.data();
        native_config.group_len = config.group.size();
        native_config.service_name = config.service_name.data();
        native_config.service_name_len = config.service_name.size();
        native_config.broker_node_id = config.broker_node_id;
        native_config.blocking_mode = config.blocking_mode;
        native_config.heartbeat_timeout_ms = config.heartbeat_timeout_ms;
        native_config.control_buffer_length = config.control_buffer_length;
        native_config.messages_buffer_length = config.messages_buffer_length;
        native_config.leader_election_enabled = config.leader_election_enabled;

        ringloom_service_t *service = nullptr;
        const Status status = toStatus(ringloom_service_start(&native_config, &service));
        throwIfNotOk("ringloom_service_start", status);
        return Service(service);
    }

    explicit Service(ringloom_service_t *handle) : handle_(handle) {
        if (handle_ == nullptr) {
            throw std::invalid_argument("service handle must not be null");
        }
    }

    ~Service() {
        close();
    }

    Service(const Service &) = delete;
    Service &operator=(const Service &) = delete;
    Service(Service &&) = delete;
    Service &operator=(Service &&) = delete;

    std::int32_t serviceId() const {
        std::int32_t id = 0;
        throwIfNotOk("ringloom_service_id", toStatus(ringloom_service_id(handle_, &id)));
        return id;
    }

    std::int16_t nodeId() const {
        std::int16_t id = 0;
        throwIfNotOk("ringloom_service_node_id", toStatus(ringloom_service_node_id(handle_, &id)));
        return id;
    }

    std::uint32_t pollControl(std::uint32_t limit = 256) {
        std::uint32_t count = 0;
        throwIfNotOk(
            "ringloom_service_poll_control",
            toStatus(ringloom_service_poll_control(handle_, limit, &count)));
        return count;
    }

    Client createClient(std::string_view target_service_name) {
        ringloom_client_t *client = nullptr;
        const Status status = toStatus(ringloom_service_create_client(
            handle_,
            target_service_name.data(),
            target_service_name.size(),
            &client));
        throwIfNotOk("ringloom_service_create_client", status);
        return Client(client);
    }

    MessageConsumer messageConsumer() {
        ringloom_message_consumer_t *consumer = nullptr;
        const Status status = toStatus(ringloom_service_create_message_consumer(handle_, &consumer));
        throwIfNotOk("ringloom_service_create_message_consumer", status);
        return MessageConsumer(consumer);
    }

    std::string aeronDirectory() const {
        const char *directory = nullptr;
        std::size_t length = 0;
        throwIfNotOk(
            "ringloom_service_aeron_directory",
            toStatus(ringloom_service_aeron_directory(handle_, &directory, &length)));
        if (directory == nullptr || length == 0) {
            return {};
        }
        return std::string(directory, length);
    }

    std::int32_t aeronInboundStreamId() const {
        std::int32_t stream_id = 0;
        throwIfNotOk(
            "ringloom_service_aeron_inbound_stream_id",
            toStatus(ringloom_service_aeron_inbound_stream_id(handle_, &stream_id)));
        return stream_id;
    }

    bool publicationConnected() const {
        bool connected = false;
        throwIfNotOk(
            "ringloom_service_publication_connected",
            toStatus(ringloom_service_publication_connected(handle_, &connected)));
        return connected;
    }

    void stop() noexcept {
        if (handle_ != nullptr) {
            ringloom_service_stop(handle_);
        }
    }

    void close() noexcept {
        if (handle_ == nullptr) {
            return;
        }
        ringloom_service_destroy(handle_);
        handle_ = nullptr;
    }

private:
    ringloom_service_t *handle_ = nullptr;
};

} // namespace ringloom

#endif
