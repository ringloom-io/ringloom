// SPDX-License-Identifier: Apache-2.0
#include <napi.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "ringloom_service.h"

namespace {

constexpr uint32_t kExpectedAbiVersion = RINGLOOM_SERVICE_ABI_VERSION;

struct TargetInfo {
  int32_t service_id;
  int16_t node_id;
  bool is_leader;
};

std::string StatusName(int status) {
  const char* value = ringloom_status_string(static_cast<ringloom_status_t>(status));
  return value == nullptr ? "unknown" : value;
}

std::string AeronPublicationStatusName(int status) {
  const char* value =
      ringloom_aeron_publication_status_string(static_cast<ringloom_aeron_publication_status_t>(status));
  return value == nullptr ? "unknown" : value;
}

std::string LastErrorMessage() {
  const char* value = ringloom_last_error_message();
  return value == nullptr ? "" : value;
}

void ThrowStatusError(Napi::Env env, const char* action, int status) {
  const std::string status_name = StatusName(status);
  const std::string native_message = LastErrorMessage();
  Napi::Error error = Napi::Error::New(
      env,
      std::string(action) + " failed with " + status_name + " (" + std::to_string(status) +
          "): " + native_message);
  error.Set("statusCode", Napi::Number::New(env, status));
  error.Set("statusName", Napi::String::New(env, status_name));
  error.Set("nativeMessage", Napi::String::New(env, native_message));
  error.ThrowAsJavaScriptException();
}

bool ThrowIfNonOk(Napi::Env env, const char* action, ringloom_status_t status) {
  if (status == RINGLOOM_OK) {
    return false;
  }
  ThrowStatusError(env, action, static_cast<int>(status));
  return true;
}

bool RequireOpen(Napi::Env env, bool closed, const char* type_name) {
  if (!closed) {
    return true;
  }
  Napi::Error::New(env, std::string(type_name) + " is closed").ThrowAsJavaScriptException();
  return false;
}

std::string RequiredString(Napi::Env env, const Napi::Object& object, const char* key) {
  const Napi::Value value = object.Get(key);
  if (!value.IsString()) {
    Napi::TypeError::New(env, std::string(key) + " must be a string").ThrowAsJavaScriptException();
    return {};
  }
  std::string result = value.As<Napi::String>().Utf8Value();
  if (result.empty()) {
    Napi::TypeError::New(env, std::string(key) + " must not be empty").ThrowAsJavaScriptException();
    return {};
  }
  return result;
}

std::string OptionalString(const Napi::Object& object, const char* key, const char* fallback) {
  const Napi::Value value = object.Get(key);
  if (value.IsUndefined() || value.IsNull()) {
    return fallback;
  }
  return value.As<Napi::String>().Utf8Value();
}

int32_t OptionalInt32(const Napi::Object& object, const char* key, int32_t fallback) {
  const Napi::Value value = object.Get(key);
  if (value.IsUndefined() || value.IsNull()) {
    return fallback;
  }
  const int32_t parsed = value.As<Napi::Number>().Int32Value();
  return parsed == 0 ? fallback : parsed;
}

uint32_t OptionalUint32(const Napi::Object& object, const char* key, uint32_t fallback) {
  const Napi::Value value = object.Get(key);
  if (value.IsUndefined() || value.IsNull()) {
    return fallback;
  }
  const uint32_t parsed = value.As<Napi::Number>().Uint32Value();
  return parsed == 0 ? fallback : parsed;
}

size_t OptionalSize(const Napi::Object& object, const char* key, size_t fallback) {
  const Napi::Value value = object.Get(key);
  if (value.IsUndefined() || value.IsNull()) {
    return fallback;
  }
  const double parsed = value.As<Napi::Number>().DoubleValue();
  return parsed == 0 ? fallback : static_cast<size_t>(parsed);
}

bool OptionalBool(const Napi::Object& object, const char* key, bool fallback) {
  const Napi::Value value = object.Get(key);
  if (value.IsUndefined() || value.IsNull()) {
    return fallback;
  }
  return value.ToBoolean().Value();
}

struct PayloadView {
  const uint8_t* data;
  size_t length;
};

bool ReadPayload(Napi::Env env, const Napi::Value& value, PayloadView* out) {
  if (value.IsUndefined() || value.IsNull()) {
    out->data = nullptr;
    out->length = 0;
    return true;
  }

  if (value.IsBuffer()) {
    const Napi::Buffer<uint8_t> buffer = value.As<Napi::Buffer<uint8_t>>();
    out->data = buffer.Length() == 0 ? nullptr : buffer.Data();
    out->length = buffer.Length();
    return true;
  }

  if (value.IsTypedArray()) {
    const Napi::TypedArray typed_array = value.As<Napi::TypedArray>();
    const napi_typedarray_type type = typed_array.TypedArrayType();
    if (type != napi_uint8_array && type != napi_uint8_clamped_array) {
      Napi::TypeError::New(env, "payload must be a Buffer, Uint8Array, Uint8ClampedArray, null, or undefined")
          .ThrowAsJavaScriptException();
      return false;
    }
    Napi::ArrayBuffer array_buffer = typed_array.ArrayBuffer();
    auto* base = static_cast<uint8_t*>(array_buffer.Data());
    out->data = typed_array.ByteLength() == 0 ? nullptr : base + typed_array.ByteOffset();
    out->length = typed_array.ByteLength();
    return true;
  }

  Napi::TypeError::New(env, "payload must be a Buffer, Uint8Array, Uint8ClampedArray, null, or undefined")
      .ThrowAsJavaScriptException();
  return false;
}

class BufferClaimWrap;
class ClientWrap;
class MessageConsumerWrap;
class ServiceWrap;

class BufferClaimWrap final : public Napi::ObjectWrap<BufferClaimWrap> {
 public:
  static Napi::FunctionReference constructor;

  static void Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "BufferClaim", {
      InstanceMethod("payloadAddress", &BufferClaimWrap::PayloadAddress),
      InstanceMethod("payloadLength", &BufferClaimWrap::PayloadLength),
      InstanceMethod("payloadBuffer", &BufferClaimWrap::PayloadBuffer),
      InstanceMethod("active", &BufferClaimWrap::Active),
      InstanceMethod("commit", &BufferClaimWrap::Commit),
      InstanceMethod("abort", &BufferClaimWrap::Abort),
      InstanceMethod("close", &BufferClaimWrap::Close),
    });
    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();
    exports.Set("BufferClaim", func);
  }

  explicit BufferClaimWrap(const Napi::CallbackInfo& info)
      : Napi::ObjectWrap<BufferClaimWrap>(info), closed_(false) {
    ClearClaim();
  }

  ~BufferClaimWrap() override {
    CloseNative();
  }

  ringloom_buffer_claim_t* MutableClaim() {
    return &claim_;
  }

  bool IsClosed() const {
    return closed_;
  }

 private:
  ringloom_buffer_claim_t claim_{};
  bool closed_;

  void ClearClaim() {
    std::memset(&claim_, 0, sizeof(claim_));
  }

  void CloseNative() {
    if (closed_) {
      return;
    }
    if (claim_._active != 0) {
      ringloom_buffer_claim_abort(&claim_);
    }
    closed_ = true;
  }

  Napi::Value PayloadAddress(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "BufferClaim")) {
      return env.Undefined();
    }
    return Napi::BigInt::New(env, reinterpret_cast<uint64_t>(claim_.payload));
  }

  Napi::Value PayloadLength(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "BufferClaim")) {
      return env.Undefined();
    }
    return Napi::Number::New(env, static_cast<double>(claim_.payload_len));
  }

  Napi::Value PayloadBuffer(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "BufferClaim")) {
      return env.Undefined();
    }
    if (claim_._active == 0 || claim_.payload == nullptr || claim_.payload_len == 0) {
      return Napi::Buffer<uint8_t>::New(env, 0);
    }
    return Napi::Buffer<uint8_t>::New(env, claim_.payload, claim_.payload_len);
  }

  Napi::Value Active(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "BufferClaim")) {
      return env.Undefined();
    }
    return Napi::Boolean::New(env, claim_._active != 0);
  }

  Napi::Value Commit(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "BufferClaim")) {
      return env.Undefined();
    }
    const ringloom_status_t status = ringloom_buffer_claim_commit(&claim_);
    return Napi::Number::New(env, static_cast<int>(status));
  }

  Napi::Value Abort(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "BufferClaim")) {
      return env.Undefined();
    }
    const ringloom_status_t status = ringloom_buffer_claim_abort(&claim_);
    return Napi::Number::New(env, static_cast<int>(status));
  }

  Napi::Value Close(const Napi::CallbackInfo& info) {
    CloseNative();
    return info.Env().Undefined();
  }
};

Napi::FunctionReference BufferClaimWrap::constructor;

class ClientWrap final : public Napi::ObjectWrap<ClientWrap> {
 public:
  static Napi::FunctionReference constructor;

  static void Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "RingloomClient", {
      InstanceMethod("newClaim", &ClientWrap::NewClaim),
      InstanceMethod("tryClaim", &ClientWrap::TryClaim),
      InstanceMethod("tryClaimTo", &ClientWrap::TryClaimTo),
      InstanceMethod("send", &ClientWrap::Send),
      InstanceMethod("sendTo", &ClientWrap::SendTo),
      InstanceMethod("sendToLeader", &ClientWrap::SendToLeader),
      InstanceMethod("targetServices", &ClientWrap::TargetServices),
      InstanceMethod("lastAeronSendStatus", &ClientWrap::LastAeronSendStatus),
      InstanceMethod("onLifecycle", &ClientWrap::OnLifecycle),
      InstanceMethod("clearLifecycleHandler", &ClientWrap::ClearLifecycleHandler),
      InstanceMethod("close", &ClientWrap::Close),
    });
    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();
    exports.Set("RingloomClient", func);
  }

  explicit ClientWrap(const Napi::CallbackInfo& info)
      : Napi::ObjectWrap<ClientWrap>(info), client_(nullptr), closed_(false) {
    if (info.Length() != 1 || !info[0].IsExternal()) {
      Napi::TypeError::New(info.Env(), "RingloomClient instances are created by RingloomService")
          .ThrowAsJavaScriptException();
      return;
    }
    client_ = info[0].As<Napi::External<ringloom_client_t>>().Data();
  }

  ~ClientWrap() override {
    CloseNative();
  }

  ringloom_status_t InstallLifecycleHandler() {
    if (client_ == nullptr) {
      return RINGLOOM_ERR_INVALID_ARGUMENT;
    }
    const ringloom_status_t status = ringloom_client_set_lifecycle_handler(client_, &ClientWrap::LifecycleDispatch, this);
    if (status != RINGLOOM_OK) {
      return status;
    }
    return RefreshTargets();
  }

  void DestroyAfterFailedInit() {
    CloseNative();
  }

 private:
  ringloom_client_t* client_;
  bool closed_;
  std::vector<TargetInfo> targets_;
  Napi::FunctionReference lifecycle_handler_;

  static void LifecycleDispatch(void* user_data, const ringloom_service_lifecycle_event_t* event) {
    auto* self = static_cast<ClientWrap*>(user_data);
    if (self == nullptr || self->closed_) {
      return;
    }

    if (event == nullptr) {
      return;
    }
    self->UpdateTargetCache(*event);

    if (self->lifecycle_handler_.IsEmpty()) {
      return;
    }

    Napi::Env env = self->Env();
    Napi::Object js_event = Napi::Object::New(env);
    js_event.Set("eventType", Napi::Number::New(env, static_cast<int>(event->event_type)));
    js_event.Set(
        "type",
        Napi::String::New(
            env,
            event->event_type == RINGLOOM_SERVICE_AVAILABLE ? "available" : "unavailable"));
    js_event.Set("serviceId", Napi::Number::New(env, event->service_id));
    js_event.Set("nodeId", Napi::Number::New(env, event->node_id));
    js_event.Set("isLeader", Napi::Boolean::New(env, event->is_leader));
    if (event->service_name != nullptr && event->service_name_len > 0) {
      js_event.Set("serviceName", Napi::String::New(env, event->service_name, event->service_name_len));
    } else {
      js_event.Set("serviceName", Napi::String::New(env, ""));
    }
    self->lifecycle_handler_.Call({js_event});
  }

  void UpdateTargetCache(const ringloom_service_lifecycle_event_t& event) {
    auto matches = [&](const TargetInfo& target) {
      return target.node_id == event.node_id && target.service_id == event.service_id;
    };
    auto it = std::find_if(targets_.begin(), targets_.end(), matches);
    if (event.event_type == RINGLOOM_SERVICE_AVAILABLE) {
      const TargetInfo target{event.service_id, event.node_id, event.is_leader};
      if (it == targets_.end()) {
        targets_.push_back(target);
      } else {
        *it = target;
      }
    } else if (it != targets_.end()) {
      targets_.erase(it);
    }
  }

  ringloom_status_t RefreshTargets() {
    if (client_ == nullptr || closed_) {
      targets_.clear();
      return RINGLOOM_ERR_INVALID_ARGUMENT;
    }

    size_t count = 0;
    ringloom_status_t status = ringloom_client_list_targets(client_, nullptr, 0, &count);
    if (status != RINGLOOM_OK) {
      targets_.clear();
      return status;
    }

    if (count == 0) {
      targets_.clear();
      return RINGLOOM_OK;
    }

    std::vector<ringloom_client_target_t> native_targets(count);
    size_t actual_count = 0;
    status = ringloom_client_list_targets(client_, native_targets.data(), native_targets.size(), &actual_count);
    if (status != RINGLOOM_OK) {
      targets_.clear();
      return status;
    }

    targets_.clear();
    targets_.reserve(actual_count);
    for (size_t i = 0; i < actual_count; ++i) {
      targets_.push_back(TargetInfo{
        native_targets[i].target_service_id,
        native_targets[i].target_node_id,
        native_targets[i].is_leader,
      });
    }
    return RINGLOOM_OK;
  }

  void CloseNative() {
    if (closed_) {
      return;
    }
    closed_ = true;
    lifecycle_handler_.Reset();
    targets_.clear();
    if (client_ != nullptr) {
      ringloom_client_set_lifecycle_handler(client_, nullptr, nullptr);
      ringloom_client_destroy(client_);
      client_ = nullptr;
    }
  }

  Napi::Value NewClaim(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomClient")) {
      return env.Undefined();
    }
    return BufferClaimWrap::constructor.New({});
  }

  Napi::Value TryClaim(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomClient")) {
      return env.Undefined();
    }
    if (info.Length() < 3 || !info[0].IsNumber() || !info[1].IsNumber() || !info[2].IsObject()) {
      Napi::TypeError::New(env, "tryClaim(templateId, payloadLength, claim) is required")
          .ThrowAsJavaScriptException();
      return env.Undefined();
    }

    auto* claim = Napi::ObjectWrap<BufferClaimWrap>::Unwrap(info[2].As<Napi::Object>());
    if (claim == nullptr || claim->IsClosed()) {
      Napi::TypeError::New(env, "claim must be an open BufferClaim").ThrowAsJavaScriptException();
      return env.Undefined();
    }

    const uint16_t template_id = static_cast<uint16_t>(info[0].As<Napi::Number>().Uint32Value());
    const size_t payload_length = static_cast<size_t>(info[1].As<Napi::Number>().DoubleValue());
    const ringloom_status_t status = ringloom_client_try_claim(client_, template_id, payload_length, claim->MutableClaim());
    return Napi::Number::New(env, static_cast<int>(status));
  }

  Napi::Value TryClaimTo(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomClient")) {
      return env.Undefined();
    }
    if (info.Length() < 5 || !info[0].IsNumber() || !info[1].IsNumber() ||
        !info[2].IsNumber() || !info[3].IsNumber() || !info[4].IsObject()) {
      Napi::TypeError::New(env, "tryClaimTo(targetNodeId, targetServiceId, templateId, payloadLength, claim) is required")
          .ThrowAsJavaScriptException();
      return env.Undefined();
    }

    auto* claim = Napi::ObjectWrap<BufferClaimWrap>::Unwrap(info[4].As<Napi::Object>());
    if (claim == nullptr || claim->IsClosed()) {
      Napi::TypeError::New(env, "claim must be an open BufferClaim").ThrowAsJavaScriptException();
      return env.Undefined();
    }

    const int16_t target_node_id = static_cast<int16_t>(info[0].As<Napi::Number>().Int32Value());
    const int32_t target_service_id = info[1].As<Napi::Number>().Int32Value();
    const uint16_t template_id = static_cast<uint16_t>(info[2].As<Napi::Number>().Uint32Value());
    const size_t payload_length = static_cast<size_t>(info[3].As<Napi::Number>().DoubleValue());
    const ringloom_status_t status = ringloom_client_try_claim_to(
        client_, target_node_id, target_service_id, template_id, payload_length, claim->MutableClaim());
    return Napi::Number::New(env, static_cast<int>(status));
  }

  Napi::Value Send(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomClient")) {
      return env.Undefined();
    }
    PayloadView payload{};
    if (!ReadPayload(env, info.Length() > 0 ? info[0] : env.Undefined(), &payload)) {
      return env.Undefined();
    }
    const ringloom_status_t status = ringloom_client_send(client_, payload.data, payload.length);
    return Napi::Number::New(env, static_cast<int>(status));
  }

  Napi::Value SendTo(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomClient")) {
      return env.Undefined();
    }
    if (info.Length() < 3 || !info[0].IsNumber() || !info[1].IsNumber()) {
      Napi::TypeError::New(env, "sendTo(targetNodeId, targetServiceId, payload) is required")
          .ThrowAsJavaScriptException();
      return env.Undefined();
    }
    PayloadView payload{};
    if (!ReadPayload(env, info[2], &payload)) {
      return env.Undefined();
    }
    const int16_t target_node_id = static_cast<int16_t>(info[0].As<Napi::Number>().Int32Value());
    const int32_t target_service_id = info[1].As<Napi::Number>().Int32Value();
    const ringloom_status_t status =
        ringloom_client_send_to(client_, target_node_id, target_service_id, payload.data, payload.length);
    return Napi::Number::New(env, static_cast<int>(status));
  }

  Napi::Value SendToLeader(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomClient")) {
      return env.Undefined();
    }
    PayloadView payload{};
    if (!ReadPayload(env, info.Length() > 0 ? info[0] : env.Undefined(), &payload)) {
      return env.Undefined();
    }
    const ringloom_status_t status = ringloom_client_send_to_leader(client_, payload.data, payload.length);
    return Napi::Number::New(env, static_cast<int>(status));
  }

  Napi::Value TargetServices(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomClient")) {
      return env.Undefined();
    }
    const ringloom_status_t status = RefreshTargets();
    if (ThrowIfNonOk(env, "ringloom_client_list_targets", status)) {
      return env.Undefined();
    }

    Napi::Array result = Napi::Array::New(env, targets_.size());
    for (size_t i = 0; i < targets_.size(); ++i) {
      Napi::Object target = Napi::Object::New(env);
      target.Set("targetServiceId", Napi::Number::New(env, targets_[i].service_id));
      target.Set("targetNodeId", Napi::Number::New(env, targets_[i].node_id));
      target.Set("isLeader", Napi::Boolean::New(env, targets_[i].is_leader));
      result.Set(i, target);
    }
    return result;
  }

  Napi::Value LastAeronSendStatus(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomClient")) {
      return env.Undefined();
    }

    ringloom_aeron_publication_status_t status = RINGLOOM_AERON_PUBLICATION_UNKNOWN;
    const ringloom_status_t call_status = ringloom_client_last_aeron_send_status(client_, &status);
    if (ThrowIfNonOk(env, "ringloom_client_last_aeron_send_status", call_status)) {
      return env.Undefined();
    }
    return Napi::Number::New(env, static_cast<int>(status));
  }

  Napi::Value OnLifecycle(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomClient")) {
      return env.Undefined();
    }
    if (info.Length() < 1 || !info[0].IsFunction()) {
      Napi::TypeError::New(env, "onLifecycle(handler) requires a function").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    lifecycle_handler_.Reset(info[0].As<Napi::Function>(), 1);
    return env.Undefined();
  }

  Napi::Value ClearLifecycleHandler(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomClient")) {
      return env.Undefined();
    }
    lifecycle_handler_.Reset();
    return env.Undefined();
  }

  Napi::Value Close(const Napi::CallbackInfo& info) {
    CloseNative();
    return info.Env().Undefined();
  }
};

Napi::FunctionReference ClientWrap::constructor;

class MessageConsumerWrap final : public Napi::ObjectWrap<MessageConsumerWrap> {
 public:
  static Napi::FunctionReference constructor;

  static void Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "MessageConsumer", {
      InstanceMethod("poll", &MessageConsumerWrap::Poll),
      InstanceMethod("lastStatus", &MessageConsumerWrap::LastStatus),
      InstanceMethod("close", &MessageConsumerWrap::Close),
    });
    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();
    exports.Set("MessageConsumer", func);
  }

  explicit MessageConsumerWrap(const Napi::CallbackInfo& info)
      : Napi::ObjectWrap<MessageConsumerWrap>(info),
        consumer_(nullptr),
        closed_(false),
        last_status_(RINGLOOM_OK) {
    if (info.Length() != 1 || !info[0].IsExternal()) {
      Napi::TypeError::New(info.Env(), "MessageConsumer instances are created by RingloomService")
          .ThrowAsJavaScriptException();
      return;
    }
    consumer_ = info[0].As<Napi::External<ringloom_message_consumer_t>>().Data();
  }

  ~MessageConsumerWrap() override {
    CloseNative();
  }

 private:
  ringloom_message_consumer_t* consumer_;
  bool closed_;
  ringloom_status_t last_status_;
  Napi::FunctionReference current_handler_;

  static void Dispatch(void* user_data, const ringloom_message_t* message) {
    auto* self = static_cast<MessageConsumerWrap*>(user_data);
    if (self == nullptr || self->closed_ || self->current_handler_.IsEmpty() || message == nullptr) {
      return;
    }

    Napi::Env env = self->Env();
    Napi::Object js_message = Napi::Object::New(env);
    js_message.Set("correlationId", Napi::BigInt::New(env, static_cast<int64_t>(message->correlation_id)));
    js_message.Set("sourceNodeId", Napi::Number::New(env, message->source_node_id));
    js_message.Set("sourceServiceId", Napi::Number::New(env, message->source_service_id));
    js_message.Set("targetNodeId", Napi::Number::New(env, message->target_node_id));
    js_message.Set("targetServiceId", Napi::Number::New(env, message->target_service_id));
    js_message.Set("templateId", Napi::Number::New(env, message->template_id));
    js_message.Set("flags", Napi::Number::New(env, message->flags));
    js_message.Set("payloadAddress", Napi::BigInt::New(env, reinterpret_cast<uint64_t>(message->payload)));
    js_message.Set("payloadLength", Napi::Number::New(env, static_cast<double>(message->payload_len)));
    if (message->payload != nullptr && message->payload_len > 0) {
      js_message.Set(
          "payload",
          Napi::Buffer<uint8_t>::New(env, const_cast<uint8_t*>(message->payload), message->payload_len));
    } else {
      js_message.Set("payload", Napi::Buffer<uint8_t>::New(env, 0));
    }

    self->current_handler_.Call({js_message});
  }

  void CloseNative() {
    if (closed_) {
      return;
    }
    closed_ = true;
    current_handler_.Reset();
    if (consumer_ != nullptr) {
      ringloom_message_consumer_destroy(consumer_);
      consumer_ = nullptr;
    }
  }

  Napi::Value Poll(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "MessageConsumer")) {
      return env.Undefined();
    }
    if (info.Length() < 1 || !info[0].IsFunction()) {
      Napi::TypeError::New(env, "poll(handler, limit) requires a function handler").ThrowAsJavaScriptException();
      return env.Undefined();
    }

    const uint32_t limit = info.Length() > 1 && info[1].IsNumber()
        ? info[1].As<Napi::Number>().Uint32Value()
        : 256;
    uint32_t count = 0;
    current_handler_.Reset(info[0].As<Napi::Function>(), 1);
    last_status_ = ringloom_message_consumer_poll(consumer_, &MessageConsumerWrap::Dispatch, this, limit, &count);
    if (last_status_ != RINGLOOM_OK) {
      return Napi::Number::New(env, -1);
    }
    return Napi::Number::New(env, count);
  }

  Napi::Value LastStatus(const Napi::CallbackInfo& info) {
    return Napi::Number::New(info.Env(), static_cast<int>(last_status_));
  }

  Napi::Value Close(const Napi::CallbackInfo& info) {
    CloseNative();
    return info.Env().Undefined();
  }
};

Napi::FunctionReference MessageConsumerWrap::constructor;

class ServiceWrap final : public Napi::ObjectWrap<ServiceWrap> {
 public:
  static Napi::FunctionReference constructor;

  static void Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "RingloomService", {
      StaticMethod("start", &ServiceWrap::Start),
      InstanceMethod("serviceId", &ServiceWrap::ServiceId),
      InstanceMethod("nodeId", &ServiceWrap::NodeId),
      InstanceMethod("pollControl", &ServiceWrap::PollControl),
      InstanceMethod("createClient", &ServiceWrap::CreateClient),
      InstanceMethod("messageConsumer", &ServiceWrap::MessageConsumer),
      InstanceMethod("aeronDirectory", &ServiceWrap::AeronDirectory),
      InstanceMethod("aeronInboundStreamId", &ServiceWrap::AeronInboundStreamId),
      InstanceMethod("publicationConnected", &ServiceWrap::PublicationConnected),
      InstanceMethod("stop", &ServiceWrap::Stop),
      InstanceMethod("close", &ServiceWrap::Close),
    });
    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();
    exports.Set("RingloomService", func);
  }

  explicit ServiceWrap(const Napi::CallbackInfo& info)
      : Napi::ObjectWrap<ServiceWrap>(info), service_(nullptr), closed_(false) {
    if (info.Length() != 1 || !info[0].IsExternal()) {
      Napi::TypeError::New(info.Env(), "RingloomService instances are created by RingloomService.start")
          .ThrowAsJavaScriptException();
      return;
    }
    service_ = info[0].As<Napi::External<ringloom_service_t>>().Data();
  }

  ~ServiceWrap() override {
    CloseNative();
  }

 private:
  ringloom_service_t* service_;
  bool closed_;

  static Napi::Value Start(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (ringloom_service_abi_version() != kExpectedAbiVersion) {
      Napi::Error::New(env, "Unsupported RingLoom native ABI version").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    if (info.Length() < 1 || !info[0].IsObject()) {
      Napi::TypeError::New(env, "RingloomService.start(config) requires a config object")
          .ThrowAsJavaScriptException();
      return env.Undefined();
    }

    Napi::Object config = info[0].As<Napi::Object>();
    const std::string service_name = RequiredString(env, config, "serviceName");
    if (env.IsExceptionPending()) {
      return env.Undefined();
    }
    const std::string storage_path = OptionalString(config, "storagePath", "/dev/shm");
    const std::string group = OptionalString(config, "group", "default");

    ringloom_service_config_t native_config{};
    native_config.storage_path = storage_path.data();
    native_config.storage_path_len = storage_path.size();
    native_config.group = group.data();
    native_config.group_len = group.size();
    native_config.service_name = service_name.data();
    native_config.service_name_len = service_name.size();
    native_config.broker_node_id = static_cast<int16_t>(OptionalInt32(config, "brokerNodeId", 1));
    native_config.blocking_mode = OptionalBool(config, "blockingMode", false);
    native_config.heartbeat_timeout_ms = static_cast<int32_t>(OptionalUint32(config, "heartbeatTimeoutMillis", 10000));
    native_config.control_buffer_length = OptionalSize(config, "controlBufferLength", 65536);
    native_config.messages_buffer_length = OptionalSize(config, "messagesBufferLength", 1048576);
    native_config.leader_election_enabled = OptionalBool(config, "leaderElectionEnabled", false);

    ringloom_service_t* service = nullptr;
    const ringloom_status_t status = ringloom_service_start(&native_config, &service);
    if (ThrowIfNonOk(env, "ringloom_service_start", status)) {
      return env.Undefined();
    }
    return constructor.New({Napi::External<ringloom_service_t>::New(env, service)});
  }

  void CloseNative() {
    if (closed_) {
      return;
    }
    closed_ = true;
    if (service_ != nullptr) {
      ringloom_service_destroy(service_);
      service_ = nullptr;
    }
  }

  Napi::Value ServiceId(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomService")) {
      return env.Undefined();
    }
    int32_t service_id = 0;
    const ringloom_status_t status = ringloom_service_id(service_, &service_id);
    if (ThrowIfNonOk(env, "ringloom_service_id", status)) {
      return env.Undefined();
    }
    return Napi::Number::New(env, service_id);
  }

  Napi::Value NodeId(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomService")) {
      return env.Undefined();
    }
    int16_t node_id = 0;
    const ringloom_status_t status = ringloom_service_node_id(service_, &node_id);
    if (ThrowIfNonOk(env, "ringloom_service_node_id", status)) {
      return env.Undefined();
    }
    return Napi::Number::New(env, node_id);
  }

  Napi::Value PollControl(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomService")) {
      return env.Undefined();
    }
    const uint32_t limit = info.Length() > 0 && info[0].IsNumber()
        ? info[0].As<Napi::Number>().Uint32Value()
        : 256;
    uint32_t count = 0;
    const ringloom_status_t status = ringloom_service_poll_control(service_, limit, &count);
    if (ThrowIfNonOk(env, "ringloom_service_poll_control", status)) {
      return env.Undefined();
    }
    return Napi::Number::New(env, count);
  }

  Napi::Value CreateClient(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomService")) {
      return env.Undefined();
    }
    if (info.Length() < 1 || !info[0].IsString()) {
      Napi::TypeError::New(env, "createClient(targetServiceName) requires a string")
          .ThrowAsJavaScriptException();
      return env.Undefined();
    }
    const std::string target = info[0].As<Napi::String>().Utf8Value();
    if (target.empty()) {
      Napi::TypeError::New(env, "targetServiceName must not be empty").ThrowAsJavaScriptException();
      return env.Undefined();
    }

    ringloom_client_t* client = nullptr;
    const ringloom_status_t status = ringloom_service_create_client(service_, target.data(), target.size(), &client);
    if (ThrowIfNonOk(env, "ringloom_service_create_client", status)) {
      return env.Undefined();
    }

    Napi::Object object = ClientWrap::constructor.New({Napi::External<ringloom_client_t>::New(env, client)});
    auto* wrapper = Napi::ObjectWrap<ClientWrap>::Unwrap(object);
    const ringloom_status_t install_status = wrapper->InstallLifecycleHandler();
    if (install_status != RINGLOOM_OK) {
      wrapper->DestroyAfterFailedInit();
      ThrowStatusError(env, "ringloom_client_set_lifecycle_handler", static_cast<int>(install_status));
      return env.Undefined();
    }
    return object;
  }

  Napi::Value MessageConsumer(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomService")) {
      return env.Undefined();
    }
    ringloom_message_consumer_t* consumer = nullptr;
    const ringloom_status_t status = ringloom_service_create_message_consumer(service_, &consumer);
    if (ThrowIfNonOk(env, "ringloom_service_create_message_consumer", status)) {
      return env.Undefined();
    }
    return MessageConsumerWrap::constructor.New({Napi::External<ringloom_message_consumer_t>::New(env, consumer)});
  }

  Napi::Value AeronDirectory(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomService")) {
      return env.Undefined();
    }

    const char* directory = nullptr;
    size_t directory_len = 0;
    const ringloom_status_t status = ringloom_service_aeron_directory(service_, &directory, &directory_len);
    if (ThrowIfNonOk(env, "ringloom_service_aeron_directory", status)) {
      return env.Undefined();
    }
    return directory == nullptr || directory_len == 0
        ? Napi::String::New(env, "")
        : Napi::String::New(env, directory, directory_len);
  }

  Napi::Value AeronInboundStreamId(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomService")) {
      return env.Undefined();
    }

    int32_t stream_id = 0;
    const ringloom_status_t status = ringloom_service_aeron_inbound_stream_id(service_, &stream_id);
    if (ThrowIfNonOk(env, "ringloom_service_aeron_inbound_stream_id", status)) {
      return env.Undefined();
    }
    return Napi::Number::New(env, stream_id);
  }

  Napi::Value PublicationConnected(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!RequireOpen(env, closed_, "RingloomService")) {
      return env.Undefined();
    }

    bool connected = false;
    const ringloom_status_t status = ringloom_service_publication_connected(service_, &connected);
    if (ThrowIfNonOk(env, "ringloom_service_publication_connected", status)) {
      return env.Undefined();
    }
    return Napi::Boolean::New(env, connected);
  }

  Napi::Value Stop(const Napi::CallbackInfo& info) {
    if (!RequireOpen(info.Env(), closed_, "RingloomService")) {
      return info.Env().Undefined();
    }
    ringloom_service_stop(service_);
    return info.Env().Undefined();
  }

  Napi::Value Close(const Napi::CallbackInfo& info) {
    CloseNative();
    return info.Env().Undefined();
  }
};

Napi::FunctionReference ServiceWrap::constructor;

Napi::Value AbiVersion(const Napi::CallbackInfo& info) {
  return Napi::Number::New(info.Env(), ringloom_service_abi_version());
}

Napi::Value StatusNameValue(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsNumber()) {
    Napi::TypeError::New(env, "statusName(status) requires a numeric status").ThrowAsJavaScriptException();
    return env.Undefined();
  }
  return Napi::String::New(env, StatusName(info[0].As<Napi::Number>().Int32Value()));
}

Napi::Value LastErrorMessageValue(const Napi::CallbackInfo& info) {
  return Napi::String::New(info.Env(), LastErrorMessage());
}

Napi::Value AeronPublicationStatusNameValue(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsNumber()) {
    Napi::TypeError::New(env, "aeronPublicationStatusName(status) requires a numeric status")
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }
  return Napi::String::New(env, AeronPublicationStatusName(info[0].As<Napi::Number>().Int32Value()));
}

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  BufferClaimWrap::Init(env, exports);
  ClientWrap::Init(env, exports);
  MessageConsumerWrap::Init(env, exports);
  ServiceWrap::Init(env, exports);
  exports.Set("abiVersion", Napi::Function::New(env, AbiVersion));
  exports.Set("statusName", Napi::Function::New(env, StatusNameValue));
  exports.Set("aeronPublicationStatusName", Napi::Function::New(env, AeronPublicationStatusNameValue));
  exports.Set("lastErrorMessage", Napi::Function::New(env, LastErrorMessageValue));
  return exports;
}

}  // namespace

NODE_API_MODULE(ringloom_node, InitAll)
