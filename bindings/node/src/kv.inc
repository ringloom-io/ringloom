// SPDX-License-Identifier: Apache-2.0
// Included inside addon.cc's anonymous namespace, before ClientWrap.
struct KvResource {
  ringloom_kv_reader_t* reader = nullptr;
  ringloom_kv_session_t* session = nullptr;
  ringloom_kv_writer_t* writer = nullptr;
  std::shared_ptr<KvResource> parent;
  std::shared_ptr<size_t> client_count, service_count;
  Napi::ObjectReference owner;
  Napi::FunctionReference on_state;
  // The mmap is never wrapped in a JS ArrayBuffer. KvBytes handles hold a raw
  // pointer that is valid only while borrowed is set and view_epoch matches.
  bool borrowed = false;
  uint64_t view_epoch = 0;
  std::vector<std::pair<uint8_t, uint64_t>> state_events;
  bool busy = false;
  bool closed = false;
  size_t children = 0;
  void Release() {
    // Invalidation is plain C++ state, so it is safe from finalizers and teardown.
    if (borrowed) { borrowed = false; ++view_epoch; }
    if (session) ringloom_kv_release(session);
  }
  void Close() {
    if (closed) return;
    closed = true;
    Release();
    if (session) ringloom_kv_session_free(session);
    if (reader) ringloom_kv_close_reader(reader);
    if (writer) ringloom_kv_close_writer(writer);
    session = nullptr; reader = nullptr; writer = nullptr;
    if (parent) { --parent->children; parent.reset(); }
    if (client_count) { --*client_count; client_count.reset(); }
    if (service_count) { --*service_count; service_count.reset(); }
    state_events.clear(); on_state.Reset(); owner.Reset();
  }
  ~KvResource() { Close(); }
};

bool KvUint(Napi::Env env, Napi::Value value, uint64_t max, uint64_t* out) {
  bool valid = false;
  if (value.IsBigInt()) {
    bool lossless; *out = value.As<Napi::BigInt>().Uint64Value(&lossless); valid = lossless && *out <= max;
  } else if (value.IsNumber()) {
    double n = value.As<Napi::Number>().DoubleValue();
    valid = std::isfinite(n) && n >= 0 && n <= 9007199254740991.0 && std::floor(n) == n && n <= static_cast<double>(max);
    if (valid) *out = static_cast<uint64_t>(n);
  }
  if (!valid) Napi::RangeError::New(env, "expected an unsigned integer in range (use BigInt for 64-bit values)").ThrowAsJavaScriptException();
  return valid;
}

bool KvOption(Napi::Object object, const char* key, uint64_t max, uint64_t* out) {
  auto v = object.Get(key);
  return v.IsUndefined() || KvUint(object.Env(), v, max, out);
}

// Never expose the mmap as a JS ArrayBuffer or typed array: LMDB maps it
// read-only, so a JS write would fault the process. This handle copies single
// bytes out and checks, on every access, that its snapshot is still borrowed.
// It holds only a weak reference, so it never keeps a session alive.
struct KvBytesInit { std::weak_ptr<KvResource> resource; const uint8_t* data; size_t length; uint64_t epoch; };
class KvBytes final : public Napi::ObjectWrap<KvBytes> {
 public:
  static Napi::FunctionReference constructor;
  static void Init(Napi::Env env) {
    auto f = DefineClass(env, "KvBytes", {
      InstanceAccessor("length", &KvBytes::Length, nullptr),
      InstanceMethod("at", &KvBytes::At),
    });
    constructor = Napi::Persistent(f); constructor.SuppressDestruct();
  }
  static Napi::Object New(Napi::Env env, KvBytesInit init) {
    return constructor.New({Napi::External<KvBytesInit>::New(env, &init)});
  }
  explicit KvBytes(const Napi::CallbackInfo& i) : Napi::ObjectWrap<KvBytes>(i) {
    if (!i.Length() || !i[0].IsExternal()) {
      Napi::TypeError::New(i.Env(), "internal KV bytes").ThrowAsJavaScriptException(); return;
    }
    init_ = *i[0].As<Napi::External<KvBytesInit>>().Data();
  }
 private:
  KvBytesInit init_{};
  size_t ValidLength() const {
    auto r = init_.resource.lock();
    return r && !r->closed && r->borrowed && r->view_epoch == init_.epoch ? init_.length : 0;
  }
  Napi::Value Length(const Napi::CallbackInfo& i) { return Napi::Number::New(i.Env(), ValidLength()); }
  Napi::Value At(const Napi::CallbackInfo& i) {
    double index = i.Length() ? i[0].ToNumber().DoubleValue() : 0;
    if (i.Env().IsExceptionPending()) return i.Env().Undefined();
    // Check validity after coercion: valueOf() may have released the session.
    const size_t length = ValidLength();
    index = std::isnan(index) ? 0 : std::trunc(index);
    if (index < 0) index += static_cast<double>(length);
    if (index < 0 || index >= static_cast<double>(length)) return i.Env().Undefined();
    return Napi::Number::New(i.Env(), init_.data[static_cast<size_t>(index)]);
  }
};
Napi::FunctionReference KvBytes::constructor;

class KvWrap final : public Napi::ObjectWrap<KvWrap> {
 public:
  static Napi::FunctionReference constructor;
  std::shared_ptr<KvResource> resource;
  static void Init(Napi::Env env, Napi::Object exports) {
    KvBytes::Init(env);
    auto f = DefineClass(env, "KvHandle", {InstanceMethod("call", &KvWrap::Call)});
    constructor = Napi::Persistent(f); constructor.SuppressDestruct();
  }
  explicit KvWrap(const Napi::CallbackInfo& i) : Napi::ObjectWrap<KvWrap>(i) {
    if (!i.Length() || !i[0].IsExternal()) { Napi::TypeError::New(i.Env(), "internal KV handle").ThrowAsJavaScriptException(); return; }
    resource = *i[0].As<Napi::External<std::shared_ptr<KvResource>>>().Data();
  }
  static Napi::Object New(Napi::Env env, std::shared_ptr<KvResource> r) {
    return constructor.New({Napi::External<std::shared_ptr<KvResource>>::New(env, &r)});
  }
  static void State(void* ctx, uint8_t state, uint64_t epoch) {
    auto* r = static_cast<KvResource*>(ctx);
    if (r->on_state.IsEmpty()) return;
    // Native transitions hold the KV context lock. Never enter JavaScript here.
    r->state_events.emplace_back(state, epoch);
  }
 private:
  struct VisitContext { Napi::Function callback; KvResource* resource; };
  static int Visit(void* ctx, const uint8_t* k, size_t kn, const uint8_t* v, size_t vn) {
    auto* c = static_cast<VisitContext*>(ctx); auto env = c->callback.Env();
    Napi::HandleScope scope(env);
    if (env.IsExceptionPending()) return 0;
    // Iteration copies are safe to retain and cannot mutate the read-only map.
    auto result = c->callback.Call({Napi::Buffer<uint8_t>::Copy(env, k, kn), Napi::Buffer<uint8_t>::Copy(env, v, vn)});
    return !env.IsExceptionPending() && !(result.IsBoolean() && !result.As<Napi::Boolean>().Value());
  }
  Napi::Value Call(const Napi::CallbackInfo& i) {
    auto env = i.Env(); auto r = resource;
    if (!r || !RequireOpen(env, r->closed, "KV handle")) return env.Undefined();
    if (!i.Length() || !i[0].IsString()) { Napi::TypeError::New(env, "KV operation required").ThrowAsJavaScriptException(); return env.Undefined(); }
    std::string op = i[0].As<Napi::String>();
    if (r->busy) { Napi::Error::New(env, "KV handle is busy in a callback").ThrowAsJavaScriptException(); return env.Undefined(); }
    if (op == "close") {
      if (r->children) { Napi::Error::New(env, "close KV sessions before closing their reader").ThrowAsJavaScriptException(); return env.Undefined(); }
      r->Close(); return env.Undefined();
    }
    if (op == "release" && r->session) { r->Release(); return env.Undefined(); }
    if (op == "newSession" && r->reader) {
      auto child = std::make_shared<KvResource>();
      int s = ringloom_kv_session_new(r->reader, &child->session);
      if (s) { ThrowStatusError(env, "kv_session_new", s); return env.Undefined(); }
      child->parent = r; ++r->children;
      return New(env, child);
    }
    if (op == "status" && r->reader) {
      ringloom_kv_status_t status{}; status.size = sizeof(status);
      int s = ringloom_kv_reader_status(r->reader, &status);
      if (s) { ThrowStatusError(env, "kv_reader_status", s); return env.Undefined(); }
      auto out = Napi::Object::New(env);
      out.Set("state", status.state); out.Set("replicationLive", bool(status.replication_live));
#define KV_STATUS(js, field) out.Set(js, Napi::BigInt::New(env, status.field))
      KV_STATUS("generation", generation); KV_STATUS("appliedIndex", applied_index);
      KV_STATUS("appliedLeaderEpoch", applied_leader_epoch); KV_STATUS("appliedAtNs", applied_at_ns);
      KV_STATUS("localLogTipIndex", local_log_tip_index); KV_STATUS("lagEntries", lag_entries);
      KV_STATUS("heartbeatAgeMs", heartbeat_age_ms); KV_STATUS("generationSwitches", generation_switches);
#undef KV_STATUS
      return out;
    }
    if (r->session) {
      if (r->borrowed) { Napi::Error::New(env, "release the borrowed view before another read").ThrowAsJavaScriptException(); return env.Undefined(); }
      if (op == "forEach") {
        if (i.Length() < 2 || !i[1].IsFunction()) { Napi::TypeError::New(env, "forEach requires a callback").ThrowAsJavaScriptException(); return env.Undefined(); }
        VisitContext ctx{i[1].As<Napi::Function>(), r.get()}; r->busy = true;
        int s = ringloom_kv_for_each(r->session, Visit, &ctx); r->busy = false;
        return Napi::Number::New(env, s);
      }
      PayloadView key{};
      if (i.Length() < 2 || !ReadPayload(env, i[1], &key)) return env.Undefined();
      size_t len = 0; int s;
      auto out = Napi::Object::New(env);
      if (op == "get") {
        PayloadView dest{};
        if (i.Length() < 3 || !ReadPayload(env, i[2], &dest)) return env.Undefined();
        s = ringloom_kv_get(r->session, key.data, key.length, const_cast<uint8_t*>(dest.data), dest.length, &len);
      } else if (op == "view") {
        const uint8_t* value = nullptr;
        s = ringloom_kv_view(r->session, key.data, key.length, &value, &len);
        if (s == 0) {
          r->borrowed = true;
          out.Set("value", KvBytes::New(env, {r, len ? value : nullptr, len, r->view_epoch}));
        } else out.Set("value", env.Null());
      } else { Napi::TypeError::New(env, "invalid session operation").ThrowAsJavaScriptException(); return env.Undefined(); }
      out.Set("status", s); out.Set("length", Napi::Number::New(env, len)); return out;
    }
    if (r->writer) {
      if (op == "state") return Napi::Number::New(env, ringloom_kv_writer_state(r->writer));
      if (op == "beginBatch") return Napi::Number::New(env, ringloom_kv_begin_batch(r->writer));
      if (op == "isAcked") {
        uint64_t seq; if (i.Length() < 2 || !KvUint(env, i[1], UINT64_MAX, &seq)) return env.Undefined();
        return Napi::Boolean::New(env, ringloom_kv_is_acked(r->writer, seq) != 0);
      }
      auto options = i.Length() > 3 && i[3].IsObject() ? i[3].As<Napi::Object>() : Napi::Object::New(env);
      uint64_t ack = 0; if (!KvOption(options, "ackMode", 1, &ack)) return env.Undefined();
      uint64_t seq = 0; int s; PayloadView key{}, value{};
      if (op == "put" || op == "delete") if (!ReadPayload(env, i[1], &key)) return env.Undefined();
      if (op == "put") {
        if (!ReadPayload(env, i[2], &value)) return env.Undefined();
        int64_t ttl = -1; auto t = options.Get("ttlMs");
        if (!t.IsNull() && !t.IsUndefined()) { uint64_t parsed; if (!KvUint(env, t, INT64_MAX, &parsed)) return env.Undefined(); ttl = static_cast<int64_t>(parsed); }
        s = ringloom_kv_put(r->writer, key.data, key.length, value.data, value.length, ttl, ack, &seq);
      } else if (op == "delete") s = ringloom_kv_delete(r->writer, key.data, key.length, ack, &seq);
      else if (op == "clear") s = ringloom_kv_clear(r->writer, ack, &seq);
      else if (op == "commitBatch") s = ringloom_kv_commit_batch(r->writer, ack, &seq);
      else { Napi::TypeError::New(env, "invalid writer operation").ThrowAsJavaScriptException(); return env.Undefined(); }
      auto out = Napi::Object::New(env); out.Set("status", s); out.Set("publishSeq", Napi::BigInt::New(env, seq)); return out;
    }
    Napi::TypeError::New(env, "invalid KV operation").ThrowAsJavaScriptException(); return env.Undefined();
  }
};
Napi::FunctionReference KvWrap::constructor;

using KvRegistry = std::vector<std::weak_ptr<KvResource>>;
void DrainKvStates(KvRegistry& registry, Napi::Env env) {
  std::vector<std::shared_ptr<KvResource>> pending;
  registry.erase(std::remove_if(registry.begin(), registry.end(), [&](auto& weak) {
    auto r = weak.lock();
    if (!r || r->closed) return true;
    pending.push_back(r); return false;
  }), registry.end());
  for (auto& r : pending) {
    if (r->busy || r->closed || r->on_state.IsEmpty()) continue;
    auto events = std::move(r->state_events); r->state_events.clear();
    r->busy = true;
    for (auto event : events) {
      Napi::HandleScope scope(env);
      r->on_state.Call({Napi::Number::New(env, event.first), Napi::BigInt::New(env, event.second)});
      if (env.IsExceptionPending()) break;
    }
    r->busy = false;
    if (env.IsExceptionPending()) return;
  }
}
