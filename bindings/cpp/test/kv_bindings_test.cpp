// SPDX-License-Identifier: Apache-2.0
// Deterministic C ABI contract tests; no broker or native library required.
#include <ringloom/service.hpp>
#include <cassert>
#include <type_traits>

struct ringloom_kv_reader {};
struct ringloom_kv_session { bool active = false; int cursor = 0; };
struct ringloom_kv_writer {};
static int readers_closed, sessions_closed, writers_closed, releases;
static std::int64_t ttl;
static std::uint8_t ack;
static bool batch;
static const std::string binary("v\0x", 3);
extern "C" {
const char *ringloom_status_string(ringloom_status_t) { return "test"; }
const char *ringloom_last_error_message() { return ""; }
void ringloom_kv_store_config_init(ringloom_kv_store_config_t *c) { *c = {}; c->size = sizeof(*c); c->max_key_len = 511; }
void ringloom_kv_reader_options_init(ringloom_kv_reader_options_t *o) { *o = {}; o->size = sizeof(*o); }
void ringloom_kv_writer_options_init(ringloom_kv_writer_options_t *o) { *o = {}; o->size = sizeof(*o); o->max_frame_length = 65536; }
void ringloom_kv_close_reader(ringloom_kv_reader_t *p) { if(p) { ++readers_closed; delete p; } }
int ringloom_kv_reader_status(const ringloom_kv_reader_t *p, ringloom_kv_status_t *s) {
    assert(s->size == sizeof(*s)); if (!p) return RINGLOOM_ERR_INVALID_ARGUMENT;
    s->generation = 7; return RINGLOOM_OK;
}
int ringloom_kv_session_new(ringloom_kv_reader_t *p, ringloom_kv_session_t **out) {
    if (!p) return RINGLOOM_ERR_INVALID_ARGUMENT;
    *out = new ringloom_kv_session; return 0;
}
void ringloom_kv_session_free(ringloom_kv_session_t *p) { assert(!p->active); ++sessions_closed; delete p; }
void ringloom_kv_release(ringloom_kv_session_t *p) { assert(!p->active); ++releases; }
int ringloom_kv_get(ringloom_kv_session_t *, const uint8_t *k, size_t kn, uint8_t *out, size_t cap, size_t *n) {
    std::string_view key(reinterpret_cast<const char *>(k), kn);
    if (key == "missing") { *n = 0; return RINGLOOM_KV_MISSING; }
    *n = key == "empty" ? 0 : binary.size();
    if (cap < *n) return RINGLOOM_ERR_KV_BUFFER_TOO_SMALL;
    if (*n) std::memcpy(out, binary.data(), *n);
    return 0;
}
int ringloom_kv_view(ringloom_kv_session_t *, const uint8_t *, size_t, const uint8_t **out, size_t *n) {
    *out = reinterpret_cast<const uint8_t *>(binary.data()); *n = binary.size(); return 0;
}
int ringloom_kv_for_each(ringloom_kv_session_t *p, ringloom_kv_visit_fn fn, void *ctx) {
    assert(p && !p->active);
    p->active = true;
    const uint8_t key[] = {'k', 0};
    if (fn(ctx, key, 2, nullptr, 0)) fn(ctx, key, 2, reinterpret_cast<const uint8_t *>(binary.data()), 3);
    ++p->cursor; // Native cursor remains in use after the callback returns.
    p->active = false;
    return 0;
}
int ringloom_kv_put(ringloom_kv_writer_t *, const uint8_t *k, size_t kn, const uint8_t *v, size_t vn,
                    int64_t t, uint8_t a, uint64_t *seq) {
    assert(kn == 2 && k[1] == 0 && vn == 3 && v[1] == 0);
    ttl = t; ack = a; *seq = batch ? 0 : 42; return 0;
}
int ringloom_kv_delete(ringloom_kv_writer_t *, const uint8_t *, size_t, uint8_t a, uint64_t *s) { ack = a; *s = 43; return 0; }
int ringloom_kv_clear(ringloom_kv_writer_t *, uint8_t a, uint64_t *s) { ack = a; *s = 44; return 0; }
int ringloom_kv_begin_batch(ringloom_kv_writer_t *) { batch = true; return 0; }
int ringloom_kv_commit_batch(ringloom_kv_writer_t *, uint8_t a, uint64_t *s) { batch = false; ack = a; *s = 45; return 0; }
int ringloom_kv_is_acked(ringloom_kv_writer_t *, uint64_t s) { return s == 45; }
int ringloom_kv_writer_state(const ringloom_kv_writer_t *) { return RINGLOOM_KV_GRANTED; }
void ringloom_kv_close_writer(ringloom_kv_writer_t *p) { ++writers_closed; delete p; }
}
int main() {
    static_assert(!std::is_copy_constructible_v<ringloom::KvSession>);
    static_assert(!std::is_move_constructible_v<ringloom::KvWriter>);
    ringloom::KvStoreConfig config;
    ringloom::KvReaderOptions ro;
    ringloom::KvWriterOptions wo;
    assert(config.size == sizeof(ringloom_kv_store_config_t) && config.max_key_len == 511);
    assert(ro.size == sizeof(ringloom_kv_reader_options_t) && wo.max_frame_length == 65536);
    ringloom::KvReader reader(new ringloom_kv_reader);
    assert(reader.status().generation == 7);
    auto session = reader.newSession();
    reader.close(); reader.close(); assert(readers_closed == 0);
    char out[3];
    auto short_read = session.get("k", out, 1);
    assert(short_read.status == ringloom::Status::kv_buffer_too_small && short_read.length == 3);
    assert(session.get("missing", out, 3).status == ringloom::Status::kv_missing);
    assert(session.get("empty", nullptr, 0).status == ringloom::Status::ok);
    assert(session.get("k", out, 3).status == ringloom::Status::ok && std::string(out, 3) == binary);
    assert(session.view("k").value == binary);
    session.release(); assert(releases == 1);
    int visits = 0;
    assert(session.forEach([&](auto key, auto value) { assert(key.size() == 2); assert(value.empty()); ++visits; return false; }) == ringloom::Status::ok);
    assert(visits == 1);
    bool caught = false;
    try { session.forEach([](auto, auto) -> bool { throw std::runtime_error("visitor"); }); }
    catch (const std::runtime_error &) { caught = true; }
    assert(caught);
    session.close(); session.close(); assert(sessions_closed == 1 && readers_closed == 1);
    static_assert(!std::is_move_constructible_v<ringloom::KvSession>);
    static_assert(!std::is_move_assignable_v<ringloom::KvSession>);
    // The ABI double asserts that no release/free/reentry reaches an active cursor.
    for (bool throws : {false, true}) {
        for (int operation = 0; operation != 3; ++operation) {
            ringloom::KvReader owner(new ringloom_kv_reader);
            auto active = std::unique_ptr<ringloom::KvSession>(new ringloom::KvSession(owner.newSession()));
            owner.close();
            const int readers_before = readers_closed;
            const int closed_before = sessions_closed;
            const int released_before = releases;
            bool callback_ran = false;
            bool threw = false;
            try {
                assert(active->forEach([&](auto, auto) -> bool {
                    callback_ran = true;
                    assert(active->forEach([](auto, auto) { assert(false); return true; })
                        == ringloom::Status::invalid_argument);
                    char storage[3];
                    assert(active->get("k", storage, 3).status == ringloom::Status::invalid_argument);
                    assert(active->view("k").status == ringloom::Status::invalid_argument);
                    active->release(); active->release();
                    assert(releases == released_before);
                    if (operation == 1) {
                        active->close(); active->close();
                        assert(active->forEach([](auto, auto) { return true; })
                            == ringloom::Status::invalid_argument);
                    } else if (operation == 2) {
                        active.reset();
                    }
                    assert(sessions_closed == closed_before);
                    assert(readers_closed == readers_before);
                    if (throws) throw std::runtime_error("deferred cleanup");
                    return false;
                }) == ringloom::Status::ok);
            } catch (const std::runtime_error &) { threw = true; }
            assert(callback_ran && threw == throws);
            assert(releases == released_before + 1);
            assert(sessions_closed == closed_before + (operation != 0));
            if (operation == 0) {
                // Busy state must be cleared on both normal and exceptional exit.
                assert(active->forEach([](auto, auto) { return false; }) == ringloom::Status::ok);
                assert(active->view("k").value == binary);
                active->close();
            }
            assert(sessions_closed == closed_before + 1);
            assert(readers_closed == readers_before + 1);
        }
    }
    {
        ringloom::KvReader owner(new ringloom_kv_reader);
        auto active = owner.newSession();
        const int released_before = releases;
        int calls = 0;
        assert(active.forEach([&](auto, auto) {
            ++calls;
            active.release();
            assert(releases == released_before);
            return true;
        }) == ringloom::Status::ok);
        assert(calls == 2 && releases == released_before + 1);
    }
    ringloom::KvWriter writer(new ringloom_kv_writer);
    const std::string key("k\0", 2);
    assert(writer.put(key, binary).sequence == 42 && ttl == -1);
    writer.put(key, binary, 0, ringloom::TopicAckMode::replicate_once); assert(ttl == 0 && ack == 1);
    writer.put(key, binary, 123); assert(ttl == 123);
    assert(writer.beginBatch() == ringloom::Status::ok);
    assert(writer.put(key, binary).sequence == 0);
    assert(writer.commitBatch(ringloom::TopicAckMode::replicate_once).sequence == 45 && ack == 1);
    assert(writer.isAcked(45) && !writer.isAcked(46));
    assert(writer.erase(key).sequence == 43 && writer.clear().sequence == 44);
    assert(writer.state() == ringloom::KvWriterState::granted);
    writer.close(); writer.close(); assert(writers_closed == 1);
}
