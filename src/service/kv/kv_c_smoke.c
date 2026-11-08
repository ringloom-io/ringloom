/* SPDX-License-Identifier: Apache-2.0
 * Standalone integration check against a running KV-enabled broker, node 1,
 * group k07-smoke. argv[1] is broker.storage.path. Example compile:
 * cc -Iinclude src/service/kv/kv_c_smoke.c -Lzig-out/lib -lringloom_service \
 *    -Wl,-rpath,$PWD/zig-out/lib -o /tmp/ringloom-kv-smoke
 */
#define _DEFAULT_SOURCE
#include "ringloom_service.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

_Static_assert(sizeof(ringloom_kv_store_config_t) == 80, "config ABI");
_Static_assert(sizeof(ringloom_kv_reader_options_t) == 16, "reader options ABI");
_Static_assert(sizeof(ringloom_kv_writer_options_t) == 32, "writer options ABI");
_Static_assert(sizeof(ringloom_kv_status_t) == 72, "status ABI");
static ringloom_service_t *service;
static void check(int status, const char *where) {
    if (status) {
        fprintf(stderr, "%s: %d %s\n", where, status, ringloom_last_error_message());
        exit(1);
    }
}
#define OK(expr) check((expr), #expr)
#define REQUIRE(expr) do { if (!(expr)) { fprintf(stderr, "failed: %s line %d\n", #expr, __LINE__); exit(1); } } while (0)
static void poll_control(void) {
    uint32_t n;
    OK(ringloom_service_poll_control(service, 64, &n));
    usleep(1000);
}
static void wait_value(ringloom_kv_session_t *session, const char *key, const void *value, size_t size) {
    uint8_t out[65536];
    size_t n;
    for (int i=0; i<5000; i++) {
        int status = ringloom_kv_get(session, (const uint8_t*)key, strlen(key), out, sizeof(out), &n);
        if (!status && n==size && !memcmp(out,value,size)) return;
        poll_control();
    }
    fprintf(stderr, "value failed to converge for %s\n",key); exit(1);
}
static int visit(void *ctx, const uint8_t *key, size_t key_len, const uint8_t *val, size_t val_len) {
    (void)key; (void)key_len; (void)val; (void)val_len;
    (*(int*)ctx)++;
    return 1;
}
static uint64_t put(ringloom_kv_writer_t *w, const char *key, const void *value, size_t n, int64_t ttl) {
    for (int i=0; i<5000; i++) {
        uint64_t seq=0;
        int s=ringloom_kv_put(w,(const uint8_t*)key,strlen(key),value,n,ttl,1,&seq);
        if (!s) return seq;
        REQUIRE(s==RINGLOOM_ERR_BACKPRESSURE || s==RINGLOOM_ERR_TOPIC_LEADER_UNAVAILABLE);
        poll_control();
    }
    REQUIRE(0); return 0;
}
int main(int argc, char **argv) {
    REQUIRE(argc==2);
    REQUIRE(ringloom_service_abi_version()==RINGLOOM_SERVICE_ABI_VERSION);
    ringloom_service_config_t sc={0};
    sc.storage_path=argv[1]; sc.storage_path_len=strlen(argv[1]);
    sc.group="k07-smoke"; sc.group_len=strlen(sc.group);
    sc.service_name="kv-smoke"; sc.service_name_len=strlen(sc.service_name);
    sc.broker_node_id=1;
    OK(ringloom_service_start(&sc,&service));
    ringloom_client_t *client=NULL;
    OK(ringloom_service_create_client(service,"kv",2,&client));
    ringloom_kv_store_config_t cfg;
    ringloom_kv_store_config_init(&cfg);
    cfg.map_size_hint=8*1024*1024;
    ringloom_kv_writer_t *writer=NULL;
    OK(ringloom_kv_open_writer(client,"smoke",&cfg,NULL,&writer));
    REQUIRE(ringloom_kv_writer_state(writer)==RINGLOOM_KV_GRANTED);
    ringloom_kv_writer_t *held=NULL;
    REQUIRE(ringloom_kv_open_writer(client,"smoke",&cfg,NULL,&held)==RINGLOOM_ERR_KV_HELD_BY_OTHER);
    REQUIRE(!held);
    ringloom_kv_reader_options_t ro;
    ringloom_kv_reader_options_init(&ro); ro.wait_ready_timeout_ms=5000;
    ringloom_kv_reader_t *reader=NULL;
    OK(ringloom_kv_open_reader(client,"smoke",&ro,&reader));
    ringloom_kv_session_t *session=NULL;
    OK(ringloom_kv_session_new(reader,&session));
    uint64_t seq=put(writer,"key","value",5,-1);
    REQUIRE(seq);
    wait_value(session,"key","value",5);
    for (int i=0;i<5000 && !ringloom_kv_is_acked(writer,seq);i++) poll_control();
    REQUIRE(ringloom_kv_is_acked(writer,seq));
    /* Distinct handles to one LMDB generation must share an environment;
     * closing one must not disrupt the remaining session's reader slot. */
    ringloom_kv_reader_t *second_reader=NULL;
    ringloom_kv_session_t *second_session=NULL;
    OK(ringloom_kv_open_reader(client,"smoke",&ro,&second_reader));
    OK(ringloom_kv_session_new(second_reader,&second_session));
    wait_value(second_session,"key","value",5);
    ringloom_kv_session_free(second_session);
    ringloom_kv_close_reader(second_reader);
    uint8_t tiny[1]; size_t len=0;
    REQUIRE(ringloom_kv_get(session,(const uint8_t*)"key",3,tiny,sizeof(tiny),&len)==RINGLOOM_ERR_KV_BUFFER_TOO_SMALL);
    REQUIRE(len==5);
    const uint8_t *borrow=NULL;
    OK(ringloom_kv_view(session,(const uint8_t*)"key",3,&borrow,&len));
    put(writer,"key","new",3,0);
    for(int i=0;i<50;i++) poll_control();
    REQUIRE(!memcmp(borrow,"value",5));
    ringloom_kv_release(session);
    wait_value(session,"key","new",3);
    put(writer,"empty","",0,0); wait_value(session,"empty","",0);
    put(writer,"ttl","gone",4,100); wait_value(session,"ttl","gone",4);
    usleep(150000);
    REQUIRE(ringloom_kv_get(session,(const uint8_t*)"ttl",3,tiny,sizeof(tiny),&len)==RINGLOOM_KV_MISSING);
    static uint8_t large[64000]; memset(large,'x',sizeof(large));
    put(writer,"large",large,sizeof(large),0); wait_value(session,"large",large,sizeof(large));
    OK(ringloom_kv_begin_batch(writer));
    seq=99;
    OK(ringloom_kv_put(writer,(const uint8_t*)"batch-a",7,(const uint8_t*)"a",1,0,0,&seq)); REQUIRE(seq==0);
    OK(ringloom_kv_put(writer,(const uint8_t*)"batch-b",7,(const uint8_t*)"b",1,0,0,&seq));
    OK(ringloom_kv_put(writer,(const uint8_t*)"large-a",7,large,sizeof(large),0,1,&seq));
    OK(ringloom_kv_put(writer,(const uint8_t*)"large-b",7,large,sizeof(large),0,1,&seq)); /* auto-flush */
    OK(ringloom_kv_commit_batch(writer,1,&seq)); REQUIRE(seq);
    wait_value(session,"batch-b","b",1);
    wait_value(session,"large-a",large,sizeof(large));
    wait_value(session,"large-b",large,sizeof(large));
    int visits=0;
    OK(ringloom_kv_for_each(session,visit,&visits)); REQUIRE(visits==7);
    OK(ringloom_kv_delete(writer,(const uint8_t*)"key",3,0,&seq));
    for (int i=0;i<5000;i++) { if(ringloom_kv_get(session,(const uint8_t*)"key",3,tiny,sizeof(tiny),&len)==RINGLOOM_KV_MISSING) break; poll_control(); }
    REQUIRE(ringloom_kv_get(session,(const uint8_t*)"key",3,tiny,sizeof(tiny),&len)==RINGLOOM_KV_MISSING);
    ringloom_kv_writer_options_t wo; ringloom_kv_writer_options_init(&wo); wo.acquire=1;
    ringloom_kv_writer_t *standby=NULL;
    OK(ringloom_kv_open_writer(client,"smoke",&cfg,&wo,&standby));
    REQUIRE(ringloom_kv_writer_state(standby)==RINGLOOM_KV_QUEUED);
    ringloom_kv_close_writer(writer);
    for(int i=0;i<5000 && ringloom_kv_writer_state(standby)!=RINGLOOM_KV_GRANTED;i++) poll_control();
    REQUIRE(ringloom_kv_writer_state(standby)==RINGLOOM_KV_GRANTED);
    OK(ringloom_kv_clear(standby,0,&seq));
    for(int i=0;i<5000;i++) { visits=0; OK(ringloom_kv_for_each(session,visit,&visits)); if(!visits) break; poll_control(); }
    REQUIRE(!visits);
    ringloom_kv_status_t status={.size=sizeof(status)};
    OK(ringloom_kv_reader_status(reader,&status)); REQUIRE(status.generation>0);
    ringloom_kv_close_writer(standby);
    ringloom_kv_session_free(session);
    ringloom_kv_close_reader(reader);
    ringloom_client_destroy(client);
    ringloom_service_destroy(service);
    puts("KV C ABI smoke passed: put/get/view/TTL/delete/clear/batch/large/ack/iteration/standby");
    return 0;
}
