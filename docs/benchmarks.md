# Running and reviewing benchmarks

Start here for performance work. `docs/testing.md` covers correctness tests and
machine tuning; `docs/benchmark-results.md` holds published snapshots.

## Repeatable runs

From the repository root, with Zig 0.17.1, Python 3, Bash, GNU timeout and flock:

```sh
# Focused check after ring-buffer or topic-path changes (~minutes).
scripts/bench-all.sh --output-dir /tmp/ringloom-check-001 --repeat 3 --suites 'ring topic'
# Full retained baseline (longer; recovery alone waits for several 10s timeouts).
scripts/bench-all.sh --output-dir /tmp/ringloom-baseline-001 --repeat 3
# Compare like-for-like runs, flagging >10% changes for investigation.
python3 scripts/bench-report.py /tmp/ringloom-check-002 --baseline /tmp/ringloom-check-001
```

The wrapper builds broker and services in ReleaseFast, runs suites serially,
refuses existing output directories, and saves `metadata.json`, a tracked-file
working-tree diff, build/run logs, raw JSON under `run-N/`, and `report.md`.
Untracked sources are not in the diff: commit or archive them alongside the
results when preserving a baseline. Use a new directory for every invocation.
Failures stop the wrapper; inspect its logs before rerunning. Do not treat an
incomplete directory as a baseline. The wrapper lock serializes wrapper runs;
do not run standalone benchmarks concurrently with it. Zig harness artifacts
use fixed `/tmp` paths; only files freshly written during that suite are copied.
Benchmark build steps always execute, including on a warm build cache.

Suite names are `ring`, `ipc` (local and two-broker service messaging), `topic`
(single-broker publishing in both ack modes), `kv` (KV serial write/read on one
broker plus native reader scale at 1k/1M/10M entries), `backpressure`, `recovery`, and `aeron` (raw UDP
reference). For direct harness runs:

```sh
zig build perf -Doptimize=ReleaseFast
zig build perf -Doptimize=ReleaseFast -Dperf-filter=backpressure
zig build perf -Doptimize=ReleaseFast -Dperf-filter=topic
```

Harness-only commands retain JSON under `/tmp/ringloom-perf-results/`; archive
fresh files immediately. `-Dperf-filter` selects test names by substring.
Always pass `-Doptimize=ReleaseFast`: the perf test root is optimized separately,
but spawned broker/service binaries otherwise follow the default build mode.

## What the benchmarks tell us

| Benchmark | Value | Interpretation and limits |
|---|---|---|
| Ring microbenchmarks | Isolates copy/zeroing, claim and handoff costs | 64-byte messages, mostly SPSC; not evidence about contended MPSC or broker throughput |
| Paced service transit | Typical one-way delivery latency at a fixed offered load | 10us send interval; timestamps share a host clock; excludes network hardware |
| Saturated service runs | Capacity and queueing under load | Sender throughput measures enqueue rate; remote receiver latency includes backlog; inspect counts and failures |
| Plain Aeron UDP | Reference for transport overhead | Different threading/layout from RingLoom; cannot attribute the whole difference to broker overhead |
| Topic publish | Registered producer → leader append, including final ack | Single broker; replicate_once has no remote replica to wait for. Fire-and-forget uses a final acked sentinel; offer rate is reported separately |
| Cross-broker topics | Leader → replica → tailer latency | Separate exploratory script; same host, raw publishing, best-of-N selection, incomplete-delivery handling; not part of the retained baseline wrapper |
| Backpressure | Functional behavior under pressure | Send failures and counts matter more than speed; escalating batches share accumulated backlog and ignore publisher exit status |
| Service recovery | Death observed by harness → heartbeat timeout → replacement ready | Polling resolution ~1ms; replacement readiness does not establish restored application traffic |
| Broker recovery | Restart workflow through successful send probes | Includes deliberate 2s downtime and 3s cluster settling, process startup and probe time; not minimal failover latency or receiver-confirmed delivery |
| Topic startup/restart | Smoke check and coarse startup bound | Not a durable-recovery benchmark: no populated queue or replay validation |

The topic queue dependency currently has a fixed 83,754,496-byte cycle file
and rejects appends once it fills until the time cycle rolls. The topic script
caps default runs at a conservative 64 MiB (including warmup and record overhead),
so 1024-byte runs use fewer messages. An explicit `--count` can exceed that limit
and will fail the final ack check; it is not a sustained-throughput test.

The service heartbeat checker currently uses a hard-coded 10s deadline;
`broker.heartbeat.timeout.ms` does not control this measurement. Recovery reports
now separate detection from replacement readiness. Old ~12,050ms results include
a fixed sleep and must not be compared as measured detection times.

## Updating results

1. Run correctness tests first (`zig build test`, and `zig build e2e` when service
   orchestration or wire formats changed).
2. Capture baseline and candidate with the same suites, sizes, message counts,
   build mode, affinity and machine state. Alternate baseline/candidate runs
   when investigating a regression. Never benchmark two revisions at once.
3. Use at least three runs. The report takes the median of each field, including
   per-run percentiles; this is not a pooled latency distribution. It lists run
   counts and metric spread. Spread is descriptive, not statistical significance.
4. Check sent/received/measured counts and failed sends in raw JSON. A fast run
   with missing messages is not an improvement. Inspect logs on suspicious runs.
5. Compare reports. Repeat changes near the observed spread; retain regressions
   and explain them instead of selecting only a favorable run.
6. Add a dated snapshot to `docs/benchmark-results.md`, with commit/dirty state,
   exact command, artifact location, machine state, measurement definitions and
   limitations. Paste generated tables from `report.md`; do not invent numbers.
   Keep historical results clearly labeled. A focused run only updates its suites.

Do not tune the host or change desktop/system configuration as part of a normal
rerun. Record its existing state. The older best-of-N scripts are useful for
finding an attainable floor, but do not mix their results with median baselines.

## Next improvements

Prioritize sustained runs with configurable duration and MPSC producer counts;
verify receiver counts and sequence continuity; add throughput-versus-offered-load
curves to expose saturation and coordinated omission. Reset or drain the queue
between independent backpressure load points, and distinguish expected send
rejections from publisher/process failures. Extend registered-topic
runs to two brokers with ack latency distributions, restart/replay and rollover
scenarios. Replace sleep-based broker restart choreography with repeated
receiver-confirmed probes. Preserve every cross-topic run and fail on incomplete
delivery before using that script for published baselines. Add a real two-host
network suite with clock synchronization or RTT measurement before making
network-latency claims. Keep these distinct from short correctness smoke tests.
