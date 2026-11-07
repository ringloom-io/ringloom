# RingLoom agent instructions

## Validation and benchmarks

Use Zig 0.16.0. Run `zig build test` for correctness; run `zig build e2e` for
changes to service/broker behavior. Performance tests are separate from those.

For performance work, read [docs/benchmarks.md](docs/benchmarks.md) first. The
repeatable entry point is:

```sh
scripts/bench-all.sh --output-dir /tmp/ringloom-check-001 --repeat 3 --suites 'ring topic'
```

Choose suites relevant to the change; omit `--suites` for the full retained
baseline. Use a fresh directory, run serially, and retain raw JSON and logs.
The wrapper builds ReleaseFast and records metadata. Direct `zig build perf`
commands also need `-Doptimize=ReleaseFast` for spawned broker/service binaries.
Use `-Dperf-filter=TEXT` for a focused harness run. Never infer fresh results
from old files under `/tmp`; benchmark steps must execute on every invocation.

Compare with `python3 scripts/bench-report.py NEW --baseline OLD`. Check delivery
counts, failures, repeat spread and matching configuration before calling a
change a regression or improvement. Update `docs/benchmark-results.md` only with
measured numbers, exact commands, artifact location, commit/dirty state and host
conditions. Preserve historical snapshots and label partial suite updates.
Do not change host tuning as part of an ordinary rerun.
When changing the reporting tool, run `python3 scripts/bench-report-test.py`.
