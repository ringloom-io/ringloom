#!/usr/bin/env python3
"""bench-report.py - render RingLoom benchmark JSON as Markdown, or compare runs.

Reads a results directory produced by scripts/bench-all.sh (or any directory
containing the JSON files the individual benchmark scripts write) and prints
the tables used in docs/benchmark-results.md.

Usage:
  scripts/bench-report.py RESULTS_DIR                    # Markdown tables
  scripts/bench-report.py RESULTS_DIR --baseline OLD_DIR # regression diff
  scripts/bench-report.py RESULTS_DIR --baseline OLD_DIR --threshold 15

The diff compares every metric present in both runs and flags changes worse
than --threshold percent (default 10). Exit status is 0 either way; read the
flags. Single runs on a shared machine are noisy: confirm a flagged change by
repeating both runs before acting on it.

Standard library only; no third-party packages.
"""

import argparse
import json
import re
import statistics
import sys
from pathlib import Path

# ── Loading ──────────────────────────────────────────────────────────────


RUN_DIR = re.compile(r"(^|/)run-\d+/")


def load_results(root: Path) -> dict:
    """Map relative path -> parsed JSON for every *.json under root.

    Repeated runs live in run-<N>/ subdirectories (scripts/bench-all.sh
    --repeat). Their files are merged under the path without the run-<N>/
    part: numeric fields become the median across runs, `runs` records how
    many were merged, and `<field>_spread_pct` records (max-min)/median for
    each numeric field so noisy metrics are visible.
    """
    grouped = {}
    for path in sorted(root.rglob("*.json")):
        if path.name == "metadata.json":
            continue
        try:
            data = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as err:
            print(f"warning: skipping {path}: {err}", file=sys.stderr)
            continue
        if not isinstance(data, dict):
            continue
        key = RUN_DIR.sub(r"\1", str(path.relative_to(root)))
        grouped.setdefault(key, []).append(data)
    return {key: merge_runs(runs) for key, runs in grouped.items()}


def merge_runs(runs: list) -> dict:
    if len(runs) == 1:
        return runs[0]
    merged = dict(runs[0])
    for field, value in runs[0].items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        values = sorted(r[field] for r in runs
                        if isinstance(r.get(field), (int, float)) and not isinstance(r.get(field), bool))
        median = statistics.median(values)
        merged[field] = median
        if median:
            merged[f"{field}_spread_pct"] = round((values[-1] - values[0]) / median * 100, 1)
    merged["runs"] = len(runs)
    return merged


def load_metadata(root: Path) -> dict:
    path = root / "metadata.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def size_of(name: str) -> int:
    match = re.search(r"(\d+)B\.json$", name)
    return int(match.group(1)) if match else -1


def pick(results: dict, pattern: str) -> list:
    """(size, relpath, data) for files whose relpath matches pattern, by size."""
    rx = re.compile(pattern)
    rows = [(size_of(k), k, v) for k, v in results.items() if rx.search(k)]
    return sorted(rows, key=lambda r: (r[0], r[1]))


# ── Formatting ───────────────────────────────────────────────────────────


def num(value) -> str:
    if value is None or value == "":
        return "n/a"
    if isinstance(value, float):
        return f"{value:,.1f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def size_label(size: int) -> str:
    return f"{size:,} B" if size >= 0 else "?"


def table(headers: list, rows: list) -> str:
    out = ["| " + " | ".join(headers) + " |"]
    out.append("|" + "|".join("-" * (len(h) + 2) for h in headers) + "|")
    for row in rows:
        out.append("| " + " | ".join(row) + " |")
    return "\n".join(out)


LATENCY_COLS = ["p50 (ns)", "p95 (ns)", "p99 (ns)", "p99.9 (ns)", "max (ns)"]
LATENCY_KEYS = ["latency_p50_ns", "latency_p95_ns", "latency_p99_ns", "latency_p99_9_ns", "latency_max_ns"]


def latency_cells(d: dict) -> list:
    return [num(d.get(k)) for k in LATENCY_KEYS]


# ── Sections ─────────────────────────────────────────────────────────────


def section_local_transit(results: dict) -> str:
    rows = []
    for size, name, echo in pick(results, r"local-transit-latency-echo-\d+B\.json$"):
        ping = results.get(name.replace("-echo-", "-ping-"), {})
        rows.append([
            size_label(size),
            num(ping.get("warmup_count")),
            num(echo.get("total_measured")),
            num(ping.get("sent")),
            num(ping.get("failed")),
        ] + latency_cells(echo))
    if not rows:
        return ""
    return "## Local IPC Transit Latency (single broker, same host)\n\n" + table(
        ["Message size", "Warmup", "Measured", "Sent", "Failed"] + LATENCY_COLS, rows)


def section_local_throughput(results: dict) -> str:
    rows = []
    for size, _, ping in pick(results, r"local-saturated-benchmark-ping-\d+B\.json$"):
        mb = ping.get("throughput_bytes_per_sec")
        rows.append([
            size_label(size),
            num(ping.get("warmup_count")),
            num(ping.get("sent")),
            num(ping.get("throughput_msgs_per_sec")),
            f"{mb / 1_000_000:,.1f}" if isinstance(mb, (int, float)) else "n/a",
        ])
    if not rows:
        return ""
    return "## Local IPC Throughput (single broker, same host)\n\n" + table(
        ["Message size", "Warmup", "Measured", "msgs/sec", "MB/sec"], rows)


def remote_section(results: dict, pattern: str, title: str) -> str:
    rows = []
    for size, _, echo in pick(results, pattern):
        rows.append([
            size_label(size),
            num(echo.get("total_received")),
            num(echo.get("total_measured")),
        ] + latency_cells(echo))
    if not rows:
        return ""
    return f"## {title}\n\n" + table(["Message size", "Received", "Measured"] + LATENCY_COLS, rows)


def section_plain_aeron(results: dict) -> str:
    rows = []
    for size, _, d in pick(results, r"plain-aeron-remote-transit-\d+B\.json$"):
        rows.append([
            size_label(size),
            num(d.get("sent", d.get("messages_sent"))),
            num(d.get("total_received", d.get("messages_received"))),
        ] + latency_cells(d))
    if not rows:
        return ""
    return "## Plain Aeron Remote Transit Reference (two media drivers on loopback)\n\n" + table(
        ["Message size", "Sent", "Received"] + LATENCY_COLS, rows)


def section_ring_buffer(results: dict) -> str:
    rows = []
    for _, name, d in pick(results, r"ring-buffer/.*\.json$"):
        rows.append([
            d.get("scenario", Path(name).stem),
            num(d.get("throughput_msgs_per_sec")),
            num(d.get("latency_p50_ns")),
            num(d.get("latency_p99_ns")),
            num(d.get("latency_max_ns")),
        ])
    if not rows:
        return ""
    return "## Ring Buffer Microbenchmarks (64 B, no broker)\n\n" + table(
        ["Scenario", "msgs/sec", "p50 (ns)", "p99 (ns)", "max (ns)"], rows)


def section_topic_publish(results: dict) -> str:
    rows = []
    for size, _, d in pick(results, r"topic-publish-.*\d+B\.json$"):
        ack_wait = d.get("ack_wait_ns")
        rows.append([
            size_label(size),
            "replicate_once" if d.get("ack_mode") == 1 else "fire_and_forget",
            num(d.get("published")),
            num(d.get("throughput_msgs_per_sec")),
            num(d.get("offer_msgs_per_sec")),
            f"{ack_wait / 1000:,.0f}" if isinstance(ack_wait, (int, float)) else "n/a",
        ])
    if not rows:
        return ""
    return "## Topic Publish Throughput (single broker, Aeron IPC)\n\n" + table(
        ["Message size", "Ack mode", "Published", "Appended msgs/sec", "Offer msgs/sec", "Final ack wait (us)"], rows)


def section_topic_cross(results: dict) -> str:
    rows = []
    for size, _, d in pick(results, r"topic-cross-.*\d+B\.json$"):
        rows.append([
            size_label(size),
            num(d.get("received")),
            num(d.get("lat_p50_ns")),
            num(d.get("lat_p99_ns")),
            num(d.get("lat_p99_9_ns")),
            num(d.get("lat_max_ns")),
        ])
    if not rows:
        return ""
    return "## Cross-Broker Topic Replication Latency (publish on A, read replica on B)\n\n" + table(
        ["Message size", "Received", "p50 (ns)", "p99 (ns)", "p99.9 (ns)", "max (ns)"], rows)


def section_recovery(results: dict) -> str:
    rows = []
    for _, name, d in pick(results, r"recovery/.*\.json$"):
        if "recovery_time_ms" not in d:
            continue  # Auxiliary ping/probe results are not recovery summaries.
        rows.append([
            d.get("scenario", Path(name).stem),
            num(d.get("detection_time_ms")),
            num(d.get("recovery_time_ms")),
            num(d.get("iterations")),
            num(d.get("max_recovery_time_ms")),
        ])
    if not rows:
        return ""
    return "## Recovery Time\n\n" + table(
        ["Scenario", "Failure detected (ms)", "Recovery complete (ms)", "Iterations", "Max (ms)"], rows)


def section_backpressure(results: dict) -> str:
    rows = []
    for _, name, d in pick(results, r"backpressure/.*\.json$"):
        rows.append([
            Path(name).stem,
            num(d.get("message_size")),
            num(d.get("sent")),
            num(d.get("failed")),
            num(d.get("throughput_msgs_per_sec")),
        ])
    if not rows:
        return ""
    return "## Backpressure\n\n" + table(["Scenario", "Size (B)", "Sent", "Send failures", "msgs/sec"], rows)


def section_kv(results: dict) -> str:
    parts = []
    rows = []
    for _, name, d in pick(results, r"kv/.*\.json$"):
        if d.get("scenario") in ("native-reader-scale", "kv-engine-alloc"):
            continue
        rows.append([str(d.get("scenario", name)), num(d.get("entries")),
                     num(d.get("published")), num(d.get("visible")), num(d.get("failed")),
                     num(d.get("get_p50_ns")), num(d.get("get_p99_ns")),
                     num(d.get("miss_p99_ns")), num(d.get("writer_p99_ns")), num(d.get("apply_ns")),
                     num(d.get("iteration_ns")), num(d.get("read_zig_allocs")),
                     num(d.get("read_engine_allocs"))])
    if rows:
        parts.append("## KV bounded same-host serial workload\n\n" + table(
            ["Scenario", "Entries", "Published", "Visible", "Failed", "Get p50 ns", "Get p99 ns", "Miss p99 ns", "Writer p99 ns",
             "Serial write+ack total ns", "Iteration ns", "Read Zig allocs", "Read LMDB allocs"], rows))
    rows = []
    for _, name, d in pick(results, r"kv/.*\.json$"):
        if d.get("scenario") != "native-reader-scale":
            continue
        rows.append([num(d.get("entries")), num(d.get("failed")), num(d.get("get_p50_ns")), num(d.get("get_p99_ns")),
                     num(d.get("miss_p50_ns")), num(d.get("miss_p99_ns")), num(d.get("view_p50_ns")),
                     num(d.get("view_p99_ns")), num(d.get("iteration_ns")), num(d.get("iterated")),
                     num(d.get("reader_threads")), num(d.get("parallel_operations")), num(d.get("parallel_ns")),
                     num(d.get("setup_ns")), num(d.get("read_zig_allocs")), num(d.get("read_engine_allocs"))])
    if rows:
        parts.append("## KV native reader scale\n\n" + table(
            ["Entries", "Failed", "Get p50 ns", "Get p99 ns", "Miss p50 ns", "Miss p99 ns", "View p50 ns", "View p99 ns",
             "Iteration ns", "Iterated", "Reader threads", "Parallel gets", "Parallel ns", "Setup ns",
             "Read Zig allocs", "Read LMDB allocs"], rows))
    rows = []
    for _, name, d in pick(results, r"kv/.*\.json$"):
        if d.get("scenario") == "kv-engine-alloc":
            rows.append([num(d.get("transactions")), num(d.get("engine_allocs")), num(d.get("engine_allocs_per_txn"))])
    if rows:
        parts.append("## KV engine write-transaction allocations\n\n" + table(
            ["Transactions", "LMDB allocs", "LMDB allocs per txn"], rows))
    return "\n\n".join(parts)


SECTIONS = [
    section_kv,
    section_ring_buffer,
    section_local_transit,
    section_local_throughput,
    lambda r: remote_section(r, r"remote-transit-latency-echo-\d+B\.json$",
                             "Cross-Broker Transit Latency (two brokers on loopback)"),
    section_plain_aeron,
    lambda r: remote_section(r, r"remote-saturated-benchmark-echo-\d+B\.json$",
                             "Cross-Broker Saturated Queueing Latency (two brokers on loopback)"),
    section_topic_publish,
    section_topic_cross,
    section_backpressure,
    section_recovery,
]


def render_metadata(meta: dict) -> str:
    if not meta:
        return "_No metadata.json found: record date, commit, build mode and machine state by hand._"
    fields = [
        ("Date", meta.get("date")),
        ("Commit", meta.get("commit")),
        ("Working tree dirty", meta.get("dirty")),
        ("Repeats", meta.get("repeat")),
        ("Environment overrides", meta.get("overrides")),
        ("Zig version", meta.get("zig_version")),
        ("Build mode", meta.get("build_mode")),
        ("Kernel", meta.get("kernel")),
        ("CPU", meta.get("cpu_model")),
        ("Online CPUs", meta.get("cpus")),
        ("SMT active", meta.get("smt_active")),
        ("Isolated CPUs", meta.get("isolated_cpus") or "none"),
        ("CPU governor", meta.get("governor")),
        ("Load average (start)", meta.get("loadavg")),
        ("Suites", ", ".join(meta.get("suites", []))),
    ]
    return "\n".join(f"- **{k}:** {v}" for k, v in fields if v not in (None, ""))


def render_report(root: Path) -> str:
    results = load_results(root)
    parts = ["## Run metadata\n\n" + render_metadata(load_metadata(root))]
    repeats = max((d.get("runs", 1) for d in results.values()), default=1)
    if repeats > 1:
        parts.append(f"Median of up to {repeats} runs per row. Percentiles are medians of per-run percentiles, not pooled samples.")
        rows = [[name, num(d.get("runs", 1)), num(max((v for k, v in d.items() if k.endswith("_spread_pct") and k.removesuffix("_spread_pct") in METRICS), default=0)) + "%"] for name, d in sorted(results.items())]
        parts.append("## Repeat variability\n\n" + table(["File", "Runs", "Largest metric spread"], rows))
    for section in SECTIONS:
        text = section(results)
        if text:
            parts.append(text)
    if len(parts) == 1:
        parts.append(f"_No benchmark JSON found under {root}._")
    return "\n\n".join(parts) + "\n"


# ── Comparison ───────────────────────────────────────────────────────────

# metric -> True if higher is better
METRICS = {
    "get_p50_ns": False,
    "get_p99_ns": False,
    "miss_p50_ns": False,
    "miss_p99_ns": False,
    "view_p50_ns": False,
    "view_p99_ns": False,
    "parallel_ns": False,
    "setup_ns": False,
    "engine_allocs_per_txn": False,
    "writer_p99_ns": False,
    "apply_ns": False,
    "iteration_ns": False,
    "latency_p50_ns": False,
    "latency_p99_ns": False,
    "latency_p99_9_ns": False,
    "throughput_msgs_per_sec": True,
    "tput": True,
    "lat_p50_ns": False,
    "lat_p99_ns": False,
    "ack_wait_ns": False,
    "detection_time_ms": False,
    "recovery_time_ms": False,
}


def render_comparison(new_root: Path, base_root: Path, threshold: float) -> str:
    new, base = load_results(new_root), load_results(base_root)
    rows, flagged = [], 0
    for name in sorted(set(new) & set(base)):
        for metric, higher_is_better in METRICS.items():
            a, b = base[name].get(metric), new[name].get(metric)
            if not isinstance(a, (int, float)) or not isinstance(b, (int, float)) or a == 0:
                continue
            change = (b - a) / a * 100
            worse = -change if higher_is_better else change
            flag = "⚠ worse" if worse > threshold else ("better" if worse < -threshold else "")
            flagged += flag.startswith("⚠")
            spread = max(base[name].get(f"{metric}_spread_pct", 0), new[name].get(f"{metric}_spread_pct", 0))
            rows.append([name, metric, num(a), num(b), f"{change:+.1f}%",
                         f"{spread:g}%" if spread else "", flag])
    only_new = sorted(set(new) - set(base))
    only_base = sorted(set(base) - set(new))

    out = [f"## Comparison: {new_root} vs baseline {base_root}", ""]
    out.append(f"Threshold: {threshold:g}%. {flagged} metric(s) worse than threshold.")
    out.append("")
    out.append("Spread = largest (max-min)/median across repeated runs of either side; a change")
    out.append("smaller than the spread warrants more repeats; spread is not a confidence interval.")
    out.append("")
    out.append(table(["File", "Metric", "Baseline", "New", "Change", "Spread", ""], rows) if rows
               else "_No overlapping results._")
    if only_new:
        out += ["", "Only in new run: " + ", ".join(only_new)]
    if only_base:
        out += ["", "Only in baseline: " + ", ".join(only_base)]
    return "\n".join(out) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("results_dir", type=Path)
    parser.add_argument("--baseline", type=Path, help="compare against this results directory")
    parser.add_argument("--threshold", type=float, default=10.0, help="percent change to flag (default 10)")
    args = parser.parse_args()

    if not args.results_dir.is_dir():
        parser.error(f"{args.results_dir} is not a directory")
    if args.baseline:
        if not args.baseline.is_dir():
            parser.error(f"{args.baseline} is not a directory")
        sys.stdout.write(render_comparison(args.results_dir, args.baseline, args.threshold))
    else:
        sys.stdout.write(render_report(args.results_dir))
    return 0


if __name__ == "__main__":
    sys.exit(main())
