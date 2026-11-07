#!/usr/bin/env bash
# Repeated, serial benchmark runs with metadata and a generated report.
# Usage: scripts/bench-all.sh --output-dir DIR [--repeat N] [--suites "ring ipc topic backpressure recovery aeron"]
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=""
REPEAT=3
SUITES="ring ipc topic backpressure recovery aeron"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir) OUT="$2"; shift 2 ;;
        --repeat) REPEAT="$2"; shift 2 ;;
        --suites) SUITES="$2"; shift 2 ;;
        -h|--help) sed -n '2,3p' "$0"; exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done
[[ -n "$OUT" && "$REPEAT" =~ ^[1-9][0-9]*$ && -n "$SUITES" ]] || { echo "Specify --output-dir DIR and a positive --repeat" >&2; exit 1; }
for suite in $SUITES; do
    case "$suite" in ring|ipc|topic|backpressure|recovery|aeron) ;; *) echo "Unknown suite: $suite" >&2; exit 1 ;; esac
done
# Low-level Zig benchmarks use fixed /tmp paths and ports. Serialize wrapper runs.
exec 9>/tmp/ringloom-benchmark.lock
flock -n 9 || { echo "Another benchmark wrapper is running" >&2; exit 1; }
mkdir "$OUT" # Refuse existing directories so old artifacts cannot look fresh.
OUT=$(cd "$OUT" && pwd)
python3 - "$OUT" "$SUITES" "$REPEAT" <<'PY'
import datetime, json, os, platform, subprocess, sys
from pathlib import Path

def command(*args):
    return subprocess.check_output(args, text=True).strip()

def read(path):
    try: return Path(path).read_text().strip()
    except OSError: return 'unavailable'

meta = dict(date=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            commit=command('git', 'rev-parse', 'HEAD'), dirty=bool(command('git', 'status', '--porcelain')),
            zig_version=command('zig', 'version'), build_mode='ReleaseFast',
            kernel=platform.release(), cpus=os.cpu_count(),
            cpu_model=next((line.split(':', 1)[1].strip() for line in read('/proc/cpuinfo').splitlines() if line.startswith('model name')), 'unknown'),
            smt_active=read('/sys/devices/system/cpu/smt/active'),
            isolated_cpus=read('/sys/devices/system/cpu/isolated'),
            governor=read('/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor'),
            loadavg=read('/proc/loadavg'), suites=sys.argv[2].split(), repeat=int(sys.argv[3]),
            overrides={k:v for k,v in os.environ.items() if k.startswith('RINGLOOM_BENCH_')})
Path(sys.argv[1], 'metadata.json').write_text(json.dumps(meta, indent=2)+'\n')
# Record tracked source changes; untracked sources need separate archival.
Path(sys.argv[1], 'working-tree.patch').write_text(command('git', 'diff', 'HEAD'))
PY
zig build install test-bins -Doptimize=ReleaseFast > "$OUT/build.log" 2>&1
copy_fresh() {
    python3 - "$1" "$2" "$3" <<'PY'
import shutil, sys
from pathlib import Path
src, dst, marker = map(Path, sys.argv[1:])
files = [p for p in src.rglob('*.json') if p.stat().st_mtime_ns > marker.stat().st_mtime_ns]
if not files: raise SystemExit(f'No fresh JSON artifacts under {src}')
for p in files:
    target = dst / p.relative_to(src)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(p, target)
PY
}
for run in $(seq 1 "$REPEAT"); do
    dir="$OUT/run-$run"
    mkdir "$dir"
    for suite in $SUITES; do
        echo "Run $run/$REPEAT: $suite"
        touch "$OUT/.started"
        case "$suite" in
            ring) zig build perf-ring-buffer -Doptimize=ReleaseFast > "$dir/ring.log" 2>&1
                  copy_fresh /tmp/ringloom-perf-results/ring-buffer "$dir/ring-buffer" "$OUT/.started" ;;
            ipc) bash scripts/run-benchmarks.sh --output-dir "$dir/ipc" > "$dir/ipc.log" 2>&1 ;;
            topic) bash scripts/bench-topic.sh --runs 1 --sizes "32 128 1024" --output-dir "$dir/topic" > "$dir/topic.log" 2>&1
                   bash scripts/bench-topic.sh --runs 1 --sizes "32 128 1024" --ack --output-dir "$dir/topic" >> "$dir/topic.log" 2>&1 ;;
            backpressure) zig build perf -Dperf-filter=backpressure -Doptimize=ReleaseFast > "$dir/backpressure.log" 2>&1
                          copy_fresh /tmp/ringloom-perf-results/backpressure "$dir/backpressure" "$OUT/.started" ;;
            recovery) zig build perf -Dperf-filter=recovery -Doptimize=ReleaseFast > "$dir/recovery.log" 2>&1
                      copy_fresh /tmp/ringloom-perf-results/recovery "$dir/recovery" "$OUT/.started" ;;
            aeron) zig build perf-aeron -Doptimize=ReleaseFast > "$dir/aeron.log" 2>&1
                   copy_fresh /tmp/ringloom-plain-aeron-bench-results "$dir/aeron" "$OUT/.started" ;;
        esac
    done
done
python3 scripts/bench-report.py "$OUT" > "$OUT/report.md"
echo "Results and logs: $OUT"
echo "Report: $OUT/report.md"
