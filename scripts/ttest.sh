#!/usr/bin/env bash
# Fast standalone test harness for topics modules that import ringloom_queue and/or ringloom_common.
# Usage: scripts/ttest.sh <path-to-test-file.zig>
set -euo pipefail
ROOT="$1"
RQ=/home/dragan/code/ringloom-queue/src/root.zig
COMMON=/home/dragan/code/ringloom/src/common/root.zig
exec zig test \
  --dep ringloom_queue --dep ringloom_common \
  -Mroot="$ROOT" \
  -Mringloom_queue="$RQ" \
  -Mringloom_common="$COMMON"
