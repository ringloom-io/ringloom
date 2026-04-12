#!/usr/bin/env bash
#
# tune-system.sh — Apply runtime Linux kernel tuning for low-latency benchmarks.
#
# Configures the system for deterministic, low-latency operation of the BRZ
# broker.  Pairs with BIOS/firmware and kernel boot parameter changes described
# in docs/testing.md.
#
# Must be run as root (or with sudo).
#
# Usage:
#   sudo ./scripts/tune-system.sh              # Apply tuning
#   sudo ./scripts/tune-system.sh --revert     # Revert to saved state
#   sudo ./scripts/tune-system.sh --verify     # Print current settings
#   sudo ./scripts/tune-system.sh --help
#
# What this script does (apply mode):
#   1. Stops irqbalance service
#   2. Sets CPU frequency governor to "performance" on all cores
#   3. Disables AMD turbo boost
#   4. Disables transparent huge pages (THP)
#   5. Applies sysctl tuning (swappiness, timer migration)
#   6. Moves IRQ affinity off isolated cores (best-effort)
#   7. Saves original state for --revert
#
# What this script does NOT do (requires manual steps):
#   - BIOS/firmware changes (SMT, C-states, power profile)
#   - Kernel boot parameters (isolcpus, nohz_full, rcu_nocbs)
#   - CPU pinning (done via broker.properties config)
#
# See docs/testing.md § "Low-Latency System Tuning" for the full guide.

set -euo pipefail

STATE_FILE="/tmp/brz-tune-state.env"

# Cores reserved for OS/interrupts (used for IRQ affinity migration).
# Adjust if your isolation scheme differs.
HOUSEKEEPING_CORES="0-1"

# ── Helpers ───────────────────────────────────────────────────────────

die()  { echo "ERROR: $*" >&2; exit 1; }
info() { echo "  [+] $*"; }
warn() { echo "  [!] $*" >&2; }

require_root() {
    [[ $EUID -eq 0 ]] || die "This script must be run as root (try: sudo $0)"
}

# Read a sysfs/procfs file, return empty string if missing.
read_sysfs() {
    cat "$1" 2>/dev/null || echo ""
}

# ── Save / Restore State ─────────────────────────────────────────────

save_state() {
    info "Saving current state to $STATE_FILE"
    {
        # Governor (sample from cpu0).
        echo "ORIG_GOVERNOR=$(read_sysfs /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)"

        # Turbo boost.
        echo "ORIG_BOOST=$(read_sysfs /sys/devices/system/cpu/cpufreq/boost)"

        # Transparent huge pages.
        local thp_enabled thp_defrag
        thp_enabled=$(read_sysfs /sys/kernel/mm/transparent_hugepage/enabled)
        # Extract the active setting (the one in brackets).
        thp_enabled=$(echo "$thp_enabled" | grep -oP '\[\K[^\]]+' || echo "")
        thp_defrag=$(read_sysfs /sys/kernel/mm/transparent_hugepage/defrag)
        thp_defrag=$(echo "$thp_defrag" | grep -oP '\[\K[^\]]+' || echo "")
        echo "ORIG_THP_ENABLED=$thp_enabled"
        echo "ORIG_THP_DEFRAG=$thp_defrag"

        # irqbalance status.
        if systemctl is-active --quiet irqbalance 2>/dev/null; then
            echo "ORIG_IRQBALANCE=active"
        else
            echo "ORIG_IRQBALANCE=inactive"
        fi

        # Sysctls.
        echo "ORIG_SWAPPINESS=$(sysctl -n vm.swappiness 2>/dev/null || echo "")"
        echo "ORIG_TIMER_MIGRATION=$(sysctl -n kernel.timer_migration 2>/dev/null || echo "")"
    } > "$STATE_FILE"
}

revert_state() {
    [[ -f "$STATE_FILE" ]] || die "No saved state at $STATE_FILE — nothing to revert."

    # shellcheck source=/dev/null
    source "$STATE_FILE"

    echo "Reverting to saved state from $STATE_FILE ..."

    # Governor.
    if [[ -n "${ORIG_GOVERNOR:-}" ]]; then
        info "Restoring governor to '$ORIG_GOVERNOR'"
        for gov_file in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
            echo "$ORIG_GOVERNOR" > "$gov_file" 2>/dev/null || true
        done
    fi

    # Turbo boost.
    if [[ -n "${ORIG_BOOST:-}" ]]; then
        info "Restoring turbo boost to '$ORIG_BOOST'"
        echo "$ORIG_BOOST" > /sys/devices/system/cpu/cpufreq/boost 2>/dev/null || true
    fi

    # THP.
    if [[ -n "${ORIG_THP_ENABLED:-}" ]]; then
        info "Restoring THP enabled to '$ORIG_THP_ENABLED'"
        echo "$ORIG_THP_ENABLED" > /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || true
    fi
    if [[ -n "${ORIG_THP_DEFRAG:-}" ]]; then
        info "Restoring THP defrag to '$ORIG_THP_DEFRAG'"
        echo "$ORIG_THP_DEFRAG" > /sys/kernel/mm/transparent_hugepage/defrag 2>/dev/null || true
    fi

    # irqbalance.
    if [[ "${ORIG_IRQBALANCE:-}" == "active" ]]; then
        info "Restarting irqbalance"
        systemctl start irqbalance 2>/dev/null || true
    fi

    # Sysctls.
    if [[ -n "${ORIG_SWAPPINESS:-}" ]]; then
        info "Restoring vm.swappiness=$ORIG_SWAPPINESS"
        sysctl -q vm.swappiness="$ORIG_SWAPPINESS" 2>/dev/null || true
    fi
    if [[ -n "${ORIG_TIMER_MIGRATION:-}" ]]; then
        info "Restoring kernel.timer_migration=$ORIG_TIMER_MIGRATION"
        sysctl -q kernel.timer_migration="$ORIG_TIMER_MIGRATION" 2>/dev/null || true
    fi

    rm -f "$STATE_FILE"
    echo ""
    echo "Done. Original settings restored."
}

# ── Apply Tuning ─────────────────────────────────────────────────────

apply_tuning() {
    echo "BRZ Broker — Low-Latency System Tuning"
    echo "======================================="
    echo ""

    save_state

    # 1. Stop irqbalance.
    if systemctl is-active --quiet irqbalance 2>/dev/null; then
        info "Stopping irqbalance"
        systemctl stop irqbalance
    else
        info "irqbalance already stopped"
    fi

    # 2. Set CPU governor to "performance".
    local governor_set=0
    for gov_file in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
        [[ -f "$gov_file" ]] || continue
        echo "performance" > "$gov_file" 2>/dev/null && governor_set=$((governor_set + 1))
    done
    info "Set 'performance' governor on $governor_set CPUs"

    # 3. Disable turbo boost (AMD: /sys/devices/system/cpu/cpufreq/boost).
    local boost_file="/sys/devices/system/cpu/cpufreq/boost"
    if [[ -f "$boost_file" ]]; then
        echo 0 > "$boost_file"
        info "Disabled turbo boost ($boost_file = 0)"
    else
        warn "Turbo boost file not found at $boost_file — skipping"
    fi

    # 4. Disable transparent huge pages.
    local thp_dir="/sys/kernel/mm/transparent_hugepage"
    if [[ -d "$thp_dir" ]]; then
        echo never > "$thp_dir/enabled" 2>/dev/null && info "THP enabled = never"
        echo never > "$thp_dir/defrag"  2>/dev/null && info "THP defrag  = never"
    else
        warn "THP directory not found — skipping"
    fi

    # 5. Sysctl tuning.
    sysctl -q vm.swappiness=0           2>/dev/null && info "vm.swappiness = 0"
    sysctl -q kernel.timer_migration=0  2>/dev/null && info "kernel.timer_migration = 0"

    # 6. Move IRQ affinity off isolated cores (best-effort).
    local irq_moved=0
    local irq_skipped=0
    for affinity_file in /proc/irq/*/smp_affinity_list; do
        [[ -f "$affinity_file" ]] || continue
        # Skip default (IRQ 0) and non-writable entries.
        if echo "$HOUSEKEEPING_CORES" > "$affinity_file" 2>/dev/null; then
            irq_moved=$((irq_moved + 1))
        else
            irq_skipped=$((irq_skipped + 1))
        fi
    done
    info "Moved $irq_moved IRQs to cores $HOUSEKEEPING_CORES ($irq_skipped non-writable, skipped)"

    # ── Verification summary ─────────────────────────────────────────
    echo ""
    echo "Verification"
    echo "------------"
    verify_settings
    echo ""
    echo "State saved to $STATE_FILE (use --revert to undo)."
    echo ""
    echo "Next steps:"
    echo "  • Set CPU affinity in broker.properties (see docs/testing.md)"
    echo "  • Use taskset to pin test services to isolated cores"
    echo "  • Run benchmarks: ./scripts/run-benchmarks.sh"
}

# ── Verify ───────────────────────────────────────────────────────────

verify_settings() {
    local governor boost thp_enabled thp_defrag irqbal swappiness timer_mig

    governor=$(read_sysfs /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
    boost=$(read_sysfs /sys/devices/system/cpu/cpufreq/boost)
    thp_enabled=$(read_sysfs /sys/kernel/mm/transparent_hugepage/enabled)
    thp_defrag=$(read_sysfs /sys/kernel/mm/transparent_hugepage/defrag)
    swappiness=$(sysctl -n vm.swappiness 2>/dev/null || echo "?")
    timer_mig=$(sysctl -n kernel.timer_migration 2>/dev/null || echo "?")

    if systemctl is-active --quiet irqbalance 2>/dev/null; then
        irqbal="active (should be stopped)"
    else
        irqbal="stopped ✓"
    fi

    # Check for isolated CPUs.
    local isolcpus
    isolcpus=$(cat /sys/devices/system/cpu/isolated 2>/dev/null || echo "none")

    printf "  %-28s %s\n" "CPU governor (cpu0):" "$governor"
    printf "  %-28s %s\n" "Turbo boost:" "$([ "$boost" = "0" ] && echo "disabled ✓" || echo "enabled ($boost)")"
    printf "  %-28s %s\n" "THP enabled:" "$thp_enabled"
    printf "  %-28s %s\n" "THP defrag:" "$thp_defrag"
    printf "  %-28s %s\n" "irqbalance:" "$irqbal"
    printf "  %-28s %s\n" "vm.swappiness:" "$swappiness"
    printf "  %-28s %s\n" "kernel.timer_migration:" "$timer_mig"
    printf "  %-28s %s\n" "Isolated CPUs (boot param):" "$isolcpus"

    # Check SMT status.
    local smt_active
    smt_active=$(read_sysfs /sys/devices/system/cpu/smt/active)
    if [[ "$smt_active" == "0" ]]; then
        printf "  %-28s %s\n" "SMT (hyper-threading):" "disabled ✓"
    elif [[ "$smt_active" == "1" ]]; then
        printf "  %-28s %s\n" "SMT (hyper-threading):" "ENABLED — disable in BIOS for best results"
    else
        printf "  %-28s %s\n" "SMT (hyper-threading):" "unknown"
    fi
}

# ── Usage ─────────────────────────────────────────────────────────────

usage() {
    sed -n '2,/^$/p' "$0" | sed 's/^# \?//'
    exit 0
}

# ── Main ──────────────────────────────────────────────────────────────

case "${1:-apply}" in
    --revert|-r)    require_root; revert_state ;;
    --verify|-v)    verify_settings ;;
    --help|-h)      usage ;;
    apply|--apply)  require_root; apply_tuning ;;
    *)              die "Unknown option: $1 (try --help)" ;;
esac
