#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BUILD_DIR=$ROOT_DIR/build-release
BIN_DIR=$BUILD_DIR/bin
ZK_HOME=${ZK_HOME:-/home/lyt4869/package/apache-zookeeper-3.8.6-bin}
PROFILE=${1:-smoke}
if [[ $# -gt 0 ]]; then
    shift
fi
BENCHMARK_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --size-mib|--window|--concurrency|--samples|--warmup)
            if [[ $# -lt 2 || ! "$2" =~ ^[0-9]+$ ]]; then
                echo "$1 requires a non-negative integer" >&2
                exit 1
            fi
            BENCHMARK_ARGS+=("$1" "$2")
            shift 2
            ;;
        *)
            echo "unsupported benchmark override: $1" >&2
            exit 1
            ;;
    esac
done
OUTPUT_ROOT=${OUTPUT_ROOT:-$ROOT_DIR/artifacts/release-benchmark-$PROFILE}
RUN_ROOT=$(mktemp -d /tmp/mprpc_release_benchmark.XXXXXX)
CONFIG="$RUN_ROOT/benchmark.conf"
SERVER_PID=""
CLIENT_PID=""
ZK_PID=""
SUCCESS=0

cleanup() {
    if [[ -n "$CLIENT_PID" ]]; then
        kill "$CLIENT_PID" 2>/dev/null || true
        wait "$CLIENT_PID" 2>/dev/null || true
    fi
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    if [[ -n "$ZK_PID" ]]; then
        kill "$ZK_PID" 2>/dev/null || true
        wait "$ZK_PID" 2>/dev/null || true
    fi
    if [[ "$SUCCESS" == 1 ]]; then
        rm -rf "$RUN_ROOT/uploads" "$RUN_ROOT/work"
    else
        echo "benchmark failure artifacts: $RUN_ROOT" >&2
    fi
}
trap cleanup EXIT

mkdir -p "$OUTPUT_ROOT"
rm -f "$OUTPUT_ROOT"/*.csv "$OUTPUT_ROOT"/*.log \
    "$OUTPUT_ROOT"/report.md
cat >"$CONFIG" <<EOF
rpcserverip=127.0.0.1
rpcserverport=18988
zookeeperip=127.0.0.1
zookeeperport=2181
metricsintervalms=0
EOF

if ! timeout 1 bash -c '</dev/tcp/127.0.0.1/2181' 2>/dev/null; then
    "$ZK_HOME/bin/zkServer.sh" start-foreground \
        >"$RUN_ROOT/zookeeper.log" 2>&1 &
    ZK_PID=$!
    sleep 2
fi

# The benchmark owns this test service namespace. Remove stale ephemeral
# registrations left by interrupted local integration runs.
printf 'deleteall /FileTransferServiceRpc\nquit\n' | \
    "$ZK_HOME/bin/zkCli.sh" -server 127.0.0.1:2181 \
    >"$RUN_ROOT/zk-cleanup.log" 2>&1 || true

cmake -S "$ROOT_DIR" -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE=Release
cmake --build "$BUILD_DIR" -j2 \
    --target file_transfer_server file_transfer_benchmark

"$BIN_DIR/file_transfer_server" -i "$CONFIG" \
    "$RUN_ROOT/uploads" >"$OUTPUT_ROOT/provider.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 100); do
    if timeout 1 bash -c '</dev/tcp/127.0.0.1/18988' 2>/dev/null; then
        break
    fi
    sleep 0.1
done
if ! timeout 1 bash -c '</dev/tcp/127.0.0.1/18988' 2>/dev/null; then
    echo "release benchmark Provider did not start" >&2
    exit 1
fi

sample_process() {
    local pid=$1
    local output=$2
    echo "timestamp_ms,cpu_ticks,rss_kib,peak_rss_kib" >"$output"
    while kill -0 "$pid" 2>/dev/null && [[ -r "/proc/$pid/stat" ]]; do
        local timestamp cpu rss peak
        timestamp=$(date +%s%3N)
        cpu=$(awk '{print $14 + $15}' "/proc/$pid/stat" 2>/dev/null) || break
        rss=$(awk '/^VmRSS:/ {print $2}' "/proc/$pid/status" 2>/dev/null) || break
        peak=$(awk '/^VmHWM:/ {print $2}' "/proc/$pid/status" 2>/dev/null) || break
        echo "$timestamp,$cpu,${rss:-0},${peak:-0}" >>"$output"
        sleep 0.1
    done
}

sample_process "$SERVER_PID" "$OUTPUT_ROOT/provider-resources.csv" &
SERVER_MONITOR_PID=$!

"$BIN_DIR/file_transfer_benchmark" -i "$CONFIG" \
    "$RUN_ROOT/work" --profile "$PROFILE" \
    --connection-mode warm "${BENCHMARK_ARGS[@]}" \
    --output "$OUTPUT_ROOT/results.csv" \
    >"$OUTPUT_ROOT/client.log" 2>&1 &
CLIENT_PID=$!
sample_process "$CLIENT_PID" "$OUTPUT_ROOT/client-resources.csv" &
CLIENT_MONITOR_PID=$!

set +e
wait "$CLIENT_PID"
CLIENT_STATUS=$?
set -e
CLIENT_PID=""
wait "$CLIENT_MONITOR_PID" 2>/dev/null || true
if [[ "$CLIENT_STATUS" -ne 0 ]]; then
    echo "release benchmark failed with status $CLIENT_STATUS" >&2
    exit "$CLIENT_STATUS"
fi

kill "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""
wait "$SERVER_MONITOR_PID" 2>/dev/null || true

{
    echo "# RPC Release Benchmark"
    echo
    echo "- Date: $(date --iso-8601=seconds)"
    echo "- Commit: $(git -C "$ROOT_DIR" rev-parse HEAD)"
    echo "- Profile: $PROFILE"
    echo "- Build type: Release"
    echo "- Compiler: $(c++ --version | head -1)"
    echo "- Kernel: $(uname -srmo)"
    echo "- CPU: $(awk -F: '/model name/ {gsub(/^ /, "", $2); print $2; exit}' /proc/cpuinfo)"
    echo "- Logical CPUs: $(nproc)"
    echo "- Storage root: $RUN_ROOT"
    echo
    echo "Raw measurements are stored in results.csv, client-resources.csv,"
    echo "provider-resources.csv, client.log and provider.log."
} >"$OUTPUT_ROOT/report.md"

SUCCESS=1
echo "PASS: Release benchmark profile=$PROFILE"
echo "artifacts: $OUTPUT_ROOT"
