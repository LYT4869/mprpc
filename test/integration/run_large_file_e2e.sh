#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BUILD_DIR=${BUILD_DIR:-$ROOT_DIR/build-release}
BIN_DIR=$BUILD_DIR/bin
ZK_HOME=${ZK_HOME:-/home/lyt4869/package/apache-zookeeper-3.8.6-bin}
RUN_TAG=$(date +%Y%m%d-%H%M%S)
OUTPUT_ROOT=${OUTPUT_ROOT:-$ROOT_DIR/artifacts/large-file-e2e-$RUN_TAG}
RUN_ROOT=$(mktemp -d /tmp/mprpc_large_file.XXXXXX)
ZK_PORT=${ZK_PORT:-22182}
PROVIDER_PORT=${PROVIDER_PORT:-18989}
ZK_CONFIG="$RUN_ROOT/zoo.cfg"
RPC_CONFIG="$RUN_ROOT/rpc.conf"
ZK_PID=""
PROVIDER_PID=""
MONITOR_PID=""
SUCCESS=0

cleanup() {
    if [[ -n "$PROVIDER_PID" ]]; then
        kill "$PROVIDER_PID" 2>/dev/null || true
        wait "$PROVIDER_PID" 2>/dev/null || true
    fi
    if [[ -n "$ZK_PID" ]]; then
        kill "$ZK_PID" 2>/dev/null || true
        wait "$ZK_PID" 2>/dev/null || true
    fi
    if [[ -n "$MONITOR_PID" ]]; then
        wait "$MONITOR_PID" 2>/dev/null || true
    fi
    if [[ "$SUCCESS" == 1 ]]; then
        rm -rf "$RUN_ROOT"
    else
        echo "large-file failure artifacts: $RUN_ROOT" >&2
    fi
}
trap cleanup EXIT

if timeout 1 bash -c "</dev/tcp/127.0.0.1/$ZK_PORT" 2>/dev/null; then
    echo "Test ZooKeeper port $ZK_PORT is already in use" >&2
    exit 1
fi
if timeout 1 bash -c "</dev/tcp/127.0.0.1/$PROVIDER_PORT" 2>/dev/null; then
    echo "Test Provider port $PROVIDER_PORT is already in use" >&2
    exit 1
fi

if [[ -d "$OUTPUT_ROOT" ]] &&
        [[ -n "$(find "$OUTPUT_ROOT" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "Output directory is not empty: $OUTPUT_ROOT" >&2
    exit 1
fi
mkdir -p "$RUN_ROOT/zk-data" "$OUTPUT_ROOT"
cat >"$ZK_CONFIG" <<EOF
tickTime=500
dataDir=$RUN_ROOT/zk-data
clientPort=$ZK_PORT
minSessionTimeout=1000
maxSessionTimeout=20000
admin.enableServer=false
EOF
cat >"$RPC_CONFIG" <<EOF
rpcserverip=127.0.0.1
rpcserverport=$PROVIDER_PORT
zookeeperip=127.0.0.1
zookeeperport=$ZK_PORT
metricsintervalms=0
EOF

cmake -S "$ROOT_DIR" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release
cmake --build "$BUILD_DIR" -j2 \
    --target file_transfer_server file_transfer_client

"$ZK_HOME/bin/zkServer.sh" start-foreground "$ZK_CONFIG" \
    >"$OUTPUT_ROOT/zookeeper.log" 2>&1 &
ZK_PID=$!
for _ in $(seq 1 100); do
    if timeout 1 bash -c "</dev/tcp/127.0.0.1/$ZK_PORT" 2>/dev/null; then
        break
    fi
    sleep 0.05
done
if ! timeout 1 bash -c "</dev/tcp/127.0.0.1/$ZK_PORT" 2>/dev/null; then
    echo "large-file ZooKeeper did not start" >&2
    exit 1
fi

"$BIN_DIR/file_transfer_server" -i "$RPC_CONFIG" "$RUN_ROOT/uploads" \
    >"$OUTPUT_ROOT/provider.log" 2>&1 &
PROVIDER_PID=$!
for _ in $(seq 1 100); do
    if timeout 1 bash -c "</dev/tcp/127.0.0.1/$PROVIDER_PORT" \
            2>/dev/null; then
        break
    fi
    sleep 0.05
done
if ! timeout 1 bash -c "</dev/tcp/127.0.0.1/$PROVIDER_PORT" \
        2>/dev/null; then
    echo "large-file Provider did not start" >&2
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

sample_process "$PROVIDER_PID" "$OUTPUT_ROOT/provider-resources.csv" &
MONITOR_PID=$!
RUN_TIMESTAMP=$(date --iso-8601=seconds)
RUN_COMMIT=$(git -C "$ROOT_DIR" rev-parse HEAD)
echo "commit,run_timestamp,build_type,size_mib,source_bytes,stored_bytes,"\
"elapsed_seconds,throughput_mib_s,retries,max_in_flight,"\
"client_peak_rss_kib,source_sha256,stored_sha256,result" \
    >"$OUTPUT_ROOT/results.csv"

extract_metric() {
    local key=$1
    local log=$2
    awk -v label="$key:" \
        '$1 == label {print $2; found = 1} END {if (!found) exit 1}' "$log"
}

for size_mib in 256 1024; do
    input="$RUN_ROOT/input-${size_mib}mib.bin"
    remote="large-${size_mib}mib.bin"
    transfer_id=$(printf '%032x' "$size_mib")
    truncate -s "${size_mib}M" "$input"
    /usr/bin/time -f '%e,%M' -o "$RUN_ROOT/time.csv" \
        timeout 180 "$BIN_DIR/file_transfer_client" -i "$RPC_CONFIG" \
        --window 8 --timeout-ms 30000 --max-retries 3 \
        --transfer-id "$transfer_id" "$input" "$remote" \
        >"$OUTPUT_ROOT/client-${size_mib}mib.log" 2>&1

    output="$RUN_ROOT/uploads/completed/${transfer_id}_${remote}"
    test -f "$output"
    source_sha=$(sha256sum "$input" | awk '{print $1}')
    stored_sha=$(sha256sum "$output" | awk '{print $1}')
    test "$source_sha" = "$stored_sha"
    IFS=, read -r elapsed peak_rss <"$RUN_ROOT/time.csv"
    source_bytes=$(stat -c %s "$input")
    stored_bytes=$(stat -c %s "$output")
    test "$source_bytes" = "$stored_bytes"
    retries=$(extract_metric retries \
        "$OUTPUT_ROOT/client-${size_mib}mib.log")
    max_in_flight=$(extract_metric max_in_flight \
        "$OUTPUT_ROOT/client-${size_mib}mib.log")
    [[ "$retries" =~ ^[0-9]+$ ]]
    [[ "$max_in_flight" =~ ^[0-9]+$ ]]
    throughput=$(awk -v size="$size_mib" -v seconds="$elapsed" \
        'BEGIN {printf "%.2f", size / seconds}')
    echo "$RUN_COMMIT,$RUN_TIMESTAMP,Release,$size_mib,$source_bytes,"\
"$stored_bytes,$elapsed,$throughput,$retries,$max_in_flight,$peak_rss,"\
"$source_sha,$stored_sha,PASS" \
        >>"$OUTPUT_ROOT/results.csv"
    rm -f "$input" "$output"
done

kill "$PROVIDER_PID" 2>/dev/null || true
wait "$PROVIDER_PID" 2>/dev/null || true
PROVIDER_PID=""
wait "$MONITOR_PID" 2>/dev/null || true
MONITOR_PID=""
COMPILER=$(c++ --version | head -1)
KERNEL=$(uname -srmo)
CPU_MODEL=$(awk -F: \
    '/model name/ {gsub(/^ /, "", $2); print $2; exit}' /proc/cpuinfo)
LOGICAL_CPUS=$(nproc)
FILESYSTEM=$(findmnt -no FSTYPE -T "$RUN_ROOT")
{
    echo "# RPC Large-File E2E"
    echo
    echo "- Date: $RUN_TIMESTAMP"
    echo "- Commit: $RUN_COMMIT"
    echo "- Build type: Release"
    echo "- Compiler: $COMPILER"
    echo "- Kernel: $KERNEL"
    echo "- CPU: $CPU_MODEL"
    echo "- Logical CPUs: $LOGICAL_CPUS"
    echo "- Filesystem: $FILESYSTEM"
    echo
    echo "The source and stored byte counts and SHA-256 digests must match."
} >"$OUTPUT_ROOT/report.md"
SUCCESS=1
echo "PASS: 256 MiB and 1 GiB uploads matched external SHA-256"
echo "artifacts: $OUTPUT_ROOT"
