#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BUILD_DIR=${BUILD_DIR:-$ROOT_DIR/build}
BIN_DIR=$BUILD_DIR/bin
ZK_HOME=${ZK_HOME:-/home/lyt4869/package/apache-zookeeper-3.8.6-bin}
TEST_ROOT=$(mktemp -d /tmp/mprpc_e2e.XXXXXX)
CONFIG="$TEST_ROOT/test.conf"
CONFIG_SECOND="$TEST_ROOT/test-second.conf"
SERVER_PID=""
SECOND_SERVER_PID=""
ZK_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    if [[ -n "$SECOND_SERVER_PID" ]]; then
        kill "$SECOND_SERVER_PID" 2>/dev/null || true
        wait "$SECOND_SERVER_PID" 2>/dev/null || true
    fi
    if [[ -n "$ZK_PID" ]]; then
        kill "$ZK_PID" 2>/dev/null || true
        wait "$ZK_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

cat >"$CONFIG" <<EOF
rpcserverip=127.0.0.1
rpcserverport=18888
zookeeperip=127.0.0.1
zookeeperport=2181
EOF
cat >"$CONFIG_SECOND" <<EOF
rpcserverip=127.0.0.1
rpcserverport=18889
zookeeperip=127.0.0.1
zookeeperport=2181
EOF

if ! timeout 1 bash -c '</dev/tcp/127.0.0.1/2181' 2>/dev/null; then
    "$ZK_HOME/bin/zkServer.sh" start-foreground \
        >"$TEST_ROOT/zookeeper.log" 2>&1 &
    ZK_PID=$!
    sleep 2
fi

# Integration tests own this local service namespace. Remove provider nodes
# whose ZooKeeper sessions may still be expiring after an interrupted run.
printf 'deleteall /FileTransferServiceRpc\nquit\n' | \
    "$ZK_HOME/bin/zkCli.sh" -server 127.0.0.1:2181 \
    >"$TEST_ROOT/zk-cleanup.log" 2>&1 || true

start_server() {
    "$BIN_DIR/file_transfer_server" -i "$CONFIG" \
        "$TEST_ROOT/uploads" >"$TEST_ROOT/server.log" 2>&1 &
    SERVER_PID=$!
    for _ in $(seq 1 50); do
        if timeout 1 bash -c '</dev/tcp/127.0.0.1/18888' 2>/dev/null; then
            return
        fi
        sleep 0.1
    done
    echo "file transfer server did not start" >&2
    exit 1
}

stop_server() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        SERVER_PID=""
    fi
}

start_second_server() {
    "$BIN_DIR/file_transfer_server" -i "$CONFIG_SECOND" \
        "$TEST_ROOT/uploads-second" >"$TEST_ROOT/server-second.log" 2>&1 &
    SECOND_SERVER_PID=$!
    for _ in $(seq 1 50); do
        if timeout 1 bash -c '</dev/tcp/127.0.0.1/18889' 2>/dev/null; then
            return
        fi
        sleep 0.1
    done
    echo "second file transfer server did not start" >&2
    exit 1
}

stop_second_server() {
    if [[ -n "$SECOND_SERVER_PID" ]]; then
        kill "$SECOND_SERVER_PID" 2>/dev/null || true
        wait "$SECOND_SERVER_PID" 2>/dev/null || true
        SECOND_SERVER_PID=""
    fi
}

cmake -S "$ROOT_DIR" -B "$BUILD_DIR"
cmake --build "$BUILD_DIR" -j2
"$BIN_DIR/codec_test"
"$BIN_DIR/file_transfer_manager_test"
"$BIN_DIR/file_transfer_service_test"
"$BIN_DIR/file_transfer_overload_test"

start_server
start_second_server
: >"$TEST_ROOT/empty.bin"
"$BIN_DIR/file_transfer_client" -i "$CONFIG" \
    --transfer-id 00000000000000000000000000000005 \
    "$TEST_ROOT/empty.bin" affinity-primary.bin \
    >"$TEST_ROOT/affinity-primary.log" 2>&1
"$BIN_DIR/file_transfer_client" -i "$CONFIG" \
    --transfer-id 00000000000000000000000000000004 \
    "$TEST_ROOT/empty.bin" affinity-second.bin \
    >"$TEST_ROOT/affinity-second.log" 2>&1
test -f "$TEST_ROOT/uploads/completed/00000000000000000000000000000005_affinity-primary.bin"
test -f "$TEST_ROOT/uploads-second/completed/00000000000000000000000000000004_affinity-second.bin"

dd if=/dev/urandom of="$TEST_ROOT/happy.bin" bs=1M count=12 status=none
"$BIN_DIR/file_transfer_client" -i "$CONFIG" \
    --window 8 "$TEST_ROOT/happy.bin" happy.bin \
    >"$TEST_ROOT/happy-client.log" 2>&1
HAPPY_RESULT=$(find "$TEST_ROOT/uploads" "$TEST_ROOT/uploads-second" \
    -type f -name '*_happy.bin' -print -quit)
test -n "$HAPPY_RESULT"
test "$(sha256sum "$TEST_ROOT/happy.bin" | awk '{print $1}')" = \
     "$(sha256sum "$HAPPY_RESULT" | awk '{print $1}')"
stop_second_server
sleep 0.2

# Interrupt a slower transfer after several chunks, then recover from sidecar.
dd if=/dev/urandom of="$TEST_ROOT/resume.bin" bs=1M count=64 status=none
set +e
timeout 15 "$BIN_DIR/file_transfer_client" -i "$CONFIG" \
    --window 1 --chunk-size 262144 --timeout-ms 1000 --max-retries 0 \
    --transfer-id 00000000000000000000000000000001 \
    "$TEST_ROOT/resume.bin" resume.bin \
    >"$TEST_ROOT/interrupted-client.log" 2>&1 &
INTERRUPTED_PID=$!
for _ in $(seq 1 100); do
    if [[ -f "$TEST_ROOT/uploads/temporary/00000000000000000000000000000001.part" ]]; then
        break
    fi
    sleep 0.05
done
if [[ ! -f "$TEST_ROOT/uploads/temporary/00000000000000000000000000000001.part" ]]; then
    kill "$INTERRUPTED_PID" 2>/dev/null || true
    wait "$INTERRUPTED_PID" 2>/dev/null || true
    set -e
    echo "interrupted upload did not reach the chunk phase" >&2
    exit 1
fi
sleep 0.25
stop_server
wait "$INTERRUPTED_PID"
INTERRUPTED_STATUS=$?
set -e
if [[ "$INTERRUPTED_STATUS" -ne 1 ]]; then
    echo "interrupted client exited unexpectedly: $INTERRUPTED_STATUS" >&2
    cat "$TEST_ROOT/interrupted-client.log" >&2
    exit 1
fi

TRANSFER_ID=$(sed -n 's/^transfer_id: //p' \
    "$TEST_ROOT/interrupted-client.log" | tail -1)
test -n "$TRANSFER_ID"
start_server
"$BIN_DIR/file_transfer_client" -i "$CONFIG" \
    --window 8 --transfer-id "$TRANSFER_ID" \
    "$TEST_ROOT/resume.bin" resume.bin \
    >"$TEST_ROOT/resumed-client.log" 2>&1
RESUMED_RESULT=$(find "$TEST_ROOT/uploads/completed" \
    -type f -name "${TRANSFER_ID}_resume.bin" -print -quit)
test -n "$RESUMED_RESULT"
test "$(sha256sum "$TEST_ROOT/resume.bin" | awk '{print $1}')" = \
     "$(sha256sum "$RESUMED_RESULT" | awk '{print $1}')"

# A retry-enabled client must survive a short Provider restart by resending
# the in-flight chunk. The sidecar makes an acknowledged-on-disk chunk
# idempotent even if its original response was lost.
dd if=/dev/urandom of="$TEST_ROOT/automatic.bin" bs=1M count=32 status=none
set +e
timeout 30 "$BIN_DIR/file_transfer_client" -i "$CONFIG" \
    --window 1 --chunk-size 262144 --timeout-ms 1000 --max-retries 3 \
    --transfer-id 00000000000000000000000000000003 \
    "$TEST_ROOT/automatic.bin" automatic.bin \
    >"$TEST_ROOT/automatic-client.log" 2>&1 &
AUTOMATIC_PID=$!
for _ in $(seq 1 100); do
    if [[ -f "$TEST_ROOT/uploads/temporary/00000000000000000000000000000003.part" ]]; then
        break
    fi
    sleep 0.05
done
if [[ ! -f "$TEST_ROOT/uploads/temporary/00000000000000000000000000000003.part" ]]; then
    kill "$AUTOMATIC_PID" 2>/dev/null || true
    wait "$AUTOMATIC_PID" 2>/dev/null || true
    set -e
    echo "automatic upload did not reach the chunk phase" >&2
    exit 1
fi
sleep 0.25
stop_server
sleep 0.35
start_server
wait "$AUTOMATIC_PID"
AUTOMATIC_STATUS=$?
set -e
if [[ "$AUTOMATIC_STATUS" -ne 0 ]]; then
    echo "retrying client failed after Provider restart: $AUTOMATIC_STATUS" >&2
    cat "$TEST_ROOT/automatic-client.log" >&2
    exit 1
fi
AUTOMATIC_RESULT=$(find "$TEST_ROOT/uploads/completed" \
    -type f -name '00000000000000000000000000000003_automatic.bin' \
    -print -quit)
test -n "$AUTOMATIC_RESULT"
test "$(sha256sum "$TEST_ROOT/automatic.bin" | awk '{print $1}')" = \
     "$(sha256sum "$AUTOMATIC_RESULT" | awk '{print $1}')"

if [[ ${RUN_BENCHMARK:-0} == 1 ]]; then
    "$BIN_DIR/file_transfer_benchmark" -i "$CONFIG" \
        "$TEST_ROOT/benchmark" | tee "$TEST_ROOT/benchmark.csv"
fi

echo "PASS: real RPC upload, manual resume, automatic retry and hash checks"
echo "artifacts: $TEST_ROOT"
