#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BUILD_DIR=${BUILD_DIR:-$ROOT_DIR/build}
BIN_DIR=$BUILD_DIR/bin
ZK_HOME=${ZK_HOME:-/home/lyt4869/package/apache-zookeeper-3.8.6-bin}
TEST_ROOT=$(mktemp -d /tmp/mprpc_reliability.XXXXXX)
CONFIG="$TEST_ROOT/test.conf"
EXECUTOR_CONFIG="$TEST_ROOT/executor-test.conf"
PROVIDER_PID=""
ZK_PID=""

cleanup() {
    if [[ -n "$PROVIDER_PID" ]]; then
        kill "$PROVIDER_PID" 2>/dev/null || true
        wait "$PROVIDER_PID" 2>/dev/null || true
    fi
    if [[ -n "$ZK_PID" ]]; then
        kill "$ZK_PID" 2>/dev/null || true
        wait "$ZK_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

cat >"$CONFIG" <<EOF
rpcserverip=127.0.0.1
rpcserverport=18887
zookeeperip=127.0.0.1
zookeeperport=2181
EOF

cat >"$EXECUTOR_CONFIG" <<EOF
rpcserverip=127.0.0.1
rpcserverport=18887
zookeeperip=127.0.0.1
zookeeperport=2181
rpcprovideriothreads=1
rpcproviderbusinessworkers=1
rpcproviderbusinesscapacity=2
EOF

if ! timeout 1 bash -c '</dev/tcp/127.0.0.1/2181' 2>/dev/null; then
    "$ZK_HOME/bin/zkServer.sh" start-foreground \
        >"$TEST_ROOT/zookeeper.log" 2>&1 &
    ZK_PID=$!
    sleep 2
fi


printf 'deleteall /FriendServiceRpc\nquit\n' | \
    "$ZK_HOME/bin/zkCli.sh" -server 127.0.0.1:2181 \
    >"$TEST_ROOT/zk-cleanup.log" 2>&1 || true

"$BIN_DIR/provider" -i "$CONFIG" \
    >"$TEST_ROOT/provider.log" 2>&1 &
PROVIDER_PID=$!
for _ in $(seq 1 50); do
    if timeout 1 bash -c '</dev/tcp/127.0.0.1/18887' 2>/dev/null; then
        break
    fi
    sleep 0.1
done
if ! timeout 1 bash -c '</dev/tcp/127.0.0.1/18887' 2>/dev/null; then
    echo "Friend Provider did not start" >&2
    exit 1
fi

timeout 20 "$BIN_DIR/async_rpc_test" -i "$CONFIG"
timeout 5 "$BIN_DIR/provider_deadline_test"
if ! grep -q "SERVER_CANCEL_OBSERVED" "$TEST_ROOT/provider.log"; then
    echo "Provider did not observe propagated cancellation" >&2
    exit 1
fi

kill "$PROVIDER_PID" 2>/dev/null || true
wait "$PROVIDER_PID" 2>/dev/null || true
PROVIDER_PID=""

"$BIN_DIR/provider" -i "$EXECUTOR_CONFIG" \
    >"$TEST_ROOT/provider-executor.log" 2>&1 &
PROVIDER_PID=$!
for _ in $(seq 1 50); do
    if timeout 1 bash -c '</dev/tcp/127.0.0.1/18887' 2>/dev/null; then
        break
    fi
    sleep 0.1
done
if ! timeout 1 bash -c '</dev/tcp/127.0.0.1/18887' 2>/dev/null; then
    echo "Executor test Provider did not start" >&2
    exit 1
fi

timeout 5 "$BIN_DIR/provider_executor_test"
if grep -q "QUEUED_BUSINESS_EXECUTED" \
    "$TEST_ROOT/provider-executor.log"; then
    echo "Cancelled queued business was executed" >&2
    exit 1
fi
echo "PASS: async, deadline, cancellation, concurrency and shutdown"
echo "artifacts: $TEST_ROOT"
