#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
ZK_HOME=${ZK_HOME:-/home/lyt4869/package/apache-zookeeper-3.8.6-bin}
TEST_ROOT=$(mktemp -d /tmp/mprpc_reliability.XXXXXX)
CONFIG="$TEST_ROOT/test.conf"
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

if ! timeout 1 bash -c '</dev/tcp/127.0.0.1/2181' 2>/dev/null; then
    "$ZK_HOME/bin/zkServer.sh" start-foreground \
        >"$TEST_ROOT/zookeeper.log" 2>&1 &
    ZK_PID=$!
    sleep 2
fi

"$ROOT_DIR/bin/provider" -i "$CONFIG" \
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

timeout 20 "$ROOT_DIR/bin/async_rpc_test" -i "$CONFIG"
echo "PASS: async, timeout race, cancellation, concurrency and shutdown"
echo "artifacts: $TEST_ROOT"
