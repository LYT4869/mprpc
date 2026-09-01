#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BUILD_DIR=${BUILD_DIR:-$ROOT_DIR/build}
BIN_DIR=$BUILD_DIR/bin
ZK_HOME=${ZK_HOME:-/home/lyt4869/package/apache-zookeeper-3.8.6-bin}
TEST_ROOT=$(mktemp -d /tmp/mprpc_zk_watch.XXXXXX)
CONFIG="$TEST_ROOT/test.conf"
ZK_PID=""
PROVIDER_A_PID=""
PROVIDER_B_PID=""
CLIENT_PID=""

cleanup() {
    for pid in "$CLIENT_PID" "$PROVIDER_B_PID" "$PROVIDER_A_PID"; do
        if [[ -n "$pid" ]]; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    printf 'deleteall /FriendServiceRpc\nquit\n' | \
        "$ZK_HOME/bin/zkCli.sh" -server 127.0.0.1:2181 \
        >"$TEST_ROOT/zk-service-cleanup.log" 2>&1 || true
    printf 'deleteall /mprpc-watch-integration\nquit\n' | \
        "$ZK_HOME/bin/zkCli.sh" -server 127.0.0.1:2181 \
        >"$TEST_ROOT/zk-cleanup-after.log" 2>&1 || true
    if [[ -n "$ZK_PID" ]]; then
        kill "$ZK_PID" 2>/dev/null || true
        wait "$ZK_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

cat >"$CONFIG" <<EOF
rpcserverip=127.0.0.1
rpcserverport=18891
zookeeperip=127.0.0.1
zookeeperport=2181
EOF

if ! timeout 1 bash -c '</dev/tcp/127.0.0.1/2181' 2>/dev/null; then
    "$ZK_HOME/bin/zkServer.sh" start-foreground \
        >"$TEST_ROOT/zookeeper.log" 2>&1 &
    ZK_PID=$!
    sleep 2
fi

printf 'deleteall /mprpc-watch-integration\nquit\n' | \
    "$ZK_HOME/bin/zkCli.sh" -server 127.0.0.1:2181 \
    >"$TEST_ROOT/zk-cleanup-before.log" 2>&1 || true

timeout 10 "$BIN_DIR/zk_child_watch_test" -i "$CONFIG"

if [[ "${1:-}" == "--zk-only" ]]; then
    echo "PASS: ZooKeeper child watch integration"
    exit 0
fi

wait_for_file() {
    local path=$1
    for _ in $(seq 1 200); do
        [[ -f "$path" ]] && return 0
        sleep 0.05
    done
    echo "Timed out waiting for marker: $path" >&2
    return 1
}

wait_for_port() {
    local port=$1
    for _ in $(seq 1 100); do
        if timeout 1 bash -c "</dev/tcp/127.0.0.1/$port" \
                2>/dev/null; then
            return 0
        fi
        sleep 0.05
    done
    echo "Provider did not listen on port $port" >&2
    return 1
}

delete_newest_provider() {
    local providers_path=/FriendServiceRpc/GetFriendList/providers
    local newest
    newest=$(printf 'ls %s\nquit\n' "$providers_path" | \
        "$ZK_HOME/bin/zkCli.sh" -server 127.0.0.1:2181 2>/dev/null | \
        grep -o 'provider-[0-9]\+' | sort -u | tail -1)
    if [[ -z "$newest" ]]; then
        echo "Could not locate Provider B znode" >&2
        return 1
    fi
    printf 'delete %s/%s\nquit\n' "$providers_path" "$newest" | \
        "$ZK_HOME/bin/zkCli.sh" -server 127.0.0.1:2181 \
        >"$TEST_ROOT/zk-delete-provider-b.log" 2>&1
}

CONFIG_A="$TEST_ROOT/provider-a.conf"
CONFIG_B="$TEST_ROOT/provider-b.conf"
MARKERS="$TEST_ROOT/markers"
mkdir -p "$MARKERS"
sed 's/rpcserverport=18891/rpcserverport=18891/' "$CONFIG" >"$CONFIG_A"
sed 's/rpcserverport=18891/rpcserverport=18892/' "$CONFIG" >"$CONFIG_B"

printf 'deleteall /FriendServiceRpc\nquit\n' | \
    "$ZK_HOME/bin/zkCli.sh" -server 127.0.0.1:2181 \
    >"$TEST_ROOT/zk-service-cleanup-before.log" 2>&1 || true

"$BIN_DIR/discovery_provider" -i "$CONFIG_A" \
    >"$TEST_ROOT/provider-a.log" 2>&1 &
PROVIDER_A_PID=$!
wait_for_port 18891

"$BIN_DIR/discovery_watch_test" -i "$CONFIG_A" --markers "$MARKERS" \
    >"$TEST_ROOT/client.log" 2>&1 &
CLIENT_PID=$!
wait_for_file "$MARKERS/a-warmed"

"$BIN_DIR/discovery_provider" -i "$CONFIG_B" \
    >"$TEST_ROOT/provider-b-first.log" 2>&1 &
PROVIDER_B_PID=$!
wait_for_port 18892
touch "$MARKERS/b-added"
wait_for_file "$MARKERS/b-observed"

delete_newest_provider
kill "$PROVIDER_B_PID" 2>/dev/null || true
wait "$PROVIDER_B_PID" 2>/dev/null || true
PROVIDER_B_PID=""
touch "$MARKERS/b-removed"
wait_for_file "$MARKERS/a-observed"

"$BIN_DIR/discovery_provider" -i "$CONFIG_B" \
    >"$TEST_ROOT/provider-b-second.log" 2>&1 &
PROVIDER_B_PID=$!
wait_for_port 18892
touch "$MARKERS/b-restarted"

if ! wait "$CLIENT_PID"; then
    cat "$TEST_ROOT/client.log" >&2
    exit 1
fi
CLIENT_PID=""
cat "$TEST_ROOT/client.log"
echo "PASS: dynamic ZooKeeper service discovery"
echo "artifacts: $TEST_ROOT"
