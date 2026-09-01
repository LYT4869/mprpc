#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BUILD_DIR=${BUILD_DIR:-$ROOT_DIR/build}
BIN_DIR=$BUILD_DIR/bin
ZK_HOME=${ZK_HOME:-/home/lyt4869/package/apache-zookeeper-3.8.6-bin}
TEST_ROOT=$(mktemp -d /tmp/mprpc_provider_registry.XXXXXX)
ZK_DATA="$TEST_ROOT/zk-data"
ZK_CONFIG="$TEST_ROOT/zoo.cfg"
RPC_CONFIG="$TEST_ROOT/rpc.conf"
ZK_PORT=22181
PROVIDER_PORT=18901
ZK_PID=""
PROVIDER_PID=""

stop_zookeeper() {
    if [[ -n "$ZK_PID" ]]; then
        kill "$ZK_PID" 2>/dev/null || true
        wait "$ZK_PID" 2>/dev/null || true
        ZK_PID=""
    fi
}

cleanup() {
    if [[ -n "$PROVIDER_PID" ]]; then
        kill "$PROVIDER_PID" 2>/dev/null || true
        wait "$PROVIDER_PID" 2>/dev/null || true
    fi
    stop_zookeeper
}
trap cleanup EXIT

if timeout 1 bash -c "</dev/tcp/127.0.0.1/$ZK_PORT" 2>/dev/null; then
    echo "Test ZooKeeper port $ZK_PORT is already in use" >&2
    exit 1
fi

mkdir -p "$ZK_DATA"
cat >"$ZK_CONFIG" <<EOF
tickTime=500
dataDir=$ZK_DATA
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
rpcprovideriothreads=1
rpcproviderbusinessworkers=1
rpcproviderbusinesscapacity=8
EOF

start_zookeeper() {
    "$ZK_HOME/bin/zkServer.sh" start-foreground "$ZK_CONFIG" \
        >>"$TEST_ROOT/zookeeper.log" 2>&1 &
    ZK_PID=$!
    for _ in $(seq 1 80); do
        if timeout 1 bash -c "</dev/tcp/127.0.0.1/$ZK_PORT" \
                2>/dev/null; then
            return 0
        fi
        sleep 0.05
    done
    echo "ZooKeeper did not start on port $ZK_PORT" >&2
    return 1
}

provider_count() {
    local path=/FriendServiceRpc/GetFriendList/providers
    local output
    output=$(printf 'ls %s\nquit\n' "$path" | \
        "$ZK_HOME/bin/zkCli.sh" -server "127.0.0.1:$ZK_PORT" \
        2>/dev/null || true)
    printf '%s\n' "$output" | awk '
        {
            line = $0
            while (match(line, /provider-[0-9]+/)) {
                seen[substr(line, RSTART, RLENGTH)] = 1
                line = substr(line, RSTART + RLENGTH)
            }
        }
        END {
            count = 0
            for (node in seen) ++count
            print count
        }'
}

provider_nodes() {
    local path=/FriendServiceRpc/GetFriendList/providers
    printf 'ls %s\nquit\n' "$path" | \
        "$ZK_HOME/bin/zkCli.sh" -server "127.0.0.1:$ZK_PORT" \
        2>/dev/null | awk '
        {
            line = $0
            while (match(line, /provider-[0-9]+/)) {
                print substr(line, RSTART, RLENGTH)
                line = substr(line, RSTART + RLENGTH)
            }
        }' | sort -u
}

wait_for_provider_count() {
    local expected=$1
    for _ in $(seq 1 20); do
        if [[ "$(provider_count)" == "$expected" ]]; then
            return 0
        fi
        sleep 0.2
    done
    echo "Expected $expected Provider node(s), got $(provider_count)" >&2
    return 1
}

start_zookeeper
"$BIN_DIR/provider" -i "$RPC_CONFIG" \
    >"$TEST_ROOT/provider.log" 2>&1 &
PROVIDER_PID=$!

for _ in $(seq 1 100); do
    if timeout 1 bash -c "</dev/tcp/127.0.0.1/$PROVIDER_PORT" \
            2>/dev/null; then
        break
    fi
    sleep 0.05
done
wait_for_provider_count 1
INITIAL_PROVIDER_NODES=$(provider_nodes)
timeout 8 "$BIN_DIR/provider_reregistration_test" -i "$RPC_CONFIG"

# A short disconnect must preserve the original session and must not duplicate
# the ephemeral Provider node.
stop_zookeeper
sleep 0.3
start_zookeeper
wait_for_provider_count 1
sleep 0.5
wait_for_provider_count 1
if [[ "$(provider_nodes)" != "$INITIAL_PROVIDER_NODES" ]]; then
    echo "Provider node changed during a recoverable disconnect" >&2
    exit 1
fi

# Removing the test server's session database forces the old client session to
# expire when ZooKeeper returns. The Provider process itself remains alive.
stop_zookeeper
rm -rf "$ZK_DATA/version-2"
start_zookeeper
wait_for_provider_count 1

if ! kill -0 "$PROVIDER_PID" 2>/dev/null; then
    echo "Provider exited during ZooKeeper recovery" >&2
    exit 1
fi
timeout 8 "$BIN_DIR/provider_reregistration_test" -i "$RPC_CONFIG"

echo "PASS: Provider survived disconnect and re-registered after expiration"
echo "artifacts: $TEST_ROOT"
