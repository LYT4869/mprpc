#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BUILD_DIR=${BUILD_DIR:-$ROOT_DIR/build}
BIN_DIR=$BUILD_DIR/bin
ZK_HOME=${ZK_HOME:-/home/lyt4869/package/apache-zookeeper-3.8.6-bin}
TEST_ROOT=$(mktemp -d /tmp/mprpc_zk_watch.XXXXXX)
CONFIG="$TEST_ROOT/test.conf"
ZK_PID=""

cleanup() {
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

echo "Dynamic discovery test is not built yet" >&2
exit 1
