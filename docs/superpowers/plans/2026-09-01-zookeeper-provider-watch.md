# ZooKeeper Provider Watch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Provider membership changes invalidate the affected client discovery cache immediately while retaining the existing three-second TTL and TCP-failure fallback.

**Architecture:** `ZkClient` exposes status-aware reads plus one-shot child and session callbacks. ZooKeeper callback threads only enqueue weak-pointer tasks onto the `ChannelCore` EventLoop; the next RPC performs a watched refresh and re-arms the watch. `ChannelCore` keeps its current per-core ZooKeeper client and method cache.

**Tech Stack:** C++17, Apache ZooKeeper C client, Muduo EventLoop, CMake/CTest, Protobuf RPC

**Spec:** `docs/superpowers/specs/2026-09-01-zookeeper-provider-watch-design.md`

## Global Constraints

- Keep the existing `/Service/Method/providers/provider-XXXXXXXXXX -> ip:port` registration layout.
- Keep the three-second cache TTL and endpoint failure invalidation as independent fallbacks.
- ZooKeeper callbacks must not perform ZooKeeper reads or acquire `ChannelCore::discovery_mutex_`.
- Use weak ownership across ZooKeeper and Muduo callbacks; no raw `ChannelCore*` may escape.
- Preserve C++17 and existing REQUEST/RESPONSE protocol compatibility.
- Do not add persistent watches, background refresh threads, or a process-wide discovery cache.

---

### Task 1: Add Status-Aware ZooKeeper Child Watches

**Files:**
- Modify: `src/include/zookeeperutil.h`
- Modify: `src/zookeeperutil.cc`
- Create: `test/integration/zk_child_watch_test.cc`
- Modify: `test/CMakeLists.txt`

**Interfaces:**
- Produces: `ZkChildrenResult GetChildrenAndWatch(const std::string&)`
- Produces: `ZkDataResult GetDataResult(const std::string&)`
- Produces: `SetChildrenChangedCallback(ChildrenChangedCallback)`
- Produces: `SetSessionStateCallback(SessionStateCallback)`

- [ ] **Step 1: Write the failing real-ZooKeeper child-watch test**

Create a test that starts two `ZkClient` sessions, watches an empty unique providers path, creates one ephemeral sequential child, waits at most one second for the exact path, re-arms the watch, creates a second child, and requires a second callback.

- [ ] **Step 2: Build to verify the new API is missing**

Run: `cmake --build build -j2 --target zk_child_watch_test`

Expected: compilation fails because callback setters and `GetChildrenAndWatch()` do not exist.

- [ ] **Step 3: Implement result types and callback storage**

Add:

```cpp
struct ZkChildrenResult {
    int code = ZSYSTEMERROR;
    std::vector<std::string> children;
    bool Ok() const noexcept { return code == ZOK; }
};

struct ZkDataResult {
    int code = ZSYSTEMERROR;
    std::string data;
    bool Ok() const noexcept { return code == ZOK; }
};

enum class ZkSessionState { Connected, Disconnected, Expired };
```

Store callbacks under `mutex_`, copy them while locked, and invoke them after unlocking. Clear callbacks before `zookeeper_close()`.

- [ ] **Step 4: Implement watched and status-aware reads**

Use `zoo_wget_children(handle, path, &ZkClient::ChildWatcher, this, &values)` and return the raw ZooKeeper code. Keep existing `GetChildren()` and `GetData()` as compatibility wrappers.

- [ ] **Step 5: Run the real watcher test**

Run: `test/integration/run_zookeeper_watch.sh --zk-only`

Expected: two callbacks arrive on the exact providers path and the test prints `PASS`.

- [ ] **Step 6: Commit**

```bash
git add src/include/zookeeperutil.h src/zookeeperutil.cc test/CMakeLists.txt test/integration/zk_child_watch_test.cc test/integration/run_zookeeper_watch.sh
git commit -m "feat: add ZooKeeper provider child watches"
```

### Task 2: Add Discovery Metrics

**Files:**
- Modify: `src/include/rpcmetrics.h`
- Modify: `src/rpcmetrics.cc`
- Modify: `test/rpcmetrics_test.cpp`

**Interfaces:**
- Produces: `RpcMetricEvent::{DiscoveryWatchEvent, DiscoveryRefresh, DiscoveryRefreshError}`
- Produces snapshot fields `discovery_watch_event`, `discovery_refresh`, and `discovery_refresh_error`

- [ ] **Step 1: Extend the metrics test first**

Increment all three discovery events for `FriendServiceRpc/GetFriendList`, then assert total and method snapshots contain the same amounts and formatted output contains each key.

- [ ] **Step 2: Run the focused test and verify RED**

Run: `cmake --build build -j2 --target rpcmetrics_test`

Expected: compilation fails on the missing enum members and fields.

- [ ] **Step 3: Implement counters, snapshots, and formatting**

Add the three atomics to `Counters`, wire every enum into the exhaustive switch, load them in `SnapshotCounters()`, and append them in `AppendValues()`.

- [ ] **Step 4: Run the focused test and verify GREEN**

Run: `build/bin/rpcmetrics_test`

Expected: `PASS` with no assertion failures.

- [ ] **Step 5: Commit**

```bash
git add src/include/rpcmetrics.h src/rpcmetrics.cc test/rpcmetrics_test.cpp
git commit -m "feat: add service discovery metrics"
```

### Task 3: Invalidate ChannelCore Discovery Cache From Watches

**Files:**
- Modify: `src/include/channelcore.h`
- Modify: `src/channelcore.cc`

**Interfaces:**
- Consumes: Task 1 callbacks and result types
- Consumes: Task 2 discovery metric events
- Produces: `QueueProviderCacheInvalidation(const std::string&)`
- Produces: `QueueSessionCacheInvalidation(ZkSessionState)`

- [ ] **Step 1: Make stale state observable through the E2E test build**

Add the dynamic discovery executable from Task 4 to CMake before changing production behavior. It must fail its runtime expectation with the old TTL-only implementation.

- [ ] **Step 2: Add `stale` to each cache entry**

Cache hits require a connected ZooKeeper session, `stale == false`, a future expiry, and at least one endpoint.

- [ ] **Step 3: Install callbacks once when creating `ZkClient`**

Capture `weak_from_this()`. The ZooKeeper callback calls `loop_->queueInLoop()`, whose task locks the weak pointer again, checks `shutting_down_`, then marks one method or all methods stale.

- [ ] **Step 4: Replace discovery reads with watched, status-aware reads**

Map successful empty lists and `ZNONODE` to `SERVICE_NOT_FOUND`; map session/connection failures to `NETWORK_ERROR`; skip children that disappear with `ZNONODE`; report malformed successful data as `INVALID_ADDRESS` only when no valid endpoint remains.

- [ ] **Step 5: Record discovery metrics**

Count valid watch invalidations, successful watched refreshes, and failed refreshes using the existing bounded `service/method` label.

- [ ] **Step 6: Build and run unit regressions**

Run: `cmake --build build -j2 && ctest --test-dir build --output-on-failure`

Expected: all CTest tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/include/channelcore.h src/channelcore.cc
git commit -m "feat: invalidate discovery cache from ZooKeeper watches"
```

### Task 4: Prove Dynamic Discovery And Watch Re-Registration

**Files:**
- Create: `test/integration/discovery_provider.cc`
- Create: `test/integration/discovery_watch_test.cc`
- Create: `test/integration/run_zookeeper_watch.sh`
- Modify: `test/CMakeLists.txt`

**Interfaces:**
- Consumes: a persistent `MprpcChannel` and `GetMetricsSnapshot()`
- Produces: one script covering raw watch delivery and full RPC membership changes

- [ ] **Step 1: Create an identity Provider fixture**

Implement `FriendServiceRpc::GetFriendList()` so its response contains its configured listening port. The same binary can therefore represent Provider A and B.

- [ ] **Step 2: Create a persistent-channel client fixture**

Warm Provider A, coordinate phases through marker files, observe B before the three-second TTL, converge to A after B stops, then observe restarted B again. Require at least three watch events, successful refreshes, and zero active calls.

- [ ] **Step 3: Write the orchestration script**

Start or reuse local ZooKeeper, clean `/FriendServiceRpc`, start A on `18891`, start the client, add B on `18892`, remove B, restart B, and enforce per-phase timeouts. Always terminate child processes in `trap cleanup EXIT`.

- [ ] **Step 4: Run the E2E test**

Run: `test/integration/run_zookeeper_watch.sh`

Expected: raw watch test and dynamic discovery test both print `PASS`; B is selected before TTL expiry on both registrations.

- [ ] **Step 5: Run lifetime verification under sanitizers**

Run: `cmake --build build-asan -j2 && ctest --test-dir build-asan --output-on-failure`

Expected: all tests pass with no ASan/UBSan reports.

- [ ] **Step 6: Commit**

```bash
git add test/CMakeLists.txt test/integration/discovery_provider.cc test/integration/discovery_watch_test.cc test/integration/run_zookeeper_watch.sh
git commit -m "test: cover dynamic ZooKeeper provider discovery"
```

### Task 5: Document And Regress The Feature

**Files:**
- Modify: `README.md`
- Modify: `docs/interview_questions.md` if present, otherwise the existing project interview document

**Interfaces:**
- Consumes: completed implementation and measured test behavior
- Produces: build/run instructions and an interview-ready explanation of one-shot watch + TTL + TCP fallback

- [ ] **Step 1: Document the discovery correction loop**

Add the Provider znode layout, one-shot re-registration rule, callback-thread handoff, cache invalidation behavior, three fallbacks, and exact integration command.

- [ ] **Step 2: Run full regression**

Run:

```bash
ctest --test-dir build --output-on-failure
test/integration/run_rpc_reliability.sh
test/integration/run_file_transfer_e2e.sh
test/integration/run_zookeeper_watch.sh
```

Expected: every command exits zero.

- [ ] **Step 3: Verify the diff and commit**

```bash
git diff --check
git status --short
git add README.md docs test src
git commit -m "docs: explain dynamic provider discovery"
```
