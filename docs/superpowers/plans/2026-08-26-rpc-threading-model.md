# RPC Threading Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Share client Reactor threads, isolate Provider business work, and make asynchronous callback delivery safe under overload.

**Architecture:** A process-level `RpcClientRuntime` owns round-robin I/O loops and, in the final stage, a reservation-based callback executor. Each `ChannelCore` remains pinned to one loop. `RpcProvider` uses a bounded worker pool and an atomic dispatch state distinct from response completion.

**Tech Stack:** C++17, Muduo, Protobuf, `BoundedExecutor`, CMake/CTest.

**Spec:** `docs/superpowers/specs/2026-08-26-rpc-threading-model-design.md`

## Global Constraints

- Preserve current synchronous and asynchronous Protobuf `RpcChannel` APIs.
- Never block a Muduo I/O loop while waiting for queue capacity.
- Keep FileTransfer's dedicated bounded executor.
- Keep every `ChannelCore` pinned to one EventLoop for its lifetime.
- Preserve timeout, cancellation, disconnect, and exactly-once response semantics.

---

### Task 1: Shared Client I/O Runtime

**Files:**
- Create: `src/include/rpcclientruntime.h`
- Create: `src/rpcclientruntime.cc`
- Create: `test/rpcclientruntime_test.cpp`
- Modify: `src/include/channelcore.h`
- Modify: `src/channelcore.cc`
- Modify: `src/include/mprpcchannel.h`
- Modify: `src/mprpcchannel.cc`
- Modify: `test/CMakeLists.txt`

**Interfaces:**
- Produces: `RpcClientRuntime::Default()`, `NextLoop()`, `IoThreadCount()`.
- Consumes: `muduo::net::EventLoopThread` and existing `ChannelCore` loop confinement.

- [ ] Add a test that requests five loops from a two-loop runtime and expects `0,1,0,1,0` pointer assignment.
- [ ] Run the test and verify compilation fails because `RpcClientRuntime` does not exist.
- [ ] Implement runtime-owned loop threads and round-robin selection.
- [ ] Inject the runtime into `ChannelCore` and remove its owned `EventLoopThread`.
- [ ] Verify runtime unit tests and RPC reliability integration.
- [ ] Commit as `refactor: share client RPC event loops`.

### Task 2: Provider Business Executor

**Files:**
- Create: `src/include/rpcdispatchstate.h`
- Create: `test/rpcdispatchstate_test.cpp`
- Modify: `src/include/rpcprovider.h`
- Modify: `src/rpcprovider.cc`
- Modify: `test/CMakeLists.txt`
- Modify: `example/callee/friendservice.cc`
- Modify: `test/integration/async_rpc_test.cc`

**Interfaces:**
- Produces: `RpcDispatchState::TryStart()`, `CancelQueued()`, and `Finish()`.
- Consumes: `BoundedExecutor::TrySubmit()` and existing `RpcResponseContext` completion.

- [ ] Add state tests proving cancel wins before start and cannot undo running work.
- [ ] Run the tests and verify the new dispatch type is missing.
- [ ] Add the bounded Provider executor and submit service invocation tasks.
- [ ] Register deadline before submission; skip tasks whose queued state was cancelled.
- [ ] Return framework `SERVER_BUSY` on nonblocking queue rejection.
- [ ] Add integration coverage for queued deadline/cancel and blocking-business Reactor isolation.
- [ ] Run CTest and RPC reliability integration.
- [ ] Commit as `feat: dispatch RPC business work off IO loops`.

### Task 3: Reserved Shared Callback Executor

**Files:**
- Modify: `src/include/boundedexecutor.h`
- Modify: `src/boundedexecutor.cc`
- Create: `test/boundedexecutor_test.cpp`
- Modify: `src/include/rpcclientruntime.h`
- Modify: `src/rpcclientruntime.cc`
- Modify: `src/include/channelcore.h`
- Modify: `src/channelcore.cc`
- Modify: `test/CMakeLists.txt`

**Interfaces:**
- Produces: move-only `BoundedExecutor::Reservation`, `TryReserve()`, and `SubmitReserved()`.
- Consumes: shared callback executor from `RpcClientRuntime`.

- [ ] Add tests proving a reservation consumes capacity and guarantees one later submission.
- [ ] Run tests and verify reservation APIs are absent.
- [ ] Implement RAII reservations in `BoundedExecutor`.
- [ ] Move callback executor ownership into `RpcClientRuntime`.
- [ ] Reserve callback capacity before async network admission and fail locally when full.
- [ ] Remove direct callback execution fallback from `ChannelCore::CompleteCall()`.
- [ ] Run unit and RPC reliability tests.
- [ ] Commit as `refactor: share reserved RPC callback executor`.

### Task 4: Full Verification And Documentation

**Files:**
- Modify: `README.md`
- Modify: `docs/interview-guide.md`

- [ ] Document client loop sharing, Provider dispatch state, and two-level file execution.
- [ ] Run Debug build and all CTest tests.
- [ ] Run RPC reliability and FileTransfer E2E scripts.
- [ ] Run ASan/UBSan build and CTest suite.
- [ ] Run Release latency smoke and inspect thread/resource metrics.
- [ ] Verify `git diff --check` and record final commits without pushing.
