# RPC Threading Model Design

## Goal

Bound client thread growth, keep user business code off Muduo I/O loops, and
guarantee that accepted asynchronous completions never fall back to an I/O
thread when callback capacity is exhausted.

## Client Runtime

`RpcClientRuntime` owns a configurable number of `EventLoopThread` objects.
Each `ChannelCore` obtains one loop by round-robin assignment and remains
pinned to that loop for its lifetime. A loop may serve many cores, while each
core continues to confine its endpoint sessions to its assigned loop.

The runtime outlives its cores. Core shutdown removes only that core's timers,
connections, sessions, and pending calls; it never stops a shared loop.
The default runtime uses two I/O loops and is shared process-wide. Tests may
inject a dedicated runtime.

## Provider Dispatch

Muduo I/O loops decode frames, validate routing metadata, create active-call
state, register deadlines, and submit work without blocking. A bounded
Provider executor invokes `service->CallMethod()` on worker threads. Queue
rejection completes the call with framework `SERVER_BUSY`.

Each server call has a dispatch phase:

```text
Queued -> Running -> Finished
   |
   +-----> Cancelled
```

The worker must win `Queued -> Running` before invoking business code. A
deadline, client cancellation, or disconnect that wins `Queued -> Cancelled`
prevents business execution. Cancellation after `Running` is cooperative via
`MprpcController`; response completion remains independently guarded by the
existing one-shot response state.

The FileTransfer executor remains in place. The Provider pool protects the
Reactor from arbitrary service entry code; the file pool limits disk, hash,
memory, and file-specific queue pressure.

## Callback Dispatch

The client runtime owns a shared fixed-size callback executor. An asynchronous
call reserves one completion slot before network admission. Accepted calls
therefore have guaranteed callback queue capacity when they complete. If no
slot is available, the call fails immediately with `CALLBACK_REJECTED` before
sending a request. Synchronous calls do not reserve callback capacity.

Core shutdown dispatches all accepted completions before releasing the
runtime. Runtime shutdown drains callbacks after all cores have released it.
No completion path executes a user callback as an I/O-thread fallback.

## Defaults And Tests

- Client I/O loops: 2, configurable by explicit runtime construction.
- Shared callback workers: 2; maximum reserved/queued/running callbacks: 1024.
- Provider business workers: 4; maximum outstanding tasks: 64.
- Provider I/O loops: 4.
- All queues reject without blocking I/O threads.
- Tests cover round-robin loop assignment, independent core shutdown,
  queued timeout/cancel, Provider overload, Reactor isolation, callback
  capacity, exactly-once completion, FileTransfer regression, and sanitizers.
