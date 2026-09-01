# ZooKeeper Provider Watch Design

## Goal

Add event-driven Provider membership monitoring to the existing RPC service
discovery path without replacing its TTL fallback or TCP failure feedback.
Provider child changes invalidate the affected method cache, and the next RPC
refreshes the latest membership while re-registering the one-shot watch.

## Current State

`ZkClient::GlobalWatcher` only handles `ZOO_SESSION_EVENT`. Service discovery
uses `zoo_get_children(..., 0, ...)`, so a `ChannelCore` learns about Provider
membership changes only when its three-second cache TTL expires or an existing
TCP connection fails.

Each `ChannelCore` owns its ZooKeeper client, endpoint cache, round-robin index,
and affinity routing state. This design keeps that ownership model. Sharing a
ZooKeeper session and resolver through `RpcClientRuntime` is intentionally left
as a future optimization.

## Discovery Model

The final discovery model has three independent correction mechanisms:

```text
Child Watch       Provider membership notification -> mark method cache stale
TTL fallback      Three-second periodic correction -> refresh stale data
TCP failure       Data-plane feedback              -> remove failed endpoint
```

A watch event does not perform a ZooKeeper read. It only schedules cache
invalidation. The next call to `GetEndpoint()` performs a watched read and
rebuilds the complete Provider list. This is event-driven lazy refresh, not a
background subscription that continuously materializes every intermediate
membership state.

## ZkClient Interface

`ZkClient` gains explicit result types so callers can distinguish an empty
Provider set from a registry failure:

```cpp
struct ZkChildrenResult
{
    int code = ZSYSTEMERROR;
    std::vector<std::string> children;

    bool Ok() const noexcept { return code == ZOK; }
};

struct ZkDataResult
{
    int code = ZSYSTEMERROR;
    std::string data;

    bool Ok() const noexcept { return code == ZOK; }
};
```

The new watched operation is:

```cpp
using ChildrenChangedCallback =
    std::function<void(const std::string& providers_path)>;

void SetChildrenChangedCallback(ChildrenChangedCallback callback);
ZkChildrenResult GetChildrenAndWatch(const std::string& path);
ZkDataResult GetDataResult(const std::string& path);
```

`GetChildrenAndWatch()` uses `zoo_wget_children()` so a successful operation
both reads the current children and registers `ChildWatcher` without a separate
application-level read/register gap. The existing non-watched methods remain
available for Provider registration compatibility.

All watched paths use `ZkClient*` as the C callback context. `ZkClient` stores
one thread-safe callback that receives the path supplied by ZooKeeper, avoiding
per-watch heap contexts and their cancellation lifetime.

`ChildWatcher` accepts `ZOO_CHILD_EVENT` and `ZOO_DELETED_EVENT`. It ignores
session events because `GlobalWatcher` remains responsible for session state.
The callback is copied while holding the ZkClient mutex and invoked after
releasing that mutex.

## Threading And Lifetime

ZooKeeper invokes its watcher on a ZooKeeper callback thread. That thread must
not acquire `ChannelCore::discovery_mutex_`, perform a synchronous ZooKeeper
operation, or touch a raw `ChannelCore*`.

When the Core creates its `ZkClient`, it installs a callback that captures a
`std::weak_ptr<ChannelCore>`:

```text
ZooKeeper callback thread
  -> weak_ptr.lock()
  -> EventLoop::queueInLoop()
  -> lock discovery_mutex_
  -> mark one method cache stale
```

`queueInLoop()`, rather than `runInLoop()`, is required so invalidation never
runs inline in the ZooKeeper callback. If the Core has been destroyed, the weak
pointer fails and the event is discarded safely. Holding the Core also keeps
its selected EventLoop and `RpcClientRuntime` alive until the queued handoff is
installed.

`GetEndpoint()` already holds `discovery_mutex_` across discovery and cache
replacement. A queued invalidation therefore runs either before a refresh or
after the new cache entry is inserted; an event cannot be lost between the
watched read and cache publication.

During shutdown, `ChannelCore` first sets `shutting_down_`. Watch callbacks and
queued invalidations check this flag. `ZkClient` clears its stored callback
before `zookeeper_close()`.

## Cache State And Refresh

`EndpointCacheEntry` gains:

```cpp
bool stale = false;
```

A cache hit requires all of the following:

```text
stale == false
expires_at > now
endpoints is not empty
ZooKeeper session is connected
```

The child callback derives the method path by removing the exact `/providers`
suffix and marks that entry stale. Unknown or malformed paths are ignored.

On cache miss, TTL expiry, or stale state, `GetEndpoint()`:

1. Starts or reconnects the ZooKeeper session.
2. Calls `GetChildrenAndWatch(providers_path)`.
3. Reads each child address with status-aware `GetDataResult()`.
4. Parses, sorts, and deduplicates endpoints as today.
5. Replaces the cache with `stale = false` and a new three-second expiry.
6. Applies round-robin or affinity selection.

If a child disappears between the children read and data read, `ZNONODE` is
treated as a transient membership race and skipped rather than reported as an
invalid address. Other malformed non-empty child data still produces
`INVALID_ADDRESS` when no valid Provider remains.

If the watched children read fails, no cache entry is published. Connection
errors map to `NETWORK_ERROR`; `ZNONODE` or a successful empty child list maps
to `SERVICE_NOT_FOUND`.

## Session Events

`GlobalWatcher` continues to maintain `connected_` and `expired_`. It also
publishes a lightweight session-state callback after releasing the ZkClient
mutex. `ChannelCore` queues this notification to its EventLoop and marks all
endpoint caches stale on disconnect, expiration, and reconnection.

This conservative rule handles a transient disconnect that begins and ends
between two RPCs: the next call cannot reuse a cache whose watch status is
uncertain. If the session expired, `Start()` closes the old handle and creates
a new one; the stale cache forces watched reads to re-establish all watches
lazily.

## Provider Contract

Provider registration remains unchanged:

```text
/Service/Method/providers/provider-XXXXXXXXXX -> ip:port
```

The Provider znode is ephemeral and sequential. Its address data is immutable
for the lifetime of that node. Address changes must delete the old node and
create a new one because a child watch detects child creation/deletion, not a
`setData()` on an existing child.

## Observability

Client metrics gain three counters:

```text
discovery_watch_event
discovery_refresh
discovery_refresh_error
```

Watch events are counted when a valid providers path is invalidated. A refresh
is counted after a watched children read succeeds; read/session failures count
as refresh errors. Method labels use the existing bounded service/method path,
never Provider address or request ID.

## Testing

### ZkClient Integration

Use a real local ZooKeeper process:

1. Read an empty providers path with a child watch.
2. Create an ephemeral child and require one callback within one second.
3. Read again to re-register the watch.
4. Create or delete another child and require a second callback.
5. Verify the callback carries the exact providers path.

This proves one-shot delivery and explicit re-registration.

### Dynamic Discovery E2E

Use two test Providers that return distinct identities:

1. Start Provider A and issue one RPC through a persistent Channel to cache A.
2. Start Provider B while A remains healthy.
3. Before the three-second TTL expires, issue two RPCs and require one response
   from B.
4. Stop B and require subsequent calls to converge on A.
5. Start B again and repeat the check, proving the watch was re-registered.
6. Assert watch, refresh, and refresh-error metrics have expected values and
   active calls return to zero.

### Lifetime And Regression

- Destroy a watched Channel while a child event is delivered; ASan must report
  no use-after-free.
- Run existing RPC reliability, FileTransfer E2E, Debug CTest, and ASan/UBSan
  CTest suites.
- Keep the existing three-second TTL behavior and connection-failure tests.

## Non-Goals

- No persistent/recursive ZooKeeper watch API.
- No background refresh thread or eager refresh inside a watcher.
- No process-wide shared service discovery cache.
- No consistent-hash ring or automatic retry of an already dispatched RPC on
  a newly discovered Provider.
- No guarantee of observing every intermediate Provider membership state.

## Success Criteria

- A newly registered Provider becomes selectable on the next RPC before the
  existing TTL expires.
- Provider deletion invalidates the affected method cache without clearing
  unrelated method caches.
- Every refresh installs the next one-shot watch.
- Session loss cannot leave an apparently fresh un-watched cache.
- Watch callbacks never run ZooKeeper reads or user RPC callbacks.
- Existing TTL, affinity routing, TCP invalidation, and all regression tests
  continue to pass.
