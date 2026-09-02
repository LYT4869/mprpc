# RPC Project Test Baseline: 2026-09-02

This report is the authoritative test baseline for commit
`04cf057f72ec1032d8bc3c103a6215c1546cd296`. All correctness tests,
integration tests, Release benchmarks, and large-file checks below were run
again on that commit. Older reports remain historical comparisons.

## Environment

- OS: WSL2, Linux `6.6.87.2-microsoft-standard-WSL2`, x86-64.
- CPU: Intel Core i5-10505 at 3.20 GHz, 12 logical CPUs.
- Memory: 15.5 GiB available to WSL.
- Compiler: GCC `13.3.0`.
- CMake: `3.28.3`.
- Test filesystem: ext4 under WSL `/tmp`.
- Network: local loopback; ZooKeeper and Providers ran on the same machine.
- Debug, sanitizer, and Release outputs used separate build directories.

These values describe one development machine. They are useful for regression
and design comparison, not as production capacity claims.

## Result Summary

| Test group | Build | Result | Main behavior covered |
|---|---:|---:|---|
| CTest | Debug | 12/12 PASS | Codec, metrics, runtime, dispatch state, bounded executors, callback admission, file sessions and argument validation |
| CTest | ASan/UBSan | 12/12 PASS | The same unit paths under memory and undefined-behavior instrumentation |
| RPC reliability | Debug + ASan/UBSan | PASS | Async return, timeout/late-response race, cancellation, concurrency, shutdown, deadline and overload |
| File transfer E2E | Debug + ASan/UBSan | PASS | Two Providers, random writes, restart recovery, retry, overload and final hash |
| ZooKeeper Child Watch | Debug + ASan/UBSan | PASS | One-shot watch re-registration and membership refresh before TTL expiry |
| Provider registration recovery | Debug + ASan/UBSan | PASS | Recoverable disconnect, Session Expired, re-registration and fresh-client RPC |
| Release latency | Release | PASS | 300 measured 1 MiB uploads across concurrency 1/4/8 |
| Release window | Release | PASS | Window 1/2/4/8/16 with ten 16 MiB uploads per setting |
| Release saturation | Release | PASS | Concurrency 1/2/4/8 with overload rejection allowed and classified |
| Large-file E2E | Release | PASS | 256 MiB and 1 GiB uploads with external size and SHA-256 comparison |

## Correctness And Reliability

The Debug RPC reliability run reported:

- `CallMethod` returned after 84 ms while the asynchronous callback completed
  later.
- Ten concurrent asynchronous calls completed successfully.
- Timeout, explicit cancellation, and a late response each completed their
  call state once.
- Two connections could use the same request ID without colliding at the
  Provider.
- Channel shutdown completed pending calls, while shutting down one Channel did
  not stop the shared process-level client runtime.
- Provider deadlines, bounded business dispatch, and Reactor isolation passed.
- Client metrics ended with `started=14`, `active=0`, `success=11`,
  `timeout=1`, and `cancelled=1` for the exercised scenario.

The file-transfer E2E run reported `accepted=2`, `rejected=127`,
`completed=2`, and `peak_outstanding=1` in its deliberate overload case. It
also completed normal upload, manual sidecar recovery, Provider restart during
an upload, retry, duplicate-chunk handling, and final SHA-256 comparison.

The ZooKeeper tests independently verified both sides of discovery lifecycle:

1. A client re-registers the one-shot child watch and observes Provider add,
   remove, and restart before the three-second cache TTL expires.
2. A Provider keeps its original ephemeral node across a recoverable
   disconnect, then creates a new Session and re-registers after Session
   Expired without restarting the Provider process.

The four integration suites were repeated with sanitizer binaries and produced
the same PASS outcomes. No AddressSanitizer or UndefinedBehaviorSanitizer
diagnostic was emitted.

## Release Latency

The latency profile used a 1 MiB file, window 8, warm connections, three
warmups per uploader, and 100 measured uploads per concurrency setting.

| Concurrency | Success | Failed | Goodput MiB/s | P50 ms | P95 ms | P99 ms | Client peak RSS KiB |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 100 | 0 | 28.27 | 33.03 | 47.37 | 67.27 | 19,812 |
| 4 | 100 | 0 | 66.95 | 58.89 | 74.19 | 74.42 | 35,908 |
| 8 | 100 | 0 | 72.95 | 102.97 | 136.21 | 193.27 | 62,436 |

Concurrency 8 produced the highest successful goodput in this run, but its P99
was 2.6 times the concurrency-4 value and client peak RSS also increased. The
extra concurrency therefore improved aggregate throughput at a clear tail-
latency and memory cost. This is a measured reason to keep concurrency
configurable instead of assuming that a larger value is always better.

Raw data: [latency CSV](benchmark-release-latency-2026-09-02.csv).

## Sliding Window

The window profile used 16 MiB files, concurrency 1, warm connections, three
warmups, and ten measured uploads per setting.

| Window | Success | Goodput MiB/s | Maximum in flight | Client peak RSS KiB |
|---:|---:|---:|---:|---:|
| 1 | 10/10 | 44.47 | 1 | 19,716 |
| 2 | 10/10 | 54.51 | 2 | 22,836 |
| 4 | 10/10 | 52.29 | 4 | 27,708 |
| 8 | 10/10 | 61.14 | 8 | 30,740 |
| 16 | 10/10 | 59.71 | 14 | 40,692 |

The result is not monotonic, so it does not prove a universal optimum. Window
8 was the best measured setting on this machine. Window 16 used more memory and
did not improve goodput; its observed maximum in-flight count was 14 rather
than the configured ceiling of 16. Ten samples are enough for a coarse
throughput comparison, not a meaningful P95/P99 claim.

Raw data: [window CSV](benchmark-release-window-2026-09-02.csv).

## Saturation And Overload

The saturation profile used 64 MiB files, window 8, warm connections, three
warmups per uploader, and 16 measured uploads per setting. Failure was allowed
because the purpose was to observe bounded overload behavior.

| Concurrency | Success | Failed | Successful goodput MiB/s | Queue rejected | Client peak RSS KiB |
|---:|---:|---:|---:|---:|---:|
| 1 | 16 | 0 | 60.04 | 0 | 33,024 |
| 2 | 16 | 0 | 63.21 | 0 | 58,264 |
| 4 | 16 | 0 | 73.41 | 0 | 129,140 |
| 8 | 2 | 14 | 40.15 | 14 | 249,256 |

Goodput increased through concurrency 4. At concurrency 8, 14 of 16 uploads
were rejected with `SERVER_BUSY`; those failures match the queue-rejection
count and were not misclassified as network errors. The bounded queue therefore
shed load instead of allowing outstanding work and memory use to grow without
limit. The reported goodput counts only fully successful files; it is not the
attempted byte rate. This profile has too few samples for a P95/P99 claim.

Raw data: [saturation CSV](benchmark-release-saturation-2026-09-02.csv).

## Large-File E2E

Both files were generated from zero-filled blocks and uploaded over loopback
with window 8. Elapsed time covers the complete client process, including local
SHA-256 calculation, RPC upload, and waiting for the Provider's final SHA-256
verification. After completion, `sha256sum` independently compared the source
and published server file.

| Size | Bytes | Elapsed | Throughput | Retries | Max in flight | Client peak RSS KiB | Result |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 256 MiB | 268,435,456 | 4.42 s | 57.92 MiB/s | 0 | 8 | 24,376 | PASS |
| 1 GiB | 1,073,741,824 | 19.18 s | 53.39 MiB/s | 0 | 8 | 25,380 | PASS |

The 256 MiB SHA-256 was
`a6d72ac7690f53be6ae46ba88506bd97302a093f7108472bd9efc3cefda06484`.
The 1 GiB SHA-256 was
`49bc20df15e412a64472421e13fe86ff1c5165e18b2afccf160d4dc19fe68a14`.
For both uploads, source and server byte counts and hashes were identical.

These numbers differ substantially from the older 2026-08-28 run, especially
for 1 GiB. Page cache, current system load, generated input, and WSL storage
behavior can all affect a local benchmark. The current result replaces the old
number as the regression baseline, but the difference itself is evidence that
one local run must not be advertised as stable production throughput.

Raw data: [large-file CSV](large-file-e2e-2026-09-02.csv). The generated
[run metadata](large-file-e2e-environment-2026-09-02.md) records its commit,
timestamp, toolchain, kernel, CPU, and filesystem.

## Resource Summary

| Profile | Client peak RSS KiB | Provider peak RSS KiB | Provider CPU seconds |
|---|---:|---:|---:|
| Latency | 62,436 | 62,776 | 3.04 |
| Window | 40,692 | 60,892 | 7.04 |
| Saturation | 249,256 | 128,340 | 42.98 |
| 256 MiB + 1 GiB | 25,380 | 56,384 | 7.02 |

Resource samples came from `/proc/<pid>/stat` and `/proc/<pid>/status` at
approximately 100 ms intervals. They are process-level observations, not a
breakdown by network, hashing, filesystem, or ZooKeeper code.

Raw summary: [resource CSV](benchmark-resource-summary-2026-09-02.csv).

## Commands

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j2
ctest --test-dir build --output-on-failure

cmake -S . -B build-sanitize \
  -DMPRPC_ENABLE_SANITIZERS=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build build-sanitize -j2
ASAN_OPTIONS=detect_leaks=1 \
  ctest --test-dir build-sanitize --output-on-failure

./test/integration/run_rpc_reliability.sh
./test/integration/run_file_transfer_e2e.sh
./test/integration/run_zookeeper_watch.sh
./test/integration/run_provider_reregistration.sh
./test/integration/run_large_file_e2e.sh

./test/integration/run_release_benchmark.sh latency
./test/integration/run_release_benchmark.sh window
./test/integration/run_release_benchmark.sh saturation
```

For sanitizer integration runs, the same four scripts used
`BUILD_DIR=$PWD/build-sanitize` and `ASAN_OPTIONS=detect_leaks=1`.

## Boundaries

- ASan/UBSan does not detect every data race. ThreadSanitizer and sustained
  stress testing remain separate work.
- The benchmark uses loopback and one WSL machine; it does not include real
  network loss, multiple physical hosts, or production storage.
- Only the 100-sample latency profile supports the reported P50/P95/P99 claim.
- The current transfer affinity is `hash(key) % provider_count`, not explicit
  transfer binding. Mid-upload Provider membership changes are not yet covered
  as a sticky-routing guarantee.
- Provider process restart recovery requires the same upload directory. Machine
  loss and cross-node state takeover require shared or replicated durable state
  plus ownership fencing, which this project does not implement.
- Cancellation is cooperative and cannot roll back side effects already
  committed by business code.
