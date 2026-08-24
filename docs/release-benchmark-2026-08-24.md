# RPC Release Benchmark: 2026-08-24

## Environment

- Source baseline: `d0cc21687533953c1732dab4f83d96fc0ba9fdfb` plus the M11-M13 changes in this delivery.
- Build: GCC 13.3.0, C++17, `CMAKE_BUILD_TYPE=Release`.
- Host: WSL2, Linux 6.6.87.2, Intel Core i5-10505, 12 logical CPUs.
- Transport: localhost TCP through the real Muduo RPC, ZooKeeper discovery and file Provider.
- Storage: WSL temporary directory on the local filesystem.
- Connection mode: warm; each worker reuses its uploader and long connection.
- Warmup: 5 uploads per configuration, excluded from reported samples.

Run a profile with:

```bash
./test/integration/run_release_benchmark.sh latency
./test/integration/run_release_benchmark.sh window
./test/integration/run_release_benchmark.sh saturation
```

Each run writes raw RPC results, client and Provider `/proc` samples, and logs under
`artifacts/release-benchmark-<profile>/`. Generated upload data is removed only after a
successful run; failed runs retain their temporary directory for diagnosis.

## Results

The latency profile used 1 MiB files, window 8 and 100 successful measured uploads per
concurrency level. Throughput rose from 30.44 MiB/s at concurrency 1 to 64.22 MiB/s at
concurrency 8, while P99 rose from 65.54 to 628.45 ms. The throughput gain therefore came
with substantially worse tail latency and higher memory usage.

The window profile used 16 MiB files, concurrency 1 and 10 measured uploads per window.
Throughput rose from 43.46 MiB/s at window 1 to 52.07 MiB/s at window 8. Window 16 fell
to 49.47 MiB/s, reached an actual maximum of 14 in-flight chunks, and increased peak RSS.
Window 8 is therefore a reasonable default for this machine, not a universal constant.
P95/P99 are retained in the raw file but marked invalid because ten samples are too few
for a meaningful tail-latency claim.

The saturation profile used 64 MiB files and window 8. Concurrency 1/2/4 completed all
five measured uploads. At concurrency 8, one of five uploads was rejected with
`SERVER_BUSY` for two of five uploads; this is expected load shedding from the bounded service queue, not a
transport failure. The client stayed responsive and reported the rejection explicitly.

## Raw Data

- [Latency CSV](benchmark-release-latency-2026-08-24.csv)
- [Window CSV](benchmark-release-window-2026-08-24.csv)
- [Saturation CSV](benchmark-release-saturation-2026-08-24.csv)

These localhost WSL2 results demonstrate trends and validate instrumentation. They do
not represent production network, storage or multi-host performance.
