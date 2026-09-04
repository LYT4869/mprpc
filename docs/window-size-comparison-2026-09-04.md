# Sliding-Window File-Size Comparison

This supplemental Release experiment checks whether the window-size trend from
the 16 MiB baseline remains visible during longer 256 MiB transfers.

## Environment And Method

- Commit: `3a2c3d2178a073ef9cdfdfdb9ac6e7376f4290da`.
- Environment: WSL2 loopback, Intel Core i5-10505, 12 logical CPUs, GCC 13.3.0.
- Build: Release.
- Connection mode: warm, concurrency 1.
- File size: 256 MiB.
- Windows: 1, 2, 4, 8, and 16.
- Per setting: one warmup per uploader and five measured uploads.
- Total measured data: 6.25 GiB; including warmup: 7.5 GiB.

Five samples are useful for a coarse exploratory comparison, but not for a
statistically meaningful P95/P99 claim or a stable throughput ranking. The
settings ran once in ascending window order rather than in randomized rounds.

## Results

| Window | Success | Goodput MiB/s | Max in flight |
|---:|---:|---:|---:|
| 1 | 5/5 | 40.05 | 1 |
| 2 | 5/5 | 43.32 | 2 |
| 4 | 5/5 | 57.32 | 4 |
| 8 | 5/5 | 66.56 | 8 |
| 16 | 5/5 | 70.92 | 16 |

All configurations completed without retries, queue rejection, network errors,
or unfinished RPC calls. Across the complete multi-configuration process,
client peak RSS was 53,216 KiB, Provider peak RSS was 79,872 KiB, and Provider
CPU time was 43.28 seconds. Because `ru_maxrss` is a process-lifetime high-water
mark and settings ran sequentially, the per-row RSS values cannot isolate each
window's memory cost.

Raw measurements: [256 MiB window CSV](benchmark-release-window-256mib-2026-09-04.csv).

## Comparison With 16 MiB

| Window | 16 MiB goodput | 256 MiB goodput |
|---:|---:|---:|
| 1 | 44.47 MiB/s | 40.05 MiB/s |
| 2 | 54.51 MiB/s | 43.32 MiB/s |
| 4 | 52.29 MiB/s | 57.32 MiB/s |
| 8 | 61.14 MiB/s | 66.56 MiB/s |
| 16 | 59.71 MiB/s | 70.92 MiB/s |

The 16 MiB run peaked at window 8. In this single 256 MiB run, measured goodput
continued improving through window 16, whose aggregate result was 6.6% above
window 8.

This does not establish that window 16 is generally faster: the difference may
include run order, page-cache, storage, or normal WSL variance. A plausible
hypothesis is that longer transfers give the chunk pipeline more time to remain
full, but proving it requires repeated randomized rounds and per-configuration
resource isolation. The defensible engineering conclusion is only to keep the
window configurable and avoid treating the 16 MiB optimum as universal; this
exploratory run alone is not a reason to change the default from 8.
