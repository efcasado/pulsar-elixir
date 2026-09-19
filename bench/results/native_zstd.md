# Native zstd: preliminary comparison

Measured on 2026-09-19 in a shared Linux container: AMD EPYC 9V74, 8 CPU quota,
4 BEAM schedulers (`ERL_FLAGS="+S 4:4"`), Elixir 1.19.5, Erlang/OTP 28.0.
Baseline: `eb21fbe198908009cf5486e966a97e384a1c8906`, ezstd 1.2.4.
Candidate: this change, using native zstd at the same compression level **1**.

Both revisions ran the same harness sequentially. Each scenario used 0.2 s
warmup, 0.5 s timing, 0.1 s memory, and 0.1 s reduction sampling, at parallelism 1.
Inputs were 1 KiB and 1 MiB total application data, single messages and batches
of 100, with both text and distinct high-entropy messages. All preflight payload
and message-count checks passed.

These short runs validate the comparison workflow and provide an initial signal;
they are not production throughput claims. Repeat the default, longer runs on
deployment hardware before drawing release-level conclusions. Memory covers BEAM
allocation, not all native allocations. NIF reduction accounting differs between
implementations, so reductions are not a direct CPU-cost comparison.

## Median time per entry

Negative changes mean lower latency. A batch operation contains 100 messages.
Batch labels show the exact per-message size after rounding down.

### Producer

| Payload | ezstd (µs) | Native (µs) | Change |
| --- | ---: | ---: | ---: |
| entropy/1 x 1024B | 22.35 | 32.47 | +45.2% |
| entropy/1 x 1048576B | 633.61 | 350.30 | -44.7% |
| entropy/100 x 10485B | 617.84 | 587.20 | -5.0% |
| entropy/100 x 10B | 167.98 | 168.44 | +0.3% |
| text/1 x 1024B | 23.82 | 24.81 | +4.2% |
| text/1 x 1048576B | 174.52 | 169.64 | -2.8% |
| text/100 x 10485B | 425.90 | 399.64 | -6.2% |
| text/100 x 10B | 162.51 | 165.29 | +1.7% |

### Consumer

| Payload | ezstd (µs) | Native (µs) | Change |
| --- | ---: | ---: | ---: |
| entropy/1 x 1024B | 9.44 | 9.36 | -0.8% |
| entropy/1 x 1048576B | 81.37 | 91.61 | +12.6% |
| entropy/100 x 10485B | 190.53 | 176.79 | -7.2% |
| entropy/100 x 10B | 87.44 | 78.90 | -9.8% |
| text/1 x 1024B | 10.71 | 10.67 | -0.4% |
| text/1 x 1048576B | 144.58 | 151.53 | +4.8% |
| text/100 x 10485B | 196.83 | 171.82 | -12.7% |
| text/100 x 10B | 88.61 | 79.56 | -10.2% |

## Controls and detailed measurements

Uncompressed control median changes ranged from -8.5% to +9.6%;
use this variation when interpreting small differences.

Compressed payload byte counts matched on every measured input.

[Detailed CSV](native_zstd.csv) includes every ZSTD and uncompressed scenario,
mean and median latency (nanoseconds), relative standard deviation, mean BEAM
memory bytes, mean reductions, and compressed/uncompressed byte counts.

To reproduce, use the baseline setup in [the benchmark guide](../README.md),
then run the same settings on both revisions:

```sh
ERL_FLAGS="+S 4:4" BENCH_SIZES=1024,1048576 BENCH_BATCHES=1,100 \
BENCH_WARMUP=0.2 BENCH_TIME=0.5 BENCH_MEMORY_TIME=0.1 BENCH_REDUCTION_TIME=0.1 \
mix run bench/compression.exs
```
