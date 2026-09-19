# Benchmarks

`mix bench` runs the existing protocol stream decoder benchmark.
`mix bench.compression` runs producer and consumer compression benchmarks using
Benchee, without a Pulsar broker. Both implementations use compression level 1
(the ezstd default, explicitly preserved by the native producer). Use OTP 28+ for both baseline and candidate.

## What is measured

Each producer operation sends one message, or fills and publishes a 100-message
batch, through `Pulsar.Producer.Worker`. It includes metadata encoding, compression,
checksumming, frame encoding, and a synchronous handoff to a discard-only broker
stub. Each operation starts with empty producer state; pending receipts never grow.

Each consumer operation feeds one prepared broker delivery through
`Pulsar.Consumer.Worker`, including decompression, uncompressed-size validation,
batch decoding, message construction, and callback dispatch. Payload creation and
compression happen before timing. The callback counts messages and returns
`:noreply`, so acknowledgement traffic and flow replenishment are excluded.

There is no real broker, socket I/O, persistence, producer receipt, schema decoding,
chunk reassembly, or application callback work. These are worker-path benchmarks,
not end-to-end throughput measurements. Producer and consumer numbers describe
different work and should be compared against the same job on the other revision.

Inputs cover 100 B, 1 KiB, 10 KiB, 100 KiB, 1 MiB, and 5 MiB of application data per
operation, split into either 1 or 100 messages (rounded down to whole bytes per
message). Labels state the exact message count and size. Batched metadata adds to
the bytes compressed. Both repeating JSON and deterministic high-entropy bytes
are tested with `:none` and `:zstd`. Entropy payloads differ between messages so
batching does not introduce artificial repetition. The largest input is a local codec stress case;
it does not claim to fit a broker's frame limit after metadata is included.

Before timing, every producer output is decoded and consumed, checking payload
contents and message count. Invalid deliveries abort instead of producing a fast
but meaningless result. Output includes compressed/uncompressed byte counts and
ratios, alongside Benchee's time/throughput, BEAM memory, and reductions. One
iteration is one entry: multiply iterations/sec by the label's message count for
messages/sec. BEAM allocation and reduction counts do not account for all native
Zstandard allocations or CPU work.

A [preliminary comparison](results/native_zstd.md) and its detailed CSV are included.

## Compare ezstd and native zstd

Keep the OTP/Elixir versions, scheduler counts, hardware, and benchmark settings
identical. Run revisions sequentially on an otherwise idle machine. Copy the same
harness onto the base revision; its original `mix.exs` and lock retain ezstd, while
the candidate uses native zstd without adding an optional runtime switch.

From the PR checkout:

```sh
git worktree add --detach ../pulsar-zstd-baseline eb21fbe198908009cf5486e966a97e384a1c8906
cp bench/compression.exs ../pulsar-zstd-baseline/bench/
cp bench/support/compression.ex ../pulsar-zstd-baseline/bench/support/

(cd ../pulsar-zstd-baseline && mix deps.get && \
  BENCH_TAG=ezstd BENCH_SAVE=/tmp/pulsar-ezstd.benchee \
  mix run bench/compression.exs)

mix deps.get
BENCH_TAG=native BENCH_SAVE=/tmp/pulsar-native.benchee \
BENCH_LOAD=/tmp/pulsar-ezstd.benchee mix bench.compression
```

Benchee saves tagged results; `BENCH_LOAD` accepts a saved file or glob to compare
against the current run. Retain
the console output too: it reports runtime details and compression ratios. Record
the actual saved filenames printed by Benchee, which may include the tag.

A short smoke run:

```sh
BENCH_SIZES=100,1024 BENCH_BATCHES=1,100 \
BENCH_WARMUP=0.1 BENCH_TIME=0.2 BENCH_MEMORY_TIME=0 BENCH_REDUCTION_TIME=0 \
mix bench.compression
```

Increase `BENCH_TIME`, `BENCH_WARMUP`, `BENCH_MEMORY_TIME`, and
`BENCH_REDUCTION_TIME` for stable measurements. `BENCH_PARALLEL` defaults to 1;
repeat at the same higher concurrency on both revisions to examine contention.
The shared producer sink adds contention of its own, so this is not a direct
scheduler-latency measurement. Use runtime profiling for scheduler responsiveness
and native allocation analysis; do not infer them from reductions alone.
