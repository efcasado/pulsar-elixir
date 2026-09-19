# Producer iodata prototype

This experiment is stacked on #225. Measurements use its
`78ce07d51e653f4a358a9248641914b4f11d9c3a` revision. The prototype was subsequently
rebased onto `d53f882b64dfce9cd24409f0ce3342b671167913`, which adds consumer validation
and compression-option fixes without changing the timed producer implementation.
It retains native ZSTD output as iodata, computes CRC32C with `nif_iolist/1`, and
passes the resulting frame through the broker to TCP or TLS. Compression levels,
wire bytes, supervision, and public producer APIs are unchanged. Actual chunk
splitting still flattens its input; enabling chunking without needing a split does
not. Batch construction before compression is unchanged.

`Pulsar.Protocol` and `Pulsar.Broker` are internal modules: the message encoder now
returns iodata and the broker accepts it. Command-only encoding still returns a
binary. Code calling the internal message encoder and then decoding or inspecting
its bytes must explicitly flatten the result. The minimum crc32cer version is
1.1.4, the version used to verify the iolist entry point.

## Workloads and interpretation

Run `mix run bench/iodata.exs` for the worker/sink and TCP measurements. Add
`--cert PATH --key PATH` for TLS. The harness reuses #225's worker driver and payload
generator and validates every input with the consumer before timing it. It covers:

- A 470-byte message, 100 messages of 470 bytes per batch, and a 1 MiB message.
- Deterministic JSON-shaped events and deterministic incompressible bytes.
- No compression and ZSTD level 3.
- A synchronous broker sink, TCP loopback, and TLS loopback.

The sink measures worker encoding and process handoff. TCP/TLS also execute the
broker's actual size check and socket-send handler, and wait for the receiver to
acknowledge the complete frame. Connections and TLS handshakes are outside timing.
The receiver reads the length prefix and all bytes before acknowledging. These are
serial entries, with one entry in flight. Entries/second is **not** messages/second
for batches. This is a transport experiment, not broker persistence, Pulsar receipt
latency, concurrent-producer capacity, or WAN throughput.

Benchee reports producer-process allocations and reductions. Its GC-based memory
measurement does not represent total native/off-heap binary allocations, receiver
or TLS-process allocations, peak VM memory, or RSS. In particular, avoiding a large
binary copy can improve time without appearing as an equally large reduction in
reported allocated bytes. No claim about total memory savings follows from this
metric alone.

## Results

The [measurements and per-run statistics](results/iodata/README.md) compare the prototype
with #225 using the same harness. They include all workloads, including regressions.

Across two runs per revision on an M1 Pro with OTP 29:

- Uncompressed 1 MiB messages took 62–63% less time through the worker/sink and
  24–33% less time through TCP loopback.
- Incompressible 1 MiB ZSTD messages took about 38% less time through the sink and
  17% less time through TCP. Compressible 1 MiB JSON was essentially unchanged in
  the sink and about 7% faster in the TCP experiment.
- Small-message medians generally changed by only a few percent. TLS gains were
  smaller and more variable; compressed 1 MiB JSON was essentially unchanged.
- Batch results were mixed, with some run-to-run shifts over 20%. The uncompressed
  JSON worker/sink case regressed 11% in the aggregate, while other cases improved.
  These short runs do not establish a reliable general batching improvement.
- Producer heap allocations generally increased slightly. Total binary-memory
  savings remain unmeasured.

The strongest evidence supports avoiding copies for large frames. Keep this as a
draft experiment until longer runs and representative production traffic establish
whether the workload-specific benefit warrants the change. OTP 28 and other hardware
were not benchmarked here.

## Reproduce

Use the same OTP, Elixir, hardware and benchmark files on both revisions. The
baseline must receive all three harness files below; only library code differs.
Do not run the two VMs concurrently.

```sh
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout /tmp/iodata-key.pem -out /tmp/iodata-cert.pem \
  -days 1 -subj /CN=localhost

mix run bench/iodata.exs --time 2.0 --warmup 0.5 \
  --cert /tmp/iodata-cert.pem --key /tmp/iodata-key.pem \
  --output /tmp/iodata-prototype.json

git worktree add --detach /tmp/pulsar-iodata-base 78ce07d51e653f4a358a9248641914b4f11d9c3a
cp bench/iodata.exs /tmp/pulsar-iodata-base/bench/
cp bench/support/compression.ex bench/support/transport.ex /tmp/pulsar-iodata-base/bench/support/
cd /tmp/pulsar-iodata-base
mix deps.get
mix run bench/iodata.exs --time 2.0 --warmup 0.5 \
  --cert /tmp/iodata-cert.pem --key /tmp/iodata-key.pem \
  --output /tmp/iodata-baseline.json
```

Repeat in alternating order to assess run-to-run noise. Certificates are ephemeral
localhost benchmark fixtures; the benchmark client uses `verify_none` only for
this local listener.

## Validation

On the rebased prototype:

- 570 unit checks passed, including wire-byte/CRC equivalence for nested iodata,
  exact frame-size limits over a socket, and ZSTD chunking with and without a split.
- All 121 tests in `test/integration/producer` and `test/integration/consumer` passed
  with `--max-cases 2 --warnings-as-errors` against Pulsar 4.2.4.
- Formatting, compilation with warnings as errors, strict Credo, and Dialyzer passed.

The integration run used isolated container names and ports because another cluster
occupied the default ports. BookKeeper used a temporary 2 GiB tmpfs for journal and
ledger storage because Docker's disk exceeded its 95% full threshold; startup health
checks were given a longer timeout. These local test overrides were restored and
are not part of the change. This run validates protocol interoperability, not disk
persistence performance. TCP/TLS benchmark connections used independent loopback
listeners and were measured before the integration cluster started.
