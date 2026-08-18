# Pulsar client benchmarks

This directory contains reproducible, client-level benchmarks for Pulsar. The current
suite supports producer acknowledgement throughput and latency through the Elixir, Go,
and Java clients. Each runner uses the same command-line and JSON result contract.

## Setup

The repository's mise configuration pins the development runtimes used by the benchmark
matrix:

```text
mise install
mise run deps
mise exec python -- python -m pip install -r benchmarks/python/requirements.txt
mix compile
```

The benchmark client dependencies are pinned to the official Pulsar clients:

- Go client `github.com/apache/pulsar-client-go` `v0.15.0`
- Java client `org.apache.pulsar:pulsar-client` `4.2.1`
- Python client `pulsar-client` `3.13.0` for consumer preparation

Maven `3.9.16` is pinned in mise for compiling and launching the Java runner.

Select Go or Java with `--client`:

```text
./benchmarks/run producer \
  --client go \
  --messages 100000 \
  --size 1024 \
  --partitions 3

./benchmarks/run producer \
  --client java \
  --messages 100000 \
  --size 1024 \
  --partitions 3
```

The first example uses the Pulsar cluster defined by the repository root
`docker-compose.yml`:

```text
docker compose up -d
```

When connecting from the host, make sure the broker names advertised by that Compose file
resolve locally:

```text
127.0.0.1 broker1 broker2
```

The benchmark does not start or stop Pulsar automatically. It does create or verify the
partitioned topic through the Pulsar admin API before launching the client. The admin URL is
derived from `--url` for the repository's local ports (`6650` → `8080`, `6651` → `8081`); pass
`--admin-url` when using a different deployment.

## Producer benchmark

Run the default Elixir producer benchmark:

```text
./benchmarks/run producer \
  --messages 100000 \
  --size 1024 \
  --partitions 1
```

Batching can be enabled either as a flag or with an explicit boolean:

```text
./benchmarks/run producer \
  --messages 1000000 \
  --size 1024 \
  --batching true \
  --batch-size 100 \
  --batch-delay-ms 10 \
  --partitions 5 \
  --in-flight 1000
```

Compression and payload entropy are configurable independently:

```text
./benchmarks/run producer \
  --client go \
  --messages 100000 \
  --size 1024 \
  --partitions 3 \
  --compression zstd \
  --payload high-entropy
```

`--compression` accepts `none` or `zstd`. `--payload zero` preserves the highly
compressible all-zero workload; `--payload high-entropy` generates the same deterministic
pseudo-random bytes in every runner. The high-entropy stream uses xorshift32 with seed
`0xA5A5A5A5`, masks each update to 32 bits, and emits the most-significant byte of each
state. This keeps payload contents comparable across Elixir, Go, and Java while avoiding
language-specific random-number generators.

Use a dedicated topic for each experiment. The default is
`persistent://public/default/benchmark-producer`; pass `--topic` when running multiple
experiments against the same cluster.

Use `--dry-run` to inspect the resolved client command without connecting to Pulsar:

```text
./benchmarks/run producer --messages 10 --dry-run
```

The `--client` option accepts `elixir`, `go`, or `java`. The Go and Java runners use the
official client libraries directly; they do not route through another language's client.

## Consumer benchmark

The consumer benchmark prepares a backlog with the Python Pulsar client, then starts the
native consumer runner against a unique subscription:

```text
./benchmarks/run consumer \
  --client elixir \
  --messages 100000 \
  --size 1024 \
  --partitions 3 \
  --subscription-type shared
```

The default subscription type is `shared`. The other accepted values are `exclusive`,
`failover`, and `key-shared`. `--consumers-per-partition` defaults to `1`; increase it when
testing actual sharing, failover, or key-shared distribution rather than only measuring a
single consumer per partition:

```text
./benchmarks/run consumer \
  --messages 10000 \
  --partitions 3 \
  --subscription-type key-shared \
  --consumers-per-partition 2
```

Preparation publishes exactly `--messages` binary payloads with the selected size, payload
mode, compression, and batching settings. It adds benchmark metadata properties, including
the preparation timestamp and sequence, and uses deterministic partition keys for
`key-shared`. The preparation duration is reported separately and is excluded from the
consumer child-process timing.

The consumer subscription always starts at `earliest`, because the backlog is published
before the subscription is created. A generated subscription name prevents an earlier run's
cursor from changing the result. Consumer `p50_us`, `p95_us`, and `p99_us` currently measure
end-to-end age from the preparation timestamp to delivery; these values include time spent
waiting in the preloaded backlog and should not be interpreted as live producer-to-consumer
latency.

The consumer operation has native Elixir, Go, and Java runners. Each runner uses the same
result contract; Java process resource metrics include Maven/JVM startup overhead.

## Result format

The command prints one JSON result. `--output` additionally writes that result to a file:

```text
./benchmarks/run producer \
  --messages 100000 \
  --output benchmarks/results/elixir-producer.json
```

The result contains:

- `messages_requested` and `messages_acked`
- `messages_per_second` and `bytes_per_second`
- `p50_us`, `p95_us`, and `p99_us`
- `latency_type`, `producer_ack` for producers or `consumer_e2e` for consumers
- `errors`
- workload configuration such as payload size, partition count, batching, and in-flight sends
- `cpu_seconds` and `peak_rss_mb` for the benchmark child process
- consumer runs additionally report `messages_received`, `messages_acked`,
  `subscription_type`, `subscription_name`, `consumers_per_partition`, and
  `preparation_duration_us`

Client-observed acknowledgement timing and message counts are the primary benchmark
measurements. Process CPU/RSS are collected by the Python orchestrator using Unix child
resource accounting. Broker Prometheus metrics and runtime-specific diagnostics can be
added as sidecar measurements without changing this workload contract.

The operation duration excludes client setup and producer/consumer readiness, while process
resource metrics cover the complete benchmark child lifetime, including startup and
shutdown.

## Current scope

The current implementation intentionally supports only:

- Elixir, Go, and Java
- producer and consumer workloads through Elixir, Go, and Java runners
- raw deterministic binary payloads with zero or high-entropy modes
- partitioned topics with one logical producer per client
- one benchmark invocation at a time
Broker metric snapshots and CI regression gates remain future work.
