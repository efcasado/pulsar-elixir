# Upgrading to 3.0

This guide is a code-migration checklist for applications upgrading from 2.x. For details about
new features and internal design, follow the links in [Where to look next](#where-to-look-next).

First, update the dependency:

```elixir
{:pulsar, "~> 3.0", hex: :pulsar_elixir}
```

Most required changes produce a compiler warning or a startup error. Four do not:

- `config :pulsar` is no longer read. Move it into your supervision tree.
- Consumer callback `init/1` became `init/2`. A stale `init/1` compiles but is never called.
- Partition keys use a different hash by default. Plan the routing change before deployment.
- Compressed chunk framing changed. Drain compressed chunked messages produced by 2.x before
  upgrading.

## 1. Move configuration into your supervision tree

3.0 does not start a client, consumer, or producer from application configuration. Replace
`config :pulsar` with a `Pulsar.Client` child in your application supervisor:

```elixir
# 2.x - config/config.exs
config :pulsar,
  host: "pulsar://localhost:6650",
  consumers: [
    orders: [
      topic: "persistent://public/default/orders",
      subscription_name: "order-service",
      callback_module: MyApp.OrderHandler
    ]
  ],
  producers: [
    audit: [topic: "persistent://public/default/audit"]
  ]

# 3.x - your application supervisor
children = [
  {Pulsar.Client,
   host: "pulsar://localhost:6650",
   consumers: [
     [topic: "persistent://public/default/orders",
      subscription_name: "order-service",
      callback_module: MyApp.OrderHandler,
      name: :orders]
   ],
   producers: [
     [topic: "persistent://public/default/audit", name: :audit]
   ]}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

`:consumers` and `:producers` are now lists of keyword lists. Move the old keyword-list key into
each resource's `:name` option.

Names are exact registry keys in 3.0. If existing code looks up `"orders"`, configure
`name: "orders"`; `name: :orders` is a different key. An unnamed client is registered as
`:default`, which is also the default `:client` used by consumers and producers.

For multiple clusters, add one named client child per cluster:

```elixir
children = [
  {Pulsar.Client, name: :east, host: "pulsar://east.example.com:6650"},
  {Pulsar.Client, name: :west, host: "pulsar://west.example.com:6650"}
]
```

Move the remaining application settings to the resource they configure:

| 2.x application setting | 3.0 option |
| --- | --- |
| `:ping_interval`, `:cleanup_interval`, `:request_timeout` | `Pulsar.Client` |
| `:partition_discovery_interval_ms` | `Pulsar.Consumer` or `Pulsar.Producer` |
| `:startup_delay_ms`, `:startup_jitter_ms` | `Pulsar.Consumer` or `Pulsar.Producer` |
| `:max_backoff` | Removed; reconnect backoff is capped at 30 seconds |
| `:client_version`, `:protocol_version` | Removed |

## 2. Replace calls to the `Pulsar` module

The public API is split between `Pulsar.Client`, `Pulsar.Consumer`, and `Pulsar.Producer`:

| 2.x | 3.0 |
| --- | --- |
| `Pulsar.start_client/1` | `Pulsar.Client.start_link/1` |
| `Pulsar.start/1`, `Pulsar.stop/1` | `Pulsar.Client.start_link/1`, `Pulsar.Client.stop/2` |
| `Pulsar.start_broker/2`, `lookup_broker/2`, `stop_broker/2` | Corresponding `Pulsar.Client` functions |
| `Pulsar.start_consumer/4` | `Pulsar.Consumer.start/4` or `start/1` |
| `Pulsar.stop_consumer/2` | `Pulsar.Consumer.stop/2` |
| `Pulsar.get_consumers/2`, `lookup_consumer/2` | `Pulsar.Client.consumers/1` |
| `Pulsar.ack/3`, `Pulsar.nack/3` | `Pulsar.Consumer.ack/2`, `Pulsar.Consumer.nack/2` |
| `Pulsar.send_flow/2,3` | `Pulsar.Consumer.send_flow/3` |
| `Pulsar.start_producer/2` | `Pulsar.Producer.start/2` or `start/1` |
| `Pulsar.stop_producer/2` | `Pulsar.Producer.stop/2` |
| `Pulsar.get_producers/2`, `lookup_producer/2` | `Pulsar.Client.producers/1` |
| `Pulsar.send/3` | `Pulsar.Producer.send/3` |

For example:

```elixir
# 2.x
Pulsar.start_consumer(topic, subscription, MyApp.Handler, subscription_type: :Shared)
Pulsar.send(:audit, payload)

# 3.x
Pulsar.Consumer.start(topic, subscription, MyApp.Handler, subscription_type: :shared)
Pulsar.Producer.send(:audit, payload)
```

`ack/2` and `nack/2` take the worker pid that delivered the message, not the logical consumer
name. If acknowledgement happens elsewhere, pass `self()` and `message.message_id` out of the
callback:

```elixir
def handle_message(message, state) do
  MyApp.Jobs.enqueue(message.payload, ack: {self(), message.message_id})
  {:noreply, state}
end

# In another process:
Pulsar.Consumer.ack(worker, message_id)
```

Do not call `ack(self(), ...)` synchronously from the callback: the callback runs in the worker,
so that would be a `GenServer.call` to itself.

`Pulsar.Client.consumers/1` and `producers/1` return one topology root per logical resource,
rather than listing its individual workers.

## 3. Update option values

Enum option values are lowercase and snake_case in 3.0:

| Option | 2.x | 3.0 |
| --- | --- | --- |
| `:subscription_type` | `:Exclusive`, `:Shared`, `:Failover`, `:Key_Shared` | `:exclusive`, `:shared`, `:failover`, `:key_shared` |
| `:access_mode` | `:Shared`, `:Exclusive`, `:WaitForExclusive`, `:ExclusiveWithFencing` | `:shared`, `:exclusive`, `:wait_for_exclusive`, `:exclusive_with_fencing` |
| `:compression` | `:NONE`, `:LZ4`, `:ZLIB`, `:SNAPPY`, `:ZSTD` | `:none`, `:lz4`, `:zlib`, `:snappy`, `:zstd` |

Options are validated at startup, so old or misspelled values now raise.

### Changes used by some applications

- Remove `:producer_count`. A producer now has one worker per partition. `:consumer_count`
  remains supported.
- Remove `:host` from `Pulsar.Reader.stream/2`. Start a client first and use `:client` to select
  it when necessary:

  ```elixir
  {:ok, _client} = Pulsar.Client.start_link(host: "pulsar://localhost:6650")
  Pulsar.Reader.stream(topic, start_position: :earliest)
  ```

- If you used `flow_initial: 0` for fully manual flow control, add a custom `:flow_policy`.
  `flow_initial: 0` with the default `:auto` policy is rejected:

  ```elixir
  Pulsar.Consumer.start(topic, subscription, MyApp.Handler,
    flow_initial: 0,
    flow_policy: {MyApp.Flow, :decide, []}
  )

  defmodule MyApp.Flow do
    def decide(_flow), do: :ok
  end
  ```

  Grant permits from another process with `Pulsar.Consumer.send_flow/3`. See
  `Pulsar.Consumer` for custom policy details.

## 4. Update consumer callbacks

Change callback `init/1` to `init/2`. The second argument contains the resolved topic,
partition, subscription, and consumer identity:

```elixir
# 2.x
def init(opts) do
  {:ok, %{max: Keyword.get(opts, :max, 1000)}}
end

# 3.0
def init(opts, context) do
  {:ok, %{max: Keyword.get(opts, :max, 1000), partition: context.partition}}
end
```

Search every callback module for `def init(`. Because `use Pulsar.Consumer.Callback` supplies a
default `init/2`, leaving an old `init/1` in place compiles but silently uses the default state
instead.

Invalid messages now go to the optional `handle_invalid_message/2` callback. Implement it if
your application must record or divert checksum, decompression, or framing failures:

```elixir
def handle_invalid_message(message, state) do
  MyApp.InvalidMessages.record(message.validation_error, message.raw)
  {:ok, state}
end
```

The default implementation logs and acknowledges the invalid message.

## 5. Update message access

`Pulsar.Message` no longer exposes wire-protocol fields at its top level:

| 2.x | 3.0 |
| --- | --- |
| `message.message_id_to_ack` | `message.message_id` |
| `message.command` | `message.raw.command` |
| `message.metadata` | `message.raw.metadata` |
| `message.single_metadata` | `message.raw.single_metadata` |
| `message.broker_metadata` | `message.raw.broker_metadata` |

Use accessors for values that should work across normal, batched, and chunked messages:

```elixir
# 2.x
key = message.single_metadata && message.single_metadata.partition_key
producer = message.metadata.producer_name

# 3.0
key = Pulsar.Message.key(message)
producer = Pulsar.Message.producer_name(message)
```

Other accessors include `publish_time/1`, `event_time/1`, `ordering_key/1`, `properties/1`,
`redelivery_count/1`, `message_id_string/1`, `chunked?/1`, `complete?/1`, and `valid?/1`.

Treat `message.message_id` as opaque and pass it directly to `ack/2` or `nack/2`. It can be a
list for a chunked message.

## 6. Update startup and send-result handling

Consumer and producer startup is asynchronous. If the next operation requires a ready resource,
wait explicitly:

```elixir
{:ok, _producer} = Pulsar.Producer.start(topic: topic, name: :audit)
:ok = Pulsar.Producer.await_ready(:audit, timeout: 10_000)
```

Until initialization finishes, operations can return `{:error, :not_ready}`.

Update producer result matches for these 3.0 return values:

```elixir
case Pulsar.Producer.send(:audit, payload) do
  {:ok, :deduplicated} ->
    :ok

  {:ok, message_id} ->
    {:published, message_id}

  {:error, :producer_queue_full} ->
    {:retry, :overloaded}

  {:error, :send_timeout} ->
    {:retry, :unknown_publish_outcome}
end
```

`:send_timeout` does not prove that the broker rejected the message; it means no acknowledgement
arrived before the deadline. A missing named producer now returns `{:error, :not_found}` instead
of `{:error, :producer_not_found}`.

## 7. Plan deployment compatibility

These changes may require rollout coordination even after the code compiles.

### Partition-key routing

3.0 changes the default partition-key hash from `:erlang.phash2` to `:murmur3_32`. This affects
partitioned topics produced with `:partition_key` and can temporarily break per-key ordering
during a mixed rollout.

Either drain the topic before switching, or preserve the 2.x routing during the rollout:

```elixir
Pulsar.Producer.start(topic: topic, name: :orders, hashing_scheme: :phash2_legacy)
```

Move to `:murmur3_32` after the old messages are drained. Partition keys must be binaries in
3.0.

### Compressed chunked messages

If both `:chunking_enabled` and `:compression` are enabled, drain messages produced by 2.x and
upgrade producers and consumers together. 3.0 does not read the old compressed chunk framing.
Uncompressed chunked messages remain compatible.

`:batch_enabled` and `:chunking_enabled` can no longer be combined. Choose one; 2.x silently
ignored chunking when both were enabled.

### Startup delay

`:startup_delay_ms` and `:startup_jitter_ms` now default to `0` instead of `1000`. Set them
explicitly if your deployment relied on staggered consumer or producer startup.

## Final checklist

- Move all `config :pulsar` values into supervised clients and resources.
- Replace calls to the old `Pulsar` API.
- Lowercase enum option values.
- Change every callback `init/1` to `init/2`.
- Replace direct message metadata access with `Pulsar.Message` accessors.
- Remove `:producer_count` and reader `:host` options where used.
- Update readiness, send-result, and missing-producer matches.
- Plan partition-key and compressed-chunk migrations before deployment.

## Where to look next

- `Pulsar.Client`, `Pulsar.Consumer`, and `Pulsar.Producer` document their options and API.
- `Pulsar.Consumer.Callback` documents callback return values and lifecycle events.
- The [architecture guide](architecture.html) covers ownership, startup, and recovery.
- The [acknowledgement and redelivery](acknowledgements.html), [batching](batching.html),
  [chunking](chunking.html), [schemas](schemas.html), and
  [dead letter policies](dead_letter_policies.html) guides cover those features in depth.
- Broadway users should follow the
  [off_broadway_pulsar 2.0 upgrade guide](https://hexdocs.pm/off_broadway_pulsar/upgrading_to_2-0.html).
