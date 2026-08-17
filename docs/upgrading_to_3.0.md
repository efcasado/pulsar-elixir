# Upgrading to 3.0

3.0 makes `Pulsar.Client`, `Pulsar.Consumer` and `Pulsar.Producer` the API, moves configuration
out of `config :pulsar` and into the supervision tree, and fixes two things that were on the
wire: partition key routing and chunk framing.

Most of it is caught by the compiler or at boot. Four changes are not, and they are the ones to
read first:

- [Application configuration](#application-configuration) is ignored rather than rejected, so
  an upgraded application starts with no client, no consumers and no producers.
- [`init/1` became `init/2`](#callback-initialization), and a stale `init/1` is never called.
- [Partition keys hash differently](#partition-key-routing), so a key moves to another
  partition of a partitioned topic.
- [Chunked messages are framed differently](#chunking) when compression is on, and a
  mixed-version deployment cannot read them.

Bump the dependency:

```elixir
{:pulsar, "~> 3.0", hex: :pulsar_elixir}
```

## At a glance

| 2.x | 3.0 | Where |
| --- | --- | --- |
| `config :pulsar, host: …` | `{Pulsar.Client, host: …}` in your tree | [Application configuration](#application-configuration) |
| `Pulsar.start_consumer/4`, `Pulsar.send/3`, … | `Pulsar.Consumer.start/4`, `Pulsar.Producer.send/3` | [The module surface](#the-module-surface) |
| `subscription_type: :Shared` | `subscription_type: :shared` | [Option atoms](#option-atoms) |
| `message.metadata.producer_name` | `Pulsar.Message.producer_name(message)` | [The message struct](#the-message-struct) |
| `def init(args)` | `def init(args, context)` | [Callback initialization](#callback-initialization) |
| `flow_initial: 0` | `flow_policy: {m, f, a}` | [Manual flow control](#manual-flow-control) |
| `:erlang.phash2` key routing | `:murmur3_32` | [Partition key routing](#partition-key-routing) |
| `producer_count: 4` | Removed | [Removed options](#removed-options) |
| `Pulsar.Reader.stream(topic, host: …)` | Start a client first | [Removed options](#removed-options) |

## Application configuration

2.x shipped an `Application` callback that read `config :pulsar` at boot and started everything
it found there. 3.0 has none: the library starts nothing on its own, and nothing reads
`Application.get_env(:pulsar, …)` any more.

Configuration left in `config.exs` is therefore not rejected — it is not read. The application
boots, and the first `Pulsar.Producer.send/3` answers `{:error, :not_found}` because no producer
was ever started.

Move the client into your own supervision tree, and declare consumers and producers on it:

```elixir
# 2.x
config :pulsar,
  host: "pulsar://localhost:6650",
  consumers: [
    my_consumer: [
      topic: "persistent://public/default/orders",
      subscription_name: "order-service",
      callback_module: MyApp.OrderHandler
    ]
  ],
  producers: [
    my_producer: [topic: "persistent://public/default/audit"]
  ]

# 3.0
children = [
  {Pulsar.Client,
   host: "pulsar://localhost:6650",
   consumers: [
     [topic: "persistent://public/default/orders",
      subscription_name: "order-service",
      callback_module: MyApp.OrderHandler,
      name: "my_consumer"]
   ],
   producers: [
     [topic: "persistent://public/default/audit", name: :my_producer]
   ]}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

`:consumers` and `:producers` are now lists of keyword lists rather than a keyword list keyed by
name, and the name each was keyed by moves into a `:name` option. Options are validated, so a
misspelled key raises at boot instead of being ignored.

> #### Names are registry keys, not strings to be normalized {: .warning}
>
> 2.x registered a configured consumer under `Atom.to_string(name)` and a configured producer
> under the atom itself. 3.0 registers whatever you pass. `name: :my_consumer` and
> `name: "my_consumer"` are two different keys, and only the one you registered resolves. If
> anything looks a consumer up by string, keep the string.

Multiple clusters work the same way, one client each:

```elixir
# 2.x
config :pulsar,
  clients: [
    client_1: [host: "pulsar://host.cluster1.com:6650"],
    client_2: [host: "pulsar://host.cluster2.com:6650"]
  ]

# 3.0
children = [
  {Pulsar.Client, name: :client_1, host: "pulsar://host.cluster1.com:6650"},
  {Pulsar.Client, name: :client_2, host: "pulsar://host.cluster2.com:6650"}
]
```

An unnamed client registers as `:default`, which is also the default `:client` of every consumer
and producer, so a single-cluster application needs no names at all.

### Global tunables

The rest of `config :pulsar` was a set of process-wide tunables. Each is now an option on the
thing it configures, which means two clients can differ:

| 2.x application env | 3.0 |
| --- | --- |
| `:ping_interval` | `Pulsar.Client` option |
| `:cleanup_interval` | `Pulsar.Client` option |
| `:request_timeout` | `Pulsar.Client` option |
| `:partition_discovery_interval_ms` | `Pulsar.Consumer` / `Pulsar.Producer` option |
| `:startup_delay_ms`, `:startup_jitter_ms` | `Pulsar.Consumer` / `Pulsar.Producer` option |
| `:max_backoff` | Not configurable; reconnect backoff is capped at 30s |
| `:client_version`, `:protocol_version` | Not configurable |

## The module surface

2.x put everything on `Pulsar`, which delegated to internal group and partition modules. 3.0
moves each function to the module that owns the thing it acts on, and `Pulsar` itself is
documentation only.

| 2.x | 3.0 |
| --- | --- |
| `Pulsar.start_client/1` | `Pulsar.Client.start_link/1` |
| `Pulsar.start/1`, `Pulsar.stop/1` | `Pulsar.Client.start_link/1`, `Pulsar.Client.stop/2` |
| `Pulsar.start_broker/2`, `lookup_broker/2`, `stop_broker/2` | `Pulsar.Client.start_broker/2`, `lookup_broker/2`, `stop_broker/2` |
| `Pulsar.start_consumer/4` | `Pulsar.Consumer.start/4` (or `start/1` with all options) |
| `Pulsar.stop_consumer/2` | `Pulsar.Consumer.stop/2` |
| `Pulsar.get_consumers/2`, `lookup_consumer/2` | `Pulsar.Client.consumers/1` |
| `Pulsar.ack/3`, `Pulsar.nack/3` | `Pulsar.Consumer.ack/2`, `Pulsar.Consumer.nack/2` |
| `Pulsar.send_flow/2,3` | `Pulsar.Consumer.send_flow/3` |
| `Pulsar.start_producer/2` | `Pulsar.Producer.start/2` (or `start/1` with all options) |
| `Pulsar.stop_producer/2` | `Pulsar.Producer.stop/2` |
| `Pulsar.get_producers/2`, `lookup_producer/2` | `Pulsar.Client.producers/1` |
| `Pulsar.send/3` | `Pulsar.Producer.send/3` |

These are calls to functions that no longer exist, so the compiler warns on every one.

```elixir
# 2.x
{:ok, _pid} =
  Pulsar.start_consumer(
    "persistent://public/default/orders",
    "order-service",
    MyApp.OrderHandler,
    subscription_type: :Key_Shared,
    consumer_count: 3
  )

Pulsar.send(:my_producer, "payload")

# 3.0
{:ok, _pid} =
  Pulsar.Consumer.start(
    "persistent://public/default/orders",
    "order-service",
    MyApp.OrderHandler,
    subscription_type: :key_shared,
    consumer_count: 3
  )

Pulsar.Producer.send(:my_producer, "payload")
```

Three details behind the rename:

**`ack/2` and `nack/2` take a pid, not a name.** They target the worker that delivered the
message, which a name cannot identify: a consumer with several partitions or several
`:consumer_count` workers is many processes, and only one of them holds the message. Capture
`self()` in `handle_message/2` and hand it along with the message id:

```elixir
def handle_message(message, state) do
  MyApp.Jobs.enqueue(message.payload, ack: {self(), message.message_id})
  {:noreply, state}
end
```

The job then calls `Pulsar.Consumer.ack(consumer, message_id)`. It must be another process:
every callback runs inside its worker, so `ack(self(), …)` from the callback is a `GenServer`
call to itself, which exits with `:calling_self` and takes the consumer down.

**There is no separate lookup step.** `stop/2`, `send_flow/3`, `await_ready/2`,
`Pulsar.Producer.send/3` and `send_async/3` all take a pid or a registered name directly, with
`:client` selecting the client a name is resolved against.

**`Pulsar.Client.consumers/1` lists roots, not workers.** 2.x `get_consumers/2` returned the
individual consumer processes in a group. 3.0 returns one stable pid per logical consumer,
however many partitions and workers sit under it.

## Option atoms

Every enum option value is lowercase and snake_case now. Values are validated, so a stale one
raises when the consumer or producer starts rather than being silently accepted.

| Option | 2.x | 3.0 |
| --- | --- | --- |
| `:subscription_type` | `:Exclusive`, `:Shared`, `:Failover`, `:Key_Shared` | `:exclusive`, `:shared`, `:failover`, `:key_shared` |
| `:access_mode` | `:Shared`, `:Exclusive`, `:WaitForExclusive`, `:ExclusiveWithFencing` | `:shared`, `:exclusive`, `:wait_for_exclusive`, `:exclusive_with_fencing` |
| `:compression` | `:NONE`, `:LZ4`, `:ZLIB`, `:SNAPPY`, `:ZSTD` | `:none`, `:lz4`, `:zlib`, `:snappy`, `:zstd` |

`:initial_position` was already `:earliest` / `:latest` and is unchanged. `:shared` remains the
default subscription type, and `:none` the default compression.

## The message struct

2.x exposed the wire protocol structs directly, and their shape depended on how the message was
delivered: a batched message carried its key in `single_metadata`, a non-batched one in
`metadata`, and a chunked one carried a list of both. 3.0 keeps the protocol structs under
`:raw` and answers the common questions with accessors that work the same way regardless of
delivery.

The struct went from seven fields to five:

| 2.x field | 3.0 |
| --- | --- |
| `:payload` | `:payload`, unchanged |
| `:message_id_to_ack` | `:message_id` |
| `:chunk_metadata` | `:chunk_metadata`, unchanged |
| `:command` | `raw.command` |
| `:metadata` | `raw.metadata` |
| `:single_metadata` | `raw.single_metadata` |
| `:broker_metadata` | `raw.broker_metadata` |
| — | `:validation_error`, new |

```elixir
# 2.x
def handle_message(%Pulsar.Message{} = message, state) do
  key = message.single_metadata && message.single_metadata.partition_key
  producer = message.metadata.producer_name
  Pulsar.Consumer.ack(consumer, message.message_id_to_ack)
  {:noreply, state}
end

# 3.0
def handle_message(%Pulsar.Message{} = message, state) do
  key = Pulsar.Message.key(message)
  producer = Pulsar.Message.producer_name(message)
  Pulsar.Consumer.ack(consumer, message.message_id)
  {:noreply, state}
end
```

The accessors are `producer_name/1`, `publish_time/1`, `event_time/1`, `key/1`,
`ordering_key/1`, `properties/1`, `redelivery_count/1` and `message_id_string/1`, plus
`chunked?/1`, `complete?/1`, `valid?/1` and `num_broker_messages/1`. Prefer them over `:raw`,
whose shape follows the wire protocol and is explicitly unstable.

`:message_id` is opaque: it carries a batch index for a batched message, and for a chunked one
it stands for every chunk, so it is a list there. Pass it to `ack/2` and `nack/2` rather than
matching on it.

### Invalid messages

`:validation_error` and `c:Pulsar.Consumer.Callback.handle_invalid_message/2` are new. A message
whose frame failed its CRC32C check is delivered there instead of `handle_message/2`, with
`payload` holding unverified bytes. The default implementation logs a warning and acknowledges
it, so it is not redelivered; override it to record or divert such messages. 2.x had no checksum
verification, so this is new behaviour rather than a rename.

## Callback initialization

The callback module's `init/1` became `c:Pulsar.Consumer.Callback.init/2`. The second argument
is the consumer's resolved identity, which matters on a partitioned topic: several callback
processes share one configured topic while each handles a different partition.

```elixir
%{
  topic: "persistent://public/default/orders-partition-2",
  base_topic: "persistent://public/default/orders",
  partition: 2,
  subscription_name: "order-service",
  subscription_type: :shared,
  consumer_name: "orders-order-service-partition-2-1"
}
```

`:topic` and `:base_topic` are equal, and `:partition` is `nil`, on a topic that is not
partitioned.

> #### A stale `init/1` compiles and never runs {: .warning}
>
> `use Pulsar.Consumer.Callback` defines a default `init/2` and marks it overridable. A module
> that still defines `init/1` overrides nothing: the default `init/2` returning `{:ok, nil}` is
> what the consumer calls, and the state your `init/1` built is silently never used. There is no
> warning. Grep for `def init(` in every callback module.

```elixir
# 2.x
def init(opts) do
  {:ok, %{count: 0, max: Keyword.get(opts, :max, 1000)}}
end

# 3.0
def init(opts, _context) do
  {:ok, %{count: 0, max: Keyword.get(opts, :max, 1000)}}
end
```

## Manual flow control

In 2.x, `flow_initial: 0` meant "grant nothing and leave refills to `Pulsar.send_flow/2`". In
3.0 the refill strategy is its own option, and `flow_initial: 0` under the default `:auto` policy
raises at startup — it is the configuration that never receives a message, since the broker is
granted nothing and no delivery ever arrives to trigger a refill.

```elixir
# 2.x
Pulsar.start_consumer(topic, subscription, MyApp.Handler, flow_initial: 0)

# 3.0 — permits granted entirely from outside the consumer
Pulsar.Consumer.start(topic, subscription, MyApp.Handler,
  flow_initial: 0,
  flow_policy: {MyApp.Flow, :never_grant, []}
)
```

A policy is asked after every delivery with `%{consumed: permits, outstanding: permits}` and
answers `:ok` or `{:grant, permits}`. It is called as `[flow | args]`, so the `args` in the
tuple decide its arity — `[]` above means `never_grant/1`, and a policy given `[100]` takes the
flow and that argument:

```elixir
defmodule MyApp.Flow do
  def never_grant(_flow), do: :ok

  def decide(%{outstanding: outstanding}, refill) when outstanding <= 20, do: {:grant, refill}
  def decide(_flow, _refill), do: :ok
end
```

Two constraints follow from it only being asked after a delivery: it cannot grant the first
permits, which come from `:flow_initial` or from `Pulsar.Consumer.send_flow/3` in another
process; and it runs inside the consumer, so it must not call `send_flow/3` on that consumer,
which would deadlock.

If you were using the 2.x default flow settings, nothing changes: `:auto` with the same
`:flow_initial`, `:flow_threshold` and `:flow_refill` defaults is what you already had.

## Partition key routing

2.x picked the partition of a partitioned topic with `:erlang.phash2/2`. No other Pulsar client
implements that, so an Elixir producer and a Java or Go producer sent the same key to different
partitions. 3.0 defaults to `:murmur3_32`, which every client implements identically.

Nothing fails. The keys simply land elsewhere, which breaks per-key ordering while both the old
and the new partition still hold messages for that key, and moves keys away from the consumers
currently pinned to them under a `:key_shared` subscription.

This only affects partitioned topics you publish to with a `:partition_key`. If that is you,
choose one:

**Drain, then switch.** Stop producing, let consumers empty the topic, then upgrade. Ordering
is preserved because nothing is in flight to be reordered. This is the recommended path.

**Keep the old routing while upgrading.** `:phash2_legacy` reproduces the pre-3.0 partition
choice exactly, so keys stay where they are:

```elixir
Pulsar.Producer.start(topic: topic, name: :orders, hashing_scheme: :phash2_legacy)
```

It is a migration path, not a peer of the other schemes: no other client can reproduce it. Move
to `:murmur3_32` once you can drain.

`:java_string_hash` is also available, matching what the Java and Go clients use when left at
their own defaults.

One related tightening: `:partition_key` must be a binary now, where 2.x accepted any term.

## Chunking

2.x compressed each chunk on its own. Every other Pulsar client compresses the whole message and
then splits the compressed bytes, and 3.0 now does the same — which is what makes a chunked
message readable by a Java consumer, and what makes 2.x and 3.0 unable to read each other's.

The incompatibility is limited to producers with **both** `:chunking_enabled` and
`:compression` set. Uncompressed chunked messages are framed the same way in both versions and
cross freely.

How it fails depends on the codec. Under `:lz4`, `:snappy` and `:zlib` the consumer assembles the
chunks, hands them to the decompressor, and the decompressor rejects bytes that are not one
compressed stream — which takes the worker down, and the unacknowledged message is redelivered
into the same crash. Under `:zstd` it is quiet: decompression answers an error tuple instead of
raising, and that tuple reaches your callback in place of `payload`. Compressed chunked messages
already on a topic when you upgrade behave the same way.

So if you use compression together with chunking, drain the topic before upgrading, and upgrade
producers and consumers together.

Two smaller changes come with it. A payload is now compressed *before* it is measured against
`:max_message_size`, so a large payload that compresses under the limit is sent whole and never
chunked at all. And the chunk size is capped so a chunk plus its metadata stays inside the
broker's advertised frame limit, which makes chunks slightly smaller than `:max_message_size`;
a producer whose `:properties` leave no room for a payload now gets `{:error, :metadata_too_large}`
rather than a frame the broker rejects.

Combining `:batch_enabled` with `:chunking_enabled` raises now. 2.x accepted both and batched,
silently ignoring `:chunking_enabled`, so a payload over `:max_message_size` went into a batch
entry whole rather than being split.

Incomplete chunked messages reach the callback with `chunk_metadata.complete == false`, as
before, but their payload is whatever chunks arrived — still compressed, since a partial message
cannot be decompressed. Treat it as opaque.

## Removed options

**`:producer_count` is gone.** A producer now runs exactly one worker per partition, which keeps
one ordered send lane, one sequence-id domain and one batching domain per partition. Several
workers on the same partition gave none of those guarantees; concurrency comes from partitioning
the topic instead.

```elixir
# 2.x
Pulsar.start_producer(topic, producer_count: 4)

# 3.0
Pulsar.Producer.start(topic: topic)
```

`:consumer_count` on the consumer side is unchanged — several consumers on one subscription is
a Pulsar concept, not a client-side one.

**`Pulsar.Reader.stream/2` no longer takes `:host`.** It reads through a client like everything
else, so start one first:

```elixir
# 2.x
Pulsar.Reader.stream(topic, host: "pulsar://localhost:6650", start_position: :earliest)

# 3.0
{:ok, _pid} = Pulsar.Client.start_link(host: "pulsar://localhost:6650")
Pulsar.Reader.stream(topic, start_position: :earliest)
```

The stream no longer closes a connection it did not open. `:client` selects which one to read
through, defaulting to `:default`.

## Behaviour changes that need no code change

These require no edits, but they change what you observe.

**Consumers and producers subscribe immediately.** `:startup_delay_ms` and `:startup_jitter_ms`
both default to `0`, where 2.x defaulted both to `1000`. A broker that is not connected yet is
retried, so the delay was only ever useful to stagger a large fleet of simultaneous restarts.
Set them explicitly if you relied on that.

**Startup is asynchronous.** `Pulsar.Consumer.start/1` and `Pulsar.Producer.start/1` return once
the topology root is up, while discovery and worker initialization continue. Operations answer
`{:error, :not_ready}` until they finish. Use `await_ready/2` where work must not observe that:

```elixir
:ok = Pulsar.Producer.await_ready(:my_producer, timeout: 10_000)
```

**Deduplicated sends report themselves.** On a topic with deduplication enabled,
`Pulsar.Producer.send/3` can answer `{:ok, :deduplicated}`: the broker recognised the sequence id,
kept the message it already had, and assigned this call no message id. 2.x reported this as
`{:ok, message_id}` with a message id that referred to nothing. Match on `{:ok, id}` expecting a
`MessageIdData` and you will now see the atom.

**Send timeouts changed shape.** 2.x `Pulsar.send/3` defaulted `:timeout` to 5000 and let the
`GenServer.call` exit, surfacing as `{:error, {:producer_died, reason}}`. 3.0 defaults `:timeout`
to `:infinity` and bounds the wait with the producer's `:send_timeout` (30s by default),
answering `{:error, :send_timeout}`. A producer already holding `:max_pending_messages`
unanswered sends refuses more with `{:error, :producer_queue_full}` instead of queueing
indefinitely.

**A missing producer is `{:error, :not_found}`**, where 2.x answered
`{:error, :producer_not_found}`.

**Message frames are checksum-verified.** A frame whose CRC32C does not match is delivered to
`handle_invalid_message/2` rather than being parsed as though it were intact.

**Telemetry metadata gained fields.** Consumer and producer events now carry `topic`,
`base_topic`, `partition` and `subscription_name` alongside what they carried before, so one set
of handlers can both aggregate over a partitioned topic and break down by partition. Existing
handlers keep working.

## Complete example

```elixir
# 2.x
config :pulsar,
  host: "pulsar://localhost:6650",
  consumers: [
    orders: [
      topic: "persistent://public/default/orders",
      subscription_name: "order-service",
      callback_module: MyApp.OrderHandler,
      subscription_type: :Key_Shared,
      consumer_count: 3
    ]
  ],
  producers: [
    audit: [topic: "persistent://public/default/audit", compression: :LZ4]
  ]

defmodule MyApp.OrderHandler do
  use Pulsar.Consumer.Callback

  require Logger

  def init(_args) do
    {:ok, %{count: 0}}
  end

  def handle_message(%Pulsar.Message{} = message, state) do
    key = message.single_metadata && message.single_metadata.partition_key
    Logger.info("order #{key} from #{message.metadata.producer_name}")
    Pulsar.send(:audit, message.payload)
    {:ok, %{state | count: state.count + 1}}
  end
end
```

```elixir
# 3.0
children = [
  {Pulsar.Client,
   host: "pulsar://localhost:6650",
   consumers: [
     [topic: "persistent://public/default/orders",
      subscription_name: "order-service",
      callback_module: MyApp.OrderHandler,
      subscription_type: :key_shared,
      consumer_count: 3,
      name: :orders]
   ],
   producers: [
     [topic: "persistent://public/default/audit", compression: :lz4, name: :audit]
   ]}
]

Supervisor.start_link(children, strategy: :one_for_one)

defmodule MyApp.OrderHandler do
  use Pulsar.Consumer.Callback

  require Logger

  def init(_args, context) do
    {:ok, %{count: 0, partition: context.partition}}
  end

  def handle_message(%Pulsar.Message{} = message, state) do
    Logger.info("order #{Pulsar.Message.key(message)} from #{Pulsar.Message.producer_name(message)}")
    Pulsar.Producer.send(:audit, message.payload)
    {:ok, %{state | count: state.count + 1}}
  end
end
```

## Where to look next

- `Pulsar.Client`, `Pulsar.Consumer` and `Pulsar.Producer` document every option with its type
  and default.
- The [architecture guide](architecture.html) covers the ownership tree, asynchronous startup
  and the recovery model that the new supervision layout implies.
- The [batching](batching.html), [chunking](chunking.html) and
  [dead letter policies](dead_letter_policies.html) guides cover the areas this release changed
  most.
- Broadway users upgrade through
  [off_broadway_pulsar 2.0](https://hexdocs.pm/off_broadway_pulsar/upgrading_to_2-0.html), which
  handles most of this on your behalf.
