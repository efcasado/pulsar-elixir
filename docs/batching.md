# Batching

## What is Batching?

[Batching](https://pulsar.apache.org/docs/next/concepts-messaging/#batching) collects several
messages in the producer and publishes them together in fewer entries. It trades a little latency
for throughput: fewer, larger writes to the broker instead of one per message.

The unit it publishes is an **entry** — one position on the topic, named by a ledger id and an
entry id. Everything that makes batching different from sending messages one at a time follows
from that single fact:

- The broker **acknowledges** entries, so an ack names the entry a message arrived in
- The broker **dispatches** entries, so `Key_Shared` reads one key per entry
- The broker **delays** entries, so a delayed message cannot ride in a batch
- The broker **redelivers** entries, so configured nack redelivery brings back everything batched
  alongside it

Batching is off by default. A producer with `batch_enabled: false` publishes one entry per
message, and none of the above applies.

## How Batching Works

### Producer Side

`Pulsar.Producer.start/2` adds a producer to a running client, so start one first — in your
supervision tree, or directly in a script:

```elixir
{:ok, _pid} = Pulsar.Client.start_link(host: "pulsar://localhost:6650")
```

With batching enabled, a send no longer publishes immediately:

```elixir
{:ok, producer} = Pulsar.Producer.start(
  "orders",
  batch_enabled: true,
  batch_size: 100,       # Flush once 100 messages are waiting
  flush_interval: 10     # ...or every 10ms, whichever comes first
)

:ok = Pulsar.Producer.await_ready(producer)
```

`send/3` adds a message to the pending batch and **keeps its caller waiting**. It does not return
when the message is buffered — it returns when the broker acknowledges the entry the message
ended up in. The batch is flushed when:

1. **It is full** — `batch_size` messages are waiting
2. **The interval fires** — `flush_interval` milliseconds have passed
3. **A delayed message arrives** — it cannot join a batch, so it publishes what is pending first

On flush, the messages are framed into one payload and compressed as a whole when `:compression`
is set. The default builder publishes one entry; `:key_based` publishes one entry per key. Every
send receives its result when the broker's receipt for its entry comes back.

> #### Fill a batch without blocking on each receipt {: .info}
>
> Use `send_async/3` to start several sends from one process without waiting for each broker
> receipt. It returns a reference for `await/2`; awaiting that reference gives the same result as
> `send/3`:
>
> ```elixir
> refs =
>   for payload <- ["one", "two", "three"] do
>     {:ok, ref} = Pulsar.Producer.send_async(producer, payload)
>     ref
>   end
>
> for ref <- refs do
>   {:ok, _message_id} = Pulsar.Producer.await(ref)
> end
> ```
>
> Async sends still count against `:max_pending_messages` until they finish. A full producer
> reports `{:error, :producer_queue_full}` through `await/2`. The producer's `:send_timeout` also
> covers time spent in a batch and waiting for its receipt.
>
> Call `await/2` from the same process that called `send_async/3`. A finite await timeout does not
> cancel the send; the message may still be published.

### Consumer Side

A batch arrives as one broker message and is unwrapped before your callback sees it:

```elixir
def handle_message(%Pulsar.Message{} = message, state) do
  # One call per message in the batch, not one per entry.
  IO.puts(message.payload)

  {:ok, state}
end
```

Each message carries its own key, properties and event time, which the producer wrote per
message rather than per entry. `Pulsar.Message`'s accessors resolve them for you:

```elixir
Pulsar.Message.key(message)         # this message's key, not the entry's
Pulsar.Message.properties(message)  # this message's properties
Pulsar.Message.event_time(message)  # this message's event time
```

Flow control counts messages, not entries: a batch of 100 spends 100 permits.

With `read_compacted: true`, messages that compaction has replaced are filtered out of a batch
rather than delivered. With the default `false`, the consumer reads the original topic history
and can receive values that compaction has superseded.

## Acknowledging a Batch

**This is the part that behaves differently, and the part worth reading twice.**

An ack names the entry a message arrived in. There is no way to say "message 3 of this entry"
unless the broker is configured for it, so acking one message of a batch would acknowledge every
message batched with it — and lose the ones not yet processed.

Instead, acking a batched message **counts it off**. The entry is acknowledged once every message
in it has been acked:

```elixir
def handle_message(%Pulsar.Message{} = message, state) do
  # Counted off. The entry is acked when its last message is.
  {:ok, state}
end
```

Three consequences:

- **A message left unacked holds the ones batched with it.** A callback that returns
  `{:noreply, state}` and never acks keeps its entry's bookkeeping for the life of the consumer,
  and the entry stays in the subscription backlog.
- **With `:redelivery_interval` configured, a nack brings the whole entry back**, including
  messages already acked from it. Your callback sees those again. Without an interval the entry
  remains unacknowledged and normally returns only after the consumer restarts.
- **A partially acked batch can leave backlog metrics unchanged.** The subscription cursor cannot
  advance past the entry until every message in it is acknowledged, so the backlog moves at entry
  boundaries rather than after each processed message.
- **Stopping a callback partway through a batch leaves the entry outstanding.** Without batch-index
  acknowledgements, the processed prefix may be redelivered with the unread suffix when another
  consumer receives the entry.

### Narrowing redelivery with `:batch_index_ack_enabled`

A consumer can tell the broker exactly which messages of an entry an ack covered, so that a
redelivery brings back only the rest:

```elixir
{:ok, consumer} = Pulsar.Consumer.start(
  "orders", "billing", MyConsumer,
  batch_index_ack_enabled: true
)
```

> #### Requires broker support, and cannot be detected {: .warning}
>
> `:batch_index_ack_enabled` needs `acknowledgmentAtBatchIndexLevelEnabled=true` on the broker.
> Without it the broker ignores the set and acknowledges the **whole entry**, losing the messages
> batched alongside the acked one. Nothing in the protocol reports the setting, so the client
> cannot check: Pulsar's shipped `broker.conf` enables it, `standalone.conf` does not.

For individual acknowledgements it costs one ack command per message rather than one per entry,
so it only pays for itself when messages in a batch meet different fates.

### Cumulative acknowledgements and batches

`ack_type: :cumulative` and `batch_index_ack_enabled: true` are independent settings. The first
chooses whether an ack covers one message or everything through it; the second chooses whether
that ack can identify a position within a batch.

```elixir
{:ok, consumer} = Pulsar.Consumer.start(
  "orders", "billing", MyConsumer,
  subscription_type: :exclusive,
  ack_type: :cumulative,
  batch_index_ack_enabled: true
)
```

When a cumulative ack targets part of a batch:

- With batch-index acknowledgement enabled, the ack set clears the prefix through the target.
  Redelivery returns only the suffix that is still outstanding.
- With it disabled, the cursor stops at the previous entry. The current batch remains outstanding
  in full, avoiding acknowledgement of messages after the target.

Cumulative acknowledgement is available only for `:exclusive` and `:failover` subscriptions,
which have a single cursor to move. The broker-support warning above applies to cumulative and
individual batch-index acknowledgements alike.

## Keys and `Key_Shared`

`Key_Shared` dispatches on the key of the **entry**, not of the messages inside it. A batch
therefore carries one key for dispatch purposes, taken from its first message:

```elixir
# All three can ride one entry, dispatched on "tenant-1"
{:ok, a} = Pulsar.Producer.send_async(producer, "a", partition_key: "tenant-1")
{:ok, b} = Pulsar.Producer.send_async(producer, "b", partition_key: "tenant-2")
{:ok, c} = Pulsar.Producer.send_async(producer, "c", partition_key: "tenant-1")

for ref <- [a, b, c], do: Pulsar.Producer.await(ref)
```

For a subscription that is not `Key_Shared` this does not matter. For one that is, when those
messages share a batch, the message keyed `tenant-2` is dispatched on `tenant-1`, and per-key
ordering is not preserved.

`batch_builder: :key_based` fixes that by publishing one entry per key:

```elixir
{:ok, producer} = Pulsar.Producer.start(
  "orders",
  batch_enabled: true,
  batch_builder: :key_based
)

:ok = Pulsar.Producer.await_ready(producer)
```

Messages are grouped on their ordering key, falling back to their partition key — the same order
the broker resolves a dispatch key in, so messages bound for one consumer stay in one entry.

Two things to know before enabling it:

- It **regroups the batch**. Order holds within a key, not across keys, so a subscription reading
  the topic in order sees a different order than it would with `:default`.
- `batch_size` caps the whole batch rather than each entry, so it suits a small key space. Keys
  that are mostly unique leave an entry per message, at which point batching only adds overhead.

## Delayed Delivery

`:deliver_at_time` and `:deliver_after` name a time for an entry, and an entry holds many
messages, so a delayed message cannot share one:

```elixir
# Publishes the pending batch, then this message on its own
Pulsar.Producer.send(producer, "reminder", deliver_after: 60_000)
```

The producer flushes what is pending first so the delayed message does not overtake messages
accepted before it, then publishes it as its own entry. This is what the Java and Go clients do.

## Configuration Options

> #### Warning {: .warning}
>
> Batching and chunking cannot be enabled simultaneously on a producer: a batch is one entry
> holding many messages, and a chunked message is one message spread over many entries. Starting
> a producer with both `batch_enabled: true` and `chunking_enabled: true` raises a validation
> error rather than silently picking one.

### Producer Configuration

```elixir
{Pulsar.Client,
 host: "pulsar://localhost:6650",
 producers: [
   [topic: "orders",
    name: :orders_producer,
    batch_enabled: true,        # Enable batching (default: false)
    batch_size: 100,            # Messages before a flush (default: 100)
    flush_interval: 10,         # Milliseconds between flushes (default: 10)
    batch_builder: :default,    # or :key_based (default: :default)
    send_timeout: 30_000,       # Buffer-to-receipt deadline (default: 30 seconds)
    max_pending_messages: 1000  # Sends accepted but not completed (default: 1000)
   ]
 ]}
```

### Consumer Configuration

```elixir
{Pulsar.Client,
 host: "pulsar://localhost:6650",
 consumers: [
   [topic: "orders",
    subscription_name: "billing",
    callback_module: MyConsumer,

    ack_type: :individual,           # :individual or :cumulative (default: :individual)
    batch_index_ack_enabled: false,  # Track positions within batches (default: false)
    redelivery_interval: 5_000       # Needed for a nack to bring anything back
   ]
 ]}
```

#### Configuration Details

- **`batch_size`**: Messages to collect before flushing. Counts the whole batch, including under
  `:key_based`, where the messages may end up spread across several entries.

- **`flush_interval`**: Milliseconds between flushes. This is the latency batching costs you, and
  it is part of how long `send/3` or `await/2` waits for a low-volume batch. Keep it comfortably
  below `:send_timeout`.

- **`batch_builder`**: How a flushed batch is divided into entries. `:default` publishes one
  entry; `:key_based` publishes one per key.

- **`send_timeout`**: Milliseconds from the producer accepting a send until it gives up waiting
  for a broker receipt. Defaults to 30 seconds and includes time spent waiting in a batch. A
  timeout does not prove that the broker did not publish the message.

- **`max_pending_messages`**: Sends the producer can carry before refusing more. Defaults to
  1,000 and counts messages both before and after a batch is flushed, until their sends finish.

- **`batch_index_ack_enabled`**: Whether acks name individual messages of an entry. Requires
  broker support, as described above.

- **`ack_type`**: Whether an acknowledgement covers the named message (`:individual`) or every
  message through it (`:cumulative`). Cumulative acknowledgement requires an `:exclusive` or
  `:failover` subscription and combines with `batch_index_ack_enabled` as described above.

## Example: Batched Orders with Per-Key Ordering

```elixir
{:ok, _pid} = Pulsar.Client.start_link(host: "pulsar://localhost:6650")

defmodule Billing do
  use Pulsar.Consumer.Callback

  def handle_message(%Pulsar.Message{} = message, state) do
    # One call per order, with that order's own key.
    :ok = charge(Pulsar.Message.key(message), message.payload)

    # Counted off against its entry; the entry is acked once all of its orders are.
    {:ok, state}
  end
end

{:ok, consumer} = Pulsar.Consumer.start("orders", "billing", Billing,
  subscription_type: :key_shared,
  redelivery_interval: 5_000
)

:ok = Pulsar.Consumer.await_ready(consumer)

{:ok, producer} = Pulsar.Producer.start(
  "orders",
  batch_enabled: true,
  batch_size: 50,
  flush_interval: 20,
  batch_builder: :key_based
)

:ok = Pulsar.Producer.await_ready(producer)

refs =
  for {tenant, order} <- orders do
    {:ok, ref} = Pulsar.Producer.send_async(producer, order, partition_key: tenant)
    ref
  end

for ref <- refs do
  {:ok, _msg_id} = Pulsar.Producer.await(ref)
end
```

## Telemetry Events

The producer emits one batch event per entry formed from a batch. Messages published outside a
batch use the regular message event instead:

| Event | Measurements | When |
| --- | --- | --- |
| `[:pulsar, :producer, :batch, :published]` | `count` | A batched entry is published |
| `[:pulsar, :producer, :message, :published]` | `count` | A message is published outside a batch |

For the batch event, `count` is the messages in that entry; for the message event it is always one.
A delayed message uses the message event even on a batching producer because it cannot join a
batch. Both events carry `sequence_id` alongside the `topic`, `base_topic`, `partition`,
`producer_id` and `producer_name` that every producer event carries. Under `:key_based`, the batch
event fires once per key, so a flush of three keys emits three events rather than one, and their
counts sum to the batch.

On the consumer side, `[:pulsar, :consumer, :message, :nacked]` counts messages while
`[:pulsar, :consumer, :redelivery, :requested]` counts entries, since redelivery is asked for per
entry. Against a batching producer the two do not line up. See
[Dead Letter Policies](dead_letter_policies.md) for the rest of the consumer's events.

`:topic` names a single partition and `:base_topic` the topic it belongs to, so one set of events
both aggregates over a partitioned topic and breaks down by partition. They are equal, and
`:partition` is `nil`, when the topic is not partitioned.
