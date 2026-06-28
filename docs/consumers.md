# Consumers

## What is a Consumer?

A Consumer is a callback-based, long-running subscriber to a Pulsar topic. Unlike a
[Reader](reader.md) — which pulls a finite stream of messages into the calling process — a
Consumer holds a durable **subscription** on the broker, invokes your callback for every
message, and is supervised so it reconnects and resumes from where it left off.

Reach for a Consumer when you want continuous, supervised processing; reach for a Reader
for one-off reads, replays, or stream pipelines.

## Your first consumer

A consumer is a module that uses `Pulsar.Consumer.Callback` and implements
`handle_message/2`:

```elixir
defmodule MyConsumer do
  use Pulsar.Consumer.Callback

  def handle_message(message, state) do
    IO.puts("Received: #{message.payload}")
    {:ok, state}
  end
end
```

The return value tells the client what to do with the message:

- `{:ok, state}` — acknowledge it
- `{:error, reason, state}` — negatively acknowledge it (redeliver later, or route to the dead-letter topic)
- `{:noreply, state}` — you will acknowledge manually with `Pulsar.Consumer.ack/2` / `nack/2`

Start it from your application config:

```elixir
config :pulsar,
  host: "pulsar://localhost:6650",
  consumers: [
    my_consumer: [
      topic: "persistent://public/default/my-topic",
      subscription_name: "my-subscription",
      callback_module: MyConsumer
    ]
  ]
```

or dynamically:

```elixir
Pulsar.start_consumer("persistent://public/default/my-topic", "my-subscription", MyConsumer)
```

## Classic vs. scalable topics

Pulsar has two families of topics, and this client consumes both.

**Classic topics** are what most Pulsar users know: a topic is either non-partitioned (one
stream) or **partitioned** into a fixed number of partitions you choose up front. More
partitions means more parallelism, but changing the count later is disruptive.

**Scalable topics** (new in Pulsar 5, addressed with the `topic://` scheme) are a single
logical stream that the broker sizes to its actual load: internally it divides the key
space into **segments** and splits or merges them as traffic rises and falls — no partition
count to pick, no downtime to resize, and per-key ordering preserved across resizes.

How you consume is selected by the `:consumer_type` option, *independently of how the topic
is named*:

| `:consumer_type` | Use with | Model |
|---|---|---|
| `:classic` (default) | regular & partitioned topics | one consumer group per topic/partition |
| `:queue` | scalable topics | parallel work-queue over all segments |
| `:stream` | scalable topics | ordered, controller-assigned segments |

```elixir
# classic (default)
Pulsar.start_consumer(topic, sub, MyConsumer)

# scalable, queue model
Pulsar.start_consumer(topic, sub, MyConsumer, consumer_type: :queue)

# scalable, stream model
Pulsar.start_consumer(topic, sub, MyConsumer, consumer_type: :stream)
```

> #### Note {: .info}
> `:consumer_type` — not the topic name — decides the model. A scalable topic needs
> `consumer_type: :queue` or `:stream`; the default `:classic` will not consume it.

## Queue consumers

A **queue** consumer treats a scalable topic as a parallel work queue: it attaches to every
segment and acknowledges messages individually, with optional dead-letter support — the
scalable analogue of a `Shared` / `Key_Shared` subscription.

To parallelize, start several consumers on the same subscription and pick a
`subscription_type`; the broker spreads messages across them:

```elixir
Pulsar.start_consumer(topic, "workers", MyConsumer,
  consumer_type: :queue,
  subscription_type: :Key_Shared
)
```

## Stream consumers

A **stream** consumer is ordered. It registers with the broker's controller, which assigns
it a subset of the topic's key-range segments. Run several stream consumers on the same
subscription and the controller distributes the segments — and therefore the key ranges —
across them: parallel consumption with per-key ordering preserved, even as segments split
and merge.

```elixir
# run this on several nodes/processes with the same subscription
Pulsar.start_consumer(topic, "orders", MyConsumer, consumer_type: :stream)
```

> #### Note {: .info}
> `subscription_type` does not apply to stream consumers. Each segment is consumed by one
> member at a time, and parallelism comes from running more consumers (the controller hands
> each a slice of the key space), not from the subscription type.

## Subscription types

For `:classic` and `:queue` consumers, `:subscription_type` controls how the broker
dispatches messages within a subscription:

- `:Exclusive` — a single consumer
- `:Failover` — one active consumer, the rest on standby
- `:Shared` (default) — distributed across consumers
- `:Key_Shared` — like `:Shared`, but each key always goes to the same consumer

## Acknowledgement

By default each message is acknowledged **individually**. For ordered consumption on an
`:Exclusive` or `:Failover` subscription you can switch to **cumulative** acks, which
acknowledge a message and everything before it in one step:

```elixir
Pulsar.start_consumer(topic, sub, MyConsumer,
  subscription_type: :Exclusive,
  ack_type: :Cumulative
)
```

> #### Note {: .info}
> Cumulative acks are only valid on `:Exclusive` / `:Failover` subscriptions; the broker
> rejects them on `:Shared` / `:Key_Shared`.

## Consuming a not-yet-migrated topic

Because `:consumer_type` drives routing, you can point a `:queue` consumer at a regular
`persistent://` topic that has not been migrated to a scalable topic yet — the broker
presents it as a single legacy segment and the consumer drains it like any other. The same
scalable consumer code keeps working across a topic's migration.

## Configuration options

| Option | Type | Default | Description |
|---|---|---|---|
| `:consumer_type` | atom | `:classic` | `:classic`, `:queue`, or `:stream` |
| `:subscription_type` | atom | `:Shared` | `:Exclusive`, `:Failover`, `:Shared`, `:Key_Shared` (classic/queue only) |
| `:ack_type` | atom | `:Individual` | `:Individual` or `:Cumulative` (Exclusive/Failover only) |
| `:consumer_count` | integer | `1` | Consumers per group (classic/queue) |
| `:initial_position` | atom | `:latest` | `:earliest` or `:latest` |
| `:dead_letter_policy` | keyword | - | Dead-letter topic configuration |
| `:client` | atom | `:default` | Name of the client to use |
| `:name` | term | `"<topic>-<subscription>"` | Registered name for the consumer |

There are more options (flow control, chunking, schema, …) — see `Pulsar.start_consumer/4`.
