# Dead Letter Policies

## What is a Dead Letter Policy?

A [dead letter topic](https://pulsar.apache.org/docs/next/concepts-messaging/#dead-letter-topic) is where
messages go when a subscription has tried and failed to process them enough times. Without one, a message
your callback keeps rejecting is redelivered forever: it consumes flow permits on every attempt, and on an
ordered subscription it holds up everything behind it.

A dead letter policy sets the point at which the consumer stops retrying, publishes the message to another
topic, and acknowledges it. The subscription moves on, and the message is still there to inspect or replay.

For how callback results become ACKs and NACKs before dead lettering is involved, see
[Acknowledgement and Redelivery](acknowledgements.html).

## How Dead Lettering Works

### Consumer Side

Every delivery carries a redelivery count from the broker. When it reaches `:max_redelivery`, that delivery
is diverted to the dead letter topic instead of being handed to your callback:

```elixir
{:ok, consumer} = Pulsar.Consumer.start(
  "orders",
  "billing",
  MyConsumer,
  redelivery_interval: 5_000,
  dead_letter_policy: [max_redelivery: 3]
)
```

With `max_redelivery: 3`, a message your callback keeps rejecting is delivered three times. The fourth
delivery is diverted, and the callback never sees it:

1. **Deliveries 1 to 3**: `handle_message/2` or `handle_invalid_message/2` runs and returns
   `{:error, reason, state}`, so the message is negatively acknowledged and redelivered
2. **Delivery 4**: the count has reached the threshold, so the message is published to the dead letter topic
3. **Acknowledged**: once the publish succeeds, the message is acknowledged and leaves the subscription

Diverting replaces delivery rather than accompanying it, so neither `handle_message/2` nor
`handle_invalid_message/2` runs for a message that reaches the threshold.

### The Dead Letter Producer

Publishing needs a producer, and the consumer owns it:

- It is created with the consumer, whether or not a message ever fails
- It does not need the dead letter topic to be reachable at startup; discovery retries in the background
- A partitioned consumer has **one** producer for the whole subscription, not one per partition
- It is not registered, so it cannot be reached with `Pulsar.Producer.send/3` or stopped by name

## Configuration Options

> #### Warning {: .warning}
>
> A dead letter policy needs `:redelivery_interval` to be set. It is absent by default, and without it the
> library never asks the broker to redeliver a negatively acknowledged message. The redelivery count never
> grows, the threshold is never reached, and nothing is ever diverted.

### Consumer Configuration

```elixir
{Pulsar.Client,
 host: "pulsar://localhost:6650",
 consumers: [
   [topic: "orders",
    subscription_name: "billing",
    callback_module: MyConsumer,

    redelivery_interval: 5_000,        # Required for a dead letter policy to ever trigger

    dead_letter_policy: [
      max_redelivery: 3,               # Deliveries to attempt before diverting
      topic: "orders-parked"           # Optional; defaults to "<topic>-<subscription>-DLQ"
    ]
   ]
 ]}
```

#### Configuration Details

- **`max_redelivery`**: Required, a positive integer. How many times the callback sees a message before the
  next delivery is diverted. It is the point at which diverting starts, not a cap on how many times a message
  can be delivered.

- **`topic`**: Where to divert to. Defaults to `"<topic>-<subscription>-DLQ"`, so two subscriptions on the
  same topic get their own dead letter topics. Point several subscriptions at one topic by setting it
  explicitly; each still publishes through its own producer.

- **`redelivery_interval`**: Not part of the policy, but a policy does nothing without it. Milliseconds
  between redelivery requests for negatively acknowledged messages.

- **`producer`**: Options for the producer that publishes to the dead letter topic — `:compression`,
  `:batch_enabled`, `:schema` and the rest of `Pulsar.Producer`'s options. It takes their defaults
  otherwise. Its `:topic`, `:client` and `:name` come from the consumer and are rejected here.

```elixir
dead_letter_policy: [
  max_redelivery: 3,
  producer: [compression: :lz4, batch_enabled: false]
]
```

## What a Diverted Message Carries

A diverted message is republished rather than moved, so it is a new message on a new topic. What identifies
it is carried across:

| | Value |
| --- | --- |
| Payload | Unchanged |
| Key and ordering key | The origin's, so a `:key_shared` dead letter consumer partitions as the origin did |
| Properties | The origin's, plus the two below |
| Event time | The origin's |
| `REAL_TOPIC` | The topic the message was consumed from, spelled as the consumer was configured; the partition for a partitioned consumer |
| `ORIGIN_MESSAGE_ID` | The origin's id, as `Pulsar.Message.message_id_string/1` prints it |

The two property names are the ones the Java client uses, so a dead letter topic can be consumed by either
client and read the same way.

For an invalid message, “unchanged” means the bytes the consumer was able to retain, not necessarily
the payload originally published. A decompression failure carries compressed bytes, a malformed batch
carries its decoded entry framing, and an incomplete chunked message carries only the chunks that arrived.
These are useful for inspection but must not be treated as the original application payload.

Reading them back:

```elixir
def handle_message(%Pulsar.Message{} = message, state) do
  properties = Pulsar.Message.properties(message)

  Logger.warning(
    "parked message from #{properties["REAL_TOPIC"]} " <>
      "(origin id #{properties["ORIGIN_MESSAGE_ID"]})"
  )

  {:ok, state}
end
```

## Creating the Dead Letter Topic

A consumer can ask the broker to create its topic on subscribe — that is what `:force_create_topic`
does, and it defaults to `true`. **A producer cannot.** `CommandProducer` has no equivalent field, so
the asymmetry is in the Pulsar protocol rather than in this library.

The dead letter producer therefore relies on the broker's `allowTopicAutoCreation`, which is enabled by
default. Where it has been turned off, create the dead letter topic before diverting into it, either with
`pulsar-admin topics create` or by starting the consumer that reads it — subscribing creates it.

## Partitioned Topics

A partitioned consumer runs one worker per partition, but the dead letter topic belongs to the subscription
rather than the partition. Every partition of `orders` diverts into the same `orders-billing-DLQ`, and
`REAL_TOPIC` records which partition a message actually came from.

The dead letter topic may itself be partitioned. It is discovered like any other topic, and messages are
routed across its partitions honouring the key carried over from the origin.

## When the Dead Letter Topic Is Unavailable

A dead letter topic that cannot be published to — it does not exist and auto-creation is off, the broker is
unreachable, the payload exceeds its maximum message size — takes the consumer worker down:

- The refused message is not acknowledged, so the subscription still owes it
- The worker crashes, and its group restarts it and retries the whole delivery
- A dead letter topic that stays unavailable exhausts the restart budget, and the failure reaches whatever
  supervises the client

This is deliberate. Dropping the message or acknowledging it without it arriving anywhere both lose data, and
parking it quietly means a subscription that never drains and a problem nobody sees. Failing outright gets
past the transient case — a dead letter producer that is still starting up — on the retry, and surfaces the
persistent one instead of hiding it. `:worker_restart_intensity` is the first gate and
`:resource_restart_intensity` the second, both on `Pulsar.Client`.

What is redelivered depends on how the messages arrived. A batched entry is acknowledged only once all of
it lands, so the messages that *did* reach the dead letter topic are published again on every retry. Messages
that arrived on their own are acknowledged as each one is diverted, so only the refused one and those after
it come back. Either way dead lettering is at-least-once and a consumer of the dead letter topic has to
tolerate duplicates; the restart budgets bound how many.

## Example: Complete Dead Letter Flow

Odd numbers are rejected and end up on the dead letter topic; even numbers are processed normally.
Runs as-is against a broker on `localhost:6650`:

```elixir
Mix.install([:pulsar])

defmodule Orders do
  use Pulsar.Consumer.Callback

  # Odd payloads never succeed, so they exhaust the policy and are parked.
  def handle_message(%Pulsar.Message{payload: payload}, state) do
    case String.to_integer(payload) do
      n when rem(n, 2) == 0 ->
        IO.puts("processed #{n}")
        {:ok, state}

      n ->
        {:error, {:odd, n}, state}
    end
  end
end

defmodule Parked do
  use Pulsar.Consumer.Callback

  def handle_message(%Pulsar.Message{payload: payload} = message, state) do
    properties = Pulsar.Message.properties(message)

    IO.puts("parked #{payload} from #{properties["REAL_TOPIC"]} (#{properties["ORIGIN_MESSAGE_ID"]})")

    {:ok, state}
  end
end

{:ok, _client} = Pulsar.Client.start_link(host: "pulsar://localhost:6650")

# Subscribing creates the dead letter topic, so the producer never races the broker for it.
{:ok, parked} =
  Pulsar.Consumer.start("numbers-billing-DLQ", "inspection", Parked, initial_position: :earliest)

{:ok, orders} =
  Pulsar.Consumer.start("numbers", "billing", Orders,
    redelivery_interval: 500,
    dead_letter_policy: [max_redelivery: 3]
  )

:ok = Pulsar.Consumer.await_ready(orders)
:ok = Pulsar.Consumer.await_ready(parked)

{:ok, producer} = Pulsar.Producer.start("numbers")
:ok = Pulsar.Producer.await_ready(producer)

for n <- 1..6, do: {:ok, _id} = Pulsar.Producer.send(producer, Integer.to_string(n))

Process.sleep(5_000)
```

Each odd number is delivered to `handle_message/2` three times, then diverted on the fourth:

```
processed 2
processed 4
processed 6
parked 1 from numbers (3:0:-1)
parked 3 from numbers (3:2:-1)
parked 5 from numbers (3:4:-1)
```

`examples/odds_even_dlq.exs` in the repository is the same flow declared on the client rather than
started by hand, and exits once every odd number has been parked.

## Telemetry Events

The consumer reports each stage of giving up on a message. All four carry `%{count: n}` — one event
per delivery rather than per message — and `%{topic:, subscription_name:, consumer_id:}`:

| Event | When |
| --- | --- |
| `[:pulsar, :consumer, :message, :nacked]` | A callback returned `{:error, …}`, or `Pulsar.Consumer.nack/2` was called |
| `[:pulsar, :consumer, :redelivery, :requested]` | The redelivery interval asked the broker for the nacked messages back |
| `[:pulsar, :consumer, :dead_letter, :diverted]` | Messages reached the threshold and were published to the dead letter topic |

`:diverted` adds `:dead_letter_topic` and `:redelivery_count`.

`:requested` counts entries, where the others count messages. Against a batching producer three
nacked messages of one batch are one entry to ask for again, so the two counts do not line up.

`:nacked` reports that a callback rejected a message, whether or not anything will redeliver it. With no
`:redelivery_interval` configured you will see it with no `:redelivery, :requested` behind it and nothing
ever reaching the dead letter topic, which is the shape of the mistake the warning above describes.

A steady `:diverted` rate is the ordinary signal that a policy is doing its job; a rising one means
something upstream changed. A dead letter topic that cannot be published to has no event of its own,
because the worker does not survive it: the signal there is the consumer going down, with the
`[:pulsar, :producer, ...]` events below it explaining why.

The dead letter producer is otherwise an ordinary producer, so the `[:pulsar, :producer, …]` events
fire for it too, with `producer_name` set to `"<consumer name>-dead-letter-producer"`.
