# Acknowledgement and Redelivery

## What an Acknowledgement Means

A consumer reads through a subscription. Acknowledging a message tells the broker that the
subscription no longer owes that message; it does not delete the message from the topic or affect
another subscription.

The safe order is therefore:

1. Receive the message
2. Finish the side effect it represents
3. Acknowledge it

If the process stops before step 3, the broker can deliver the message again. If the side effect
finished but its acknowledgement did not reach the broker, the message can also be delivered
again. Pulsar consumption is at-least-once, so downstream work should be idempotent or deduplicated
even when every callback normally succeeds.

## The Callback Makes the Decision

`Pulsar.Consumer.Callback`'s `handle_message/2` callback returns what should happen to the current
message:

| Result | Message outcome | Worker outcome |
| --- | --- | --- |
| `{:ok, state}` | Acknowledge | Continue |
| `{:error, reason, state}` | Negatively acknowledge | Continue |
| `{:noreply, state}` | Neither; the application decides later | Continue |
| `{:stop, reason, state}` | Acknowledge | Finish with `reason` |

The same outcomes apply to `handle_invalid_message/2`. Its default implementation acknowledges
the invalid message so corrupt data is not redelivered forever.

Most consumers only need automatic acknowledgement:

```elixir
defmodule Billing do
  use Pulsar.Consumer.Callback

  def handle_message(%Pulsar.Message{payload: order}, state) do
    case MyApp.Billing.charge(order) do
      :ok -> {:ok, state}
      {:error, reason} -> {:error, reason, state}
    end
  end
end

{:ok, consumer} =
  Pulsar.Consumer.start("orders", "billing", Billing,
    redelivery_interval: 5_000
  )

:ok = Pulsar.Consumer.await_ready(consumer)
```

Here a successful charge is acknowledged immediately. A failed charge is marked for redelivery,
which the configured interval requests from the broker.

> #### A NACK does not schedule itself {: .warning}
>
> `:redelivery_interval` is absent by default. Without it, `{:error, ...}` and
> `Pulsar.Consumer.nack/2` report a NACK but no timer asks the broker to redeliver it. The message
> remains unacknowledged and normally returns only after the consumer reconnects or restarts.

## Manual Acknowledgement

Return `{:noreply, state}` when work continues outside the callback. Pass two opaque values to
that work:

- `self()`, the worker pid that received the message
- `message.message_id`, the broker message id

The process doing the work calls `Pulsar.Consumer.ack/2` or `Pulsar.Consumer.nack/2` when it knows
the outcome:

```elixir
defmodule AsyncBilling do
  use Pulsar.Consumer.Callback

  def handle_message(%Pulsar.Message{} = message, state) do
    worker = self()

    Task.Supervisor.start_child(MyApp.TaskSupervisor, fn ->
      case MyApp.Billing.charge(message.payload) do
        :ok -> Pulsar.Consumer.ack(worker, message.message_id)
        {:error, _reason} -> Pulsar.Consumer.nack(worker, message.message_id)
      end
    end)

    {:noreply, state}
  end
end
```

The pid is deliberately the short-lived worker, not the stable consumer root or its registered
name. An acknowledgement carries that worker's broker-side consumer id, so another worker cannot
answer for it.

Do not call `Pulsar.Consumer.ack(self(), ...)` synchronously from inside the callback. The callback
already runs in that GenServer, so it would call itself and exit with `:calling_self`. Capture the
pid and hand it to another process as above, or return `{:ok, state}` for automatic acknowledgement.

Treat `message.message_id` as opaque. A chunked message can carry several broker ids, and both
`ack/2` and `nack/2` accept that value directly. They also accept a list of ids when one call should
cover several messages.

## ACK Scope

There are two independent controls, not three mutually exclusive ACK modes:

- `:ack_type` chooses **individual** or **cumulative** scope
- `:batch_index_ack_enabled` chooses whether an ACK can identify messages within a batch

Together they produce four useful combinations:

| `ack_type` | Batch-index ACK | What is acknowledged |
| --- | --- | --- |
| `:individual` | `false` | Named messages; a batch is sent as one entry after all its messages are acknowledged |
| `:individual` | `true` | Named messages, tracked individually within their batch |
| `:cumulative` | `false` | Everything through the target entry; a partial batch stops at the preceding safe entry |
| `:cumulative` | `true` | Everything through the target message, including its position within a batch |

`ack_type: :individual` is the default and works with every subscription type. Cumulative
acknowledgement requires an `:exclusive` or `:failover` subscription, because `:shared` and
`:key_shared` subscriptions have no single cursor to move. Invalid combinations are refused when
the consumer starts.

### Individual ACKs

Individual acknowledgement is the safe general-purpose choice. One message can fail or finish
later without a successful sibling acknowledging it. It suits parallel and out-of-order work,
including `:shared` and `:key_shared` subscriptions.

Without batch-index acknowledgement, the client counts successful members of a batch locally and
acknowledges the entry once every member is done. Enable batch-index acknowledgement when a partly
processed batch should redeliver only the messages still outstanding.

### Cumulative ACKs

A cumulative ACK covers every message before its target as well as the target itself:

```elixir
{:ok, consumer} =
  Pulsar.Consumer.start("events", "projector", Projector,
    subscription_type: :exclusive,
    ack_type: :cumulative
  )
```

When `Pulsar.Consumer.ack/2` receives several ids, the client sends only the furthest cumulative
target. Automatic acknowledgement still happens after each callback that returns `{:ok, state}`;
use `{:noreply, state}` between checkpoints when the intended behavior is one ACK covering several
successfully processed messages:

```elixir
def handle_message(message, %{since_checkpoint: count} = state) do
  :ok = MyApp.Projector.apply_in_order(message)
  state = %{state | since_checkpoint: count + 1}

  if state.since_checkpoint == 100 do
    # This ACK also covers the preceding 99 messages.
    {:ok, %{state | since_checkpoint: 0}}
  else
    {:noreply, state}
  end
end
```

> #### A later cumulative ACK passes earlier failures {: .warning}
>
> Cumulative scope does not remember that an earlier message was deferred or NACKed. A later ACK
> acknowledges everything through its target, including that earlier message. Use cumulative
> acknowledgement only when processing is ordered and a later success proves the whole prefix is
> complete. Use individual acknowledgement when messages can finish or fail independently.

### Batch-Index ACKs

With `batch_index_ack_enabled: true`, the client sends an `ack_set` describing which messages of a
batch remain outstanding. Individual ACKs can clear separate indexes; cumulative ACKs clear the
whole prefix through their target. See [Batching](batching.html) for entry boundaries, redelivery,
and the effect on backlog metrics.

> #### Requires broker support, and cannot be detected {: .warning}
>
> The broker must set `acknowledgmentAtBatchIndexLevelEnabled=true`. Without it, the broker ignores
> the set and can acknowledge the whole entry, including messages the application has not
> processed. The protocol does not expose the setting, so the client cannot validate it.

## What a NACK Does

A NACK says that processing did not succeed; it does not undo callback state or immediately pull
the message back. With `:redelivery_interval` configured, the worker collects NACKed message ids.
On each interval it asks the broker to redeliver their entries and starts collecting again.

Redelivery is entry-based:

- An unbatched message is requested on its own
- NACKing several members of one batch produces one redelivery request for the entry
- Without batch-index state, previously acknowledged members of that batch can be delivered again

The broker increments the redelivery count when it sends the message again. A dead letter policy
can divert it after enough attempts; see [Dead Letter Policies](dead_letter_policies.html) for the
retry threshold, failure behavior, and telemetry.

## Batches and Chunks

The broker stores and acknowledges entries, while callbacks see logical messages. Usually those
are the same thing, but producer features can change the relationship:

- A **batch** puts several messages in one entry. Individual ACKs are counted off until the entry
  is complete, unless batch-index acknowledgement is enabled. See [Batching](batching.html).
- A **chunked message** spans several entries. Its `message.message_id` covers every chunk, so pass
  it through unchanged when acknowledging manually. See [Chunking](chunking.html).

These are why application code should not inspect, reconstruct, or persist assumptions about the
shape of a message id.

## Choosing a Configuration

| Workload | Recommended starting point |
| --- | --- |
| Independent or parallel processing | Default individual ACKs |
| Ordered processing with periodic checkpoints | Cumulative ACKs on `:exclusive` or `:failover` |
| Partial batches should redeliver narrowly | Add `batch_index_ack_enabled: true` after verifying broker support |
| Work finishes outside the callback | Return `{:noreply, state}` and pass the worker pid and message id to it |
| Failed work should retry | Set `:redelivery_interval` and return `{:error, ...}` or NACK manually |
| Repeated failures need parking | Add a dead letter policy as well as a redelivery interval |

Whatever the configuration, acknowledge only after the durable side effect is complete and make
that side effect safe to repeat. A worker or connection can fail between the side effect and its
ACK, and no ACK strategy can make that boundary exactly-once by itself.
