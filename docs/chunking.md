# Chunking

## What is Chunking?

[Chunking](https://pulsar.apache.org/docs/next/concepts-messaging/#chunking) is a feature in Apache Pulsar
that allows large messages to be split into smaller pieces (chunks) for transmission.
This is particularly useful when your messages exceed the broker's maximum message size limit
(typically 5MB by default), and you want to handle large payloads without hitting broker or network constraints.

When a producer sends a large message with chunking enabled, it automatically splits the message into multiple
chunks. The consumer then reassembles these chunks back into the original message before delivering it to the
application layer.

## How Chunking Works

### Producer Side

`Pulsar.Producer.start/2` adds a producer to a running client, so start one first — in your
supervision tree, or directly in a script:

```elixir
{:ok, _pid} = Pulsar.Client.start_link(host: "pulsar://localhost:6650")
```

When a producer is configured with chunking enabled:

```elixir
{:ok, producer} = Pulsar.Producer.start(
  "my-topic",
  chunking_enabled: true,
  max_message_size: 1024  # Split messages larger than 1KB
)
```

Large messages are automatically:
1. Compressed as a whole, when `:compression` is set
2. Split into chunks of `max_message_size` bytes, capped so that a chunk plus the metadata
   travelling with it stays inside the limit the broker advertised at connect time
3. Each chunk is assigned a unique UUID and sequence number
4. Chunks are sent to the broker individually
5. Each chunk consumes one flow control permit

Compression runs before the split, so a chunk carries a slice of the compressed message rather
than being compressed on its own. This is how the Java client frames chunks, so a compressed
chunked message can cross between the two. It also means a payload that compresses to under
`max_message_size` is sent whole and never chunked.

> #### `is_chunk` is not visible to consumers {: .info}
>
> The producer sets `is_chunk` on the `CommandSend` that carries each chunk, which tells the
> broker the entry is part of a larger message. It is a field of `CommandSend` only: the broker
> does not relay it, and the `CommandMessage` a consumer receives has no equivalent. A consumer
> recognises a chunk from the message metadata instead — a `uuid` together with a `chunk_id` —
> which is what `Pulsar.Message.chunked?/1` reports.

### Consumer Side

The consumer automatically handles chunk assembly:

1. **Chunk Reception**: Each chunk arrives as a separate broker message
2. **Buffering**: Chunks are buffered in memory until all chunks for a message arrive
3. **Assembly**: Once complete, chunks are reassembled into the original payload
4. **Delivery**: The complete message is delivered to your `handle_message/2` callback

```elixir
def handle_message(%Pulsar.Message{} = message, state) do
  # message.payload contains the complete, reassembled payload
  # message.chunk_metadata indicates if this was a chunked message

  case message.chunk_metadata do
    %{chunked: true, complete: true, num_chunks: n} ->
      IO.puts("Received complete chunked message with #{n} chunks")

    %{chunked: true, complete: false, error: reason} ->
      IO.puts("Received incomplete chunked message: #{reason}")

    nil ->
      IO.puts("Received non-chunked message")
  end

  {:ok, state}
end
```

## Chunked Message Metadata

The `Pulsar.Message` struct provides information about chunked messages:

- `chunk_metadata` - Contains chunking information:
  - `chunked: true` - Indicates this was a chunked message
  - `complete: true|false` - Whether all chunks were received
  - `uuid` - Unique identifier for the chunked message
  - `num_chunks` - Total number of chunks (for complete messages)
  - `received_chunks` - Number of chunks received (for incomplete messages)
  - `error` - Reason for incompleteness (if incomplete)

A chunked message is assembled before the callback sees it, so `payload` is the complete
payload and `message_id` covers every chunk: acknowledging it acknowledges them all.

`Pulsar.Message`'s accessors — `producer_name/1`, `key/1`, `properties/1` and the rest — answer
the same way for a chunked message as for any other. Only the `raw` field reflects the split,
holding a list of protocol structs per chunk rather than a single one.

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
   [topic: "my-topic",
    name: :my_producer,
    chunking_enabled: true,        # Enable chunking (default: false)
    max_message_size: 1024 * 1024  # Split messages larger than 1MB (default: 5MB)
   ]
 ]}
```

### Consumer Configuration

```elixir
{Pulsar.Client,
 host: "pulsar://localhost:6650",
 consumers: [
   [topic: "my-topic",
    subscription_name: "my-sub",
    callback_module: MyConsumer,

    # Chunking-related options:
    max_pending_chunked_messages: 10,                        # Max concurrent chunked messages (default: 10)
    expire_incomplete_chunked_message_after: 60_000,         # Timeout in ms (default: 60s)
    chunk_cleanup_interval: 30_000                           # Cleanup check interval in ms (default: 30s)
   ]
 ]}
```

#### Configuration Details

- **`max_pending_chunked_messages`**: Maximum number of incomplete chunked messages to buffer simultaneously. If this limit is reached and a new chunked message arrives, the oldest incomplete message is evicted and delivered as incomplete with `error: :queue_full`.

- **`expire_incomplete_chunked_message_after`**: How long to wait for all chunks before timing out. Expired messages are delivered as incomplete with `error: :expired`.

- **`chunk_cleanup_interval`**: How often to check for and clean up expired chunked messages. Set to `false` to disable automatic cleanup (not recommended for production); `nil` is accepted as an alias. Keep it below `expire_incomplete_chunked_message_after`, since an expired chunk is only released on the next sweep.

## Handling Incomplete Chunks

Chunks may not complete for several reasons:

1. **Expiration**: Not all chunks arrived within the timeout period
2. **Queue overflow**: Too many concurrent chunked messages

Incomplete chunks are delivered to your callback with `complete: false`. Their `payload` is
whatever chunks did arrive, concatenated, and under `:compression` those are still compressed:
a message only decompresses once all of its chunks are back together, so a partial one cannot
be decompressed at all. Treat the payload of an incomplete message as opaque.

```elixir
def handle_message(%Pulsar.Message{chunk_metadata: %{complete: false, error: reason, received_chunks: n}}, state) do
  Logger.warning("Incomplete chunk: #{reason}, received #{n} chunks")

  # Return error to trigger redelivery
  {:error, :incomplete_chunk, state}
end
```

## Flow Control and Permits

Flow control permits are only decremented when messages are assembled and delivered to your callback:

- **Individual chunks arriving**: No permits are decremented yet
- **Chunked message completed**: Decrements N permits (where N = number of chunks)
- **Chunked message expired/evicted**: Decrements M permits (where M = number of chunks received)
- **Non-chunked message**: Decrements 1 permit

The `Pulsar.Message.num_broker_messages/1` helper returns the correct permit count:

```elixir
# Non-chunked message
Pulsar.Message.num_broker_messages(message) # => 1

# Complete chunked message with 3 chunks
Pulsar.Message.num_broker_messages(message) # => 3

# Incomplete chunked message with 2 out of 3 chunks received
Pulsar.Message.num_broker_messages(message) # => 2
```

This ensures that flow control accurately reflects the number of broker messages consumed, regardless of whether messages are chunked or not.

## Helper Functions

The `Pulsar.Message` module provides helpers for working with chunked messages:

```elixir
# Check if message is chunked
Pulsar.Message.chunked?(message) # => true for chunked, false otherwise

# Check if chunked message is complete
Pulsar.Message.complete?(message) # => true if complete, false if incomplete

# Get maximum redelivery count (max across all chunks for chunked messages)
redelivery_count = Pulsar.Message.redelivery_count(message)

# Get number of broker messages consumed (for flow control)
num_permits = Pulsar.Message.num_broker_messages(message)
```

## Example: Complete Chunked Message Flow

```elixir
{:ok, _pid} = Pulsar.Client.start_link(host: "pulsar://localhost:6650")

# Producer sends large message
{:ok, producer} = Pulsar.Producer.start(
  "large-files",
  chunking_enabled: true,
  max_message_size: 1024 * 1024  # 1MB chunks
)

# Send 5MB file
large_payload = File.read!("large_file.dat")  # 5MB
{:ok, _msg_id} = Pulsar.Producer.send(producer, large_payload)
# Producer automatically splits into 5 chunks

# Consumer receives and assembles
defmodule MyConsumer do
  use Pulsar.Consumer.Callback

  def handle_message(%Pulsar.Message{} = message, state) do
    if Pulsar.Message.chunked?(message) and Pulsar.Message.complete?(message) do
      # message.payload contains complete 5MB file
      num_chunks = message.chunk_metadata.num_chunks
      IO.puts("Received complete file in #{num_chunks} chunks")
      process_file(message.payload)
      {:ok, state}
    else
      # Regular non-chunked message or incomplete chunked message
      {:ok, state}
    end
  end
end
```

## Telemetry Events

The consumer emits telemetry events for chunk lifecycle:

| Event | Measurements | When |
| --- | --- | --- |
| `[:pulsar, :consumer, :chunk, :received]` | `chunk_id`, `num_chunks` | A chunk arrives |
| `[:pulsar, :consumer, :chunk, :complete]` | `num_chunks`, `total_size`, `age_ms` | All chunks are assembled |
| `[:pulsar, :consumer, :chunk, :discarded]` | `received_chunks`, `num_chunks` | A chunked message is evicted |
| `[:pulsar, :consumer, :chunk, :expired]` | `age_ms`, `received_chunks`, `num_chunks` | A chunked message times out |

Each of these carries `uuid` alongside the `topic`, `base_topic`, `partition`,
`subscription_name` and `consumer_id` that every consumer event carries, and the two that give
up on a message also carry `reason`.
`received_chunks` against `num_chunks` says how much of the message had arrived before it was
dropped, and `age_ms` on `:complete` is how long assembly took, which is what
`:expire_incomplete_chunked_message_after` should be set against.

The producer emits `[:pulsar, :producer, :chunk, :start]`, `:sent` and `:complete`, all carrying
`uuid` alongside the `topic`, `base_topic`, `partition`, `producer_id` and `producer_name` that
every producer event carries. Their `total_size` and
`chunk_size` count the bytes actually sent, so with `:compression` set they describe the
compressed message rather than the payload handed to `Pulsar.Producer.send/3`. On the consumer
side `:complete`'s `total_size` is the reassembled message after decompression, so the two do
not line up when compression is on.

`:topic` names a single partition and `:base_topic` the topic it belongs to, so one set of
events both aggregates over a partitioned topic and breaks down by partition. They are equal,
and `:partition` is `nil`, when the topic is not partitioned.

See the Telemetry documentation for more details on monitoring chunked messages.
