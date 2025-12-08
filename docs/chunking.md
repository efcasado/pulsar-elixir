# Chunking

## What is Chunking?

Chunking is a feature in Apache Pulsar that allows large messages to be split into smaller pieces (chunks) for transmission. This is particularly useful when:

- Your messages exceed the broker's maximum message size limit (typically 5MB by default)
- You want to handle large payloads without hitting broker or network constraints
- You need to send messages that are larger than what can fit in a single network frame

When a producer sends a large message with chunking enabled, it automatically splits the message into multiple chunks. The consumer then reassembles these chunks back into the original message before delivering it to your callback.

## How Chunking Works

### Producer Side

When a producer is configured with chunking enabled:

```elixir
{:ok, producer} = Pulsar.start_producer(
  "my-topic",
  chunking_enabled: true,
  max_message_size: 1024  # Split messages larger than 1KB
)
```

Large messages are automatically:
1. Split into chunks of `max_message_size` bytes
2. Each chunk is assigned a unique UUID and sequence number
3. Chunks are sent to the broker individually
4. Each chunk consumes one flow control permit

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

For chunked messages, some fields contain data from all chunks:
- `command` - List of commands from all chunks
- `metadata` - List of metadata from all chunks
- `broker_metadata` - List of broker metadata from all chunks
- `message_id_to_ack` - List of all chunk message IDs

For non-chunked messages, these fields contain single values.

## Configuration Options

> #### Warning {: .warning}
>
> Batching and chunking cannot be enabled simultaneously on a producer. When `chunking_enabled: true` is set, batching is automatically disabled. These features are mutually exclusive because they represent different strategies for message transmission.

### Producer Configuration

```elixir
producers: [
  my_producer: [
    topic: "my-topic",
    chunking_enabled: true,        # Enable chunking (default: false)
    max_message_size: 1024 * 1024  # Chunk size in bytes (default: 5MB)
  ]
]
```

### Consumer Configuration

```elixir
consumers: [
  my_consumer: [
    topic: "my-topic",
    subscription_name: "my-sub",
    callback_module: MyConsumer,

    # Chunking-related options:
    max_pending_chunked_messages: 10,                        # Max concurrent chunked messages (default: 10)
    expire_incomplete_chunked_message_after: 60_000,         # Timeout in ms (default: 60s)
    chunk_cleanup_interval: 30_000                           # Cleanup check interval in ms (default: 30s)
  ]
]
```

#### Configuration Details

- **`max_pending_chunked_messages`**: Maximum number of incomplete chunked messages to buffer simultaneously. If this limit is reached and a new chunked message arrives, the oldest incomplete message is evicted and delivered as incomplete with `error: :queue_full`.

- **`expire_incomplete_chunked_message_after`**: How long to wait for all chunks before timing out. Expired messages are delivered as incomplete with `error: :expired`.

- **`chunk_cleanup_interval`**: How often to check for and clean up expired chunked messages. Set to `nil` to disable automatic cleanup (not recommended for production).

## Handling Incomplete Chunks

Chunks may not complete for several reasons:

1. **Expiration**: Not all chunks arrived within the timeout period
2. **Queue overflow**: Too many concurrent chunked messages

Incomplete chunks are delivered to your callback with `complete: false`:

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

## Best Practices

1. **Enable chunking on producer** only if you expect to send large messages
2. **Set appropriate timeouts** based on your network latency and message size
3. **Monitor incomplete chunks** - frequent incomplete chunks may indicate network issues
4. **Handle incomplete chunks** explicitly in your consumer callback
5. **Adjust `max_pending_chunked_messages`** based on expected concurrency
6. **Consider message size** - very large messages consume more memory during assembly

## Example: Complete Chunked Message Flow

```elixir
# Producer sends large message
{:ok, producer} = Pulsar.start_producer(
  "large-files",
  chunking_enabled: true,
  max_message_size: 1024 * 1024  # 1MB chunks
)

# Send 5MB file
large_payload = File.read!("large_file.dat")  # 5MB
{:ok, _msg_id} = Pulsar.send(producer, large_payload)
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

- `[:pulsar, :consumer, :chunk, :received]` - When a chunk is received
- `[:pulsar, :consumer, :chunk, :complete]` - When all chunks are assembled
- `[:pulsar, :consumer, :chunk, :discarded]` - When a chunked message is discarded
- `[:pulsar, :consumer, :chunk, :expired]` - When a chunked message expires

See the Telemetry documentation for more details on monitoring chunked messages.
