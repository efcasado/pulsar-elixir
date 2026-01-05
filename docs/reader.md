# Reader

## What is a Reader?

A Reader is a high-level interface for reading messages from Pulsar topics using Elixir's [Stream](https://hexdocs.pm/elixir/Stream.html) abstraction. Unlike Consumers, which are callback-based and designed for continuous message processing with persistent subscriptions, readers are designed for:

- **Batch processing**: Reading a sequence of messages and stopping
- **Stream pipelines**: Transforming and filtering data using Elixir's functional `Enum` and `Stream` modules
- **Replay**: Reading messages from a specific position (e.g., from the beginning or a specific message ID)
- **One-off tasks**: Scripts or jobs that need to consume data without setting up a full Consumer supervision tree

Readers use **non-durable subscriptions**, meaning they don't persist their position on the broker. Each time you start a Reader, you specify where to start reading from.

## Basic Usage

The simplest way to use a Reader is to stream messages from a topic using an internal client:

```elixir
{:ok, stream} = Pulsar.Reader.stream("persistent://public/default/my-topic",
  host: "pulsar://localhost:6650"
)

stream
|> Stream.map(fn msg -> msg.payload end)
|> Enum.take(10)
```

This creates a stream that:
1. Connects to the Pulsar broker
2. Reads 10 messages from the topic (starting from `:earliest` by default)
3. Extracts the payload
4. Automatically closes the connection when done

### Error Handling

`Pulsar.Reader.stream/2` returns `{:ok, stream}` on success or `{:error, reason}` if initialization fails:

```elixir
case Pulsar.Reader.stream(topic, host: "pulsar://localhost:6650") do
  {:ok, stream} ->
    Enum.take(stream, 10)

  {:error, reason} ->
    Logger.error("Failed to start reader: #{inspect(reason)}")
end
```

> #### Note {: .info}
>
> The Reader stream is bound to the process that creates it.
>
> Messages are delivered to the creating process's mailbox. You cannot create a stream in one process and pass it to another for consumption. If you need concurrent consumption:
>
> 1. Create multiple streams in separate processes (e.g., inside `Task.async`)
> 2. Use partitioned topics (the Reader handles them automatically, merging partitions into a single stream)

## Connection Management

You can choose between two connection modes:

### Internal Client (Host Mode)
Provide a `:host` URL, and the stream will manage its own temporary Pulsar client. The client is started when the stream begins and stopped when it terminates.

```elixir
{:ok, stream} = Pulsar.Reader.stream(topic, host: "pulsar://localhost:6650")
```

### External Client (Client Mode)
Use an existing Pulsar client from your application's supervision tree. This is more efficient if you're running multiple streams or already have a client connection.

```elixir
# In application.ex
children = [
  {Pulsar, host: "pulsar://localhost:6650", name: :my_app_client}
]

# In your code
{:ok, stream} = Pulsar.Reader.stream(topic, client: :my_app_client)
```

## Start Positions

You can control where the Reader starts consuming messages:

### From Earliest/Latest
```elixir
# Start from the oldest available message (default)
{:ok, stream} = Pulsar.Reader.stream(topic, start_position: :earliest)

# Start only with new messages published after the reader starts
{:ok, stream} = Pulsar.Reader.stream(topic, start_position: :latest)
```

### From Specific Message ID
Resume reading from a specific message (inclusive):

```elixir
message_id = {ledger_id, entry_id} # e.g. {123, 456}

{:ok, stream} = Pulsar.Reader.stream(topic, start_message_id: message_id)
```

### From Timestamp
Read messages published at or after a specific timestamp (Unix timestamp in milliseconds):

```elixir
timestamp = :os.system_time(:millisecond) - 3600_000 # 1 hour ago

{:ok, stream} = Pulsar.Reader.stream(topic, start_timestamp: timestamp)
```

## Stream Processing Examples

### Filter and Map
Read messages, filter for interesting ones, and transform them:

```elixir
{:ok, stream} = Pulsar.Reader.stream(topic, client: :default)

stream
|> Stream.map(fn msg -> Jason.decode!(msg.payload) end)
|> Stream.filter(fn event -> event["type"] == "user_signup" end)
|> Stream.map(fn event -> event["user_id"] end)
|> Enum.each(&IO.inspect/1)
```

### Batch Processing
Process messages in chunks using `Stream.chunk_every/2`:

```elixir
{:ok, stream} = Pulsar.Reader.stream(topic, client: :default)

stream
|> Stream.chunk_every(100)
|> Enum.each(fn batch ->
  # Insert batch of 100 messages into database
  Repo.insert_all(User, batch)
end)
```

### Timeout Handling
By default, the stream waits up to 60 seconds for new messages before terminating. You can adjust this with `:timeout`:

```elixir
{:ok, stream} = Pulsar.Reader.stream(topic, client: :default, timeout: 5000) # 5s timeout
Enum.to_list(stream)
```

## Flow Control

The Reader manages flow control internally. You can configure the number of permits (messages requested from the broker) using `:flow_permits`:

```elixir
# Request 50 messages at a time (default: 100)
{:ok, stream} = Pulsar.Reader.stream(topic, flow_permits: 50)
```

For most use cases, the default is fine. Adjust this if you're processing very large messages or want finer-grained control over memory usage.

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:host` | string | - | Pulsar broker URL (mutually exclusive with `:client`) |
| `:name` | atom | `:default` | Name for the internal client (only used with `:host`) |
| `:auth` | tuple | - | Authentication configuration (only used with `:host`) |
| `:socket_opts` | list | - | Socket options (only used with `:host`) |
| `:client` | atom | `:default` | Name of existing client (mutually exclusive with `:host`) |
| `:start_position` | atom | `:earliest` | `:earliest` or `:latest` |
| `:start_message_id` | tuple | - | `{ledger_id, entry_id}` tuple to start from |
| `:start_timestamp` | integer | - | Unix timestamp (ms) to start from |
| `:flow_permits` | integer | 100 | Number of messages to request per flow batch |
| `:timeout` | integer | 60_000 | Inactivity timeout in milliseconds |
| `:read_compacted` | boolean | `false` | Read only latest value for each key (compacted topics) |
