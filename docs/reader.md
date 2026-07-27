# Reader

## What is a Reader?

A Reader is a high-level interface for reading messages from Pulsar topics using Elixir's [Stream](https://hexdocs.pm/elixir/Stream.html) abstraction. Unlike Consumers, which are callback-based and designed for continuous message processing with persistent subscriptions, readers are designed for:

- **Batch processing**: Reading a sequence of messages and stopping
- **Stream pipelines**: Transforming and filtering data using Elixir's functional `Enum` and `Stream` modules
- **Replay**: Reading messages from a specific position (e.g., from the beginning or a specific message ID)
- **One-off tasks**: Scripts or jobs that need to consume data without setting up a full Consumer supervision tree

Readers use **non-durable subscriptions**, meaning they don't persist their position on the broker. Each time you start a Reader, you specify where to start reading from.

## Basic Usage

A Reader reads through a client, which belongs in your supervision tree:

```elixir
children = [{Pulsar.Client, host: "pulsar://localhost:6650"}]

"persistent://public/default/my-topic"
|> Pulsar.Reader.stream()
|> Stream.map(fn msg -> msg.payload end)
|> Enum.take(10)
```

This creates a stream that:
1. Subscribes through the client
2. Reads 10 messages from the topic (starting from `:earliest` by default)
3. Extracts the payload
4. Unsubscribes when done, leaving the client running

> #### Note {: .info}
>
> The Reader stream is bound to the process that creates it.
>
> Messages are delivered to the creating process's mailbox. You cannot create a stream in one process and pass it to another for consumption. If you need concurrent consumption:
>
> 1. Create multiple streams in separate processes (e.g., inside `Task.async`)
> 2. Use partitioned topics (the Reader handles them automatically, merging partitions into a single stream)

## Choosing a client

Streams read through the `:default` client unless told otherwise, so a single-client
application needs to say nothing. Name a client to read through a different cluster:

```elixir
children = [
  {Pulsar.Client, name: :analytics, host: "pulsar://analytics:6650"},
  {Pulsar.Client, name: :events, host: "pulsar://events:6650"}
]

Pulsar.Reader.stream(topic, client: :analytics)
```

The client outlives the stream, so several streams can share one connection.

### Error Handling
If initialization fails (e.g., invalid topic, connection error), the stream emits `{:error, reason}` as its first and only element:

```elixir
topic
|> Pulsar.Reader.stream(client: :not_running)
|> Enum.take(1)
|> case do
  [{:error, reason}] -> Logger.error("Failed: #{inspect(reason)}")
  messages -> process(messages)
end
```

## Flow Control

The Reader manages flow control internally. You can configure the number of permits (messages requested from the broker) using `:flow_permits`:

```elixir
# Request 50 messages at a time (default: 100)
Pulsar.Reader.stream(topic, flow_permits: 50)
```

For most use cases, the default is fine. Adjust this if you're processing very large messages or want finer-grained control over memory usage.

## Configuration Options

See `Pulsar.Reader.stream/2`, whose option list is generated from the schema it
validates against, so the two cannot disagree.
