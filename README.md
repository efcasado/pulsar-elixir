# Elixir Client for Apache Pulsar

[![CI](https://github.com/efcasado/pulsar-elixir/actions/workflows/ci.yml/badge.svg)](https://github.com/efcasado/pulsar-elixir/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/efcasado/pulsar-elixir/badge.svg?branch=main)](https://coveralls.io/github/efcasado/pulsar-elixir?branch=main)
[![Package Version](https://img.shields.io/hexpm/v/pulsar_elixir.svg)](https://hex.pm/packages/pulsar_elixir)
[![hexdocs.pm](https://img.shields.io/badge/hex-docs-purple.svg)](https://hexdocs.pm/pulsar_elixir/)


> [!TIP]
> Using [Broadway](https://github.com/dashbitco/broadway)? Check out the companion project: [off_broadway_pulsar](https://github.com/efcasado/off_broadway_pulsar).

An Elixir client for [Apache Pulsar](https://pulsar.apache.org/).


## Installation

Add `:pulsar_elixir` to your dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:pulsar, "~> 2.11.1", hex: :pulsar_elixir} <!-- x-release-please-version -->
  ]
end
```


## Quick Start

Assuming you have Pulsar running on `localhost:6650`, the quickest way to consume messages
from a Pulsar topic is using the Reader interface, reading through a client:

```elixir
{:ok, _pid} = Pulsar.Client.start_link(host: "pulsar://localhost:6650")

"persistent://my-tenant/my-namespace/my-topic"
|> Pulsar.Reader.stream(timeout: 100)
|> Enum.map(fn msg -> String.to_integer(msg.payload) end)
|> Enum.filter(fn n -> rem(n, 2) == 0 end)
|> Enum.map(fn n -> n * 2 end)
```

For more complex scenarios and assuming that you have implemented a basic consumer like the
one below:

```elixir
defmodule MyPulsarConsumer do
  use Pulsar.Consumer.Callback

  def handle_message(message, state) do
    IO.puts("Received: #{message.payload}")
    {:ok, state}
  end
end
```

Put a client in your supervision tree and declare its consumers and producers on it:

```elixir
children = [
  {Pulsar.Client,
   host: "pulsar://localhost:6650",
   producers: [
     [topic: "persistent://my-tenant/my-namespace/my-topic", name: :my_producer]
   ],
   consumers: [
     [topic: "persistent://my-tenant/my-namespace/my-topic",
      subscription_name: "my-subscription",
      callback_module: MyPulsarConsumer]
   ]}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

The client is the only thing your tree holds; consumers and producers run under it. Sets
only known at runtime are added with `Pulsar.Consumer.start/1` and `Pulsar.Producer.start/1`.
Resource initialization is asynchronous, so operations may temporarily return
`{:error, :not_ready}`. Call `Pulsar.Consumer.await_ready/2` or
`Pulsar.Producer.await_ready/2` when work must wait for initial topic discovery:

```elixir
:ok = Pulsar.Producer.await_ready(:my_producer, timeout: 10_000)
```

Sending a message using the configured producer can be done as follows:

```elixir
Pulsar.Producer.send(:my_producer, "Hello, Pulsar!")
```

In a script or an IEx session, start the client directly and add to it as you go:

```elixir
{:ok, _pid} = Pulsar.Client.start_link(host: "pulsar://localhost:6650")
{:ok, _pid} = Pulsar.Producer.start(topic: "persistent://public/default/t", name: :p)
```

Brokers, consumers and producers belong to the `:default` client unless told otherwise.
Several clients can coexist, which is useful when connecting to more than one cluster:

```elixir
children = [
  {Pulsar.Client, name: :client_1, host: "pulsar://host.cluster1.com:6650"},
  {Pulsar.Client, name: :client_2, host: "pulsar://host.cluster2.com:6650"}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

A consumer or producer added at runtime selects its client with `:client`:

```elixir
Pulsar.Producer.start(
  client: :client_1,
  topic: "persistent://my-tenant/my-namespace/my-topic",
  name: :my_producer_1
)
```

See the [architecture guide](https://hexdocs.pm/pulsar_elixir/architecture.html) for ownership,
resource lifecycle, and recovery details.

If your Pulsar cluster requires authentication, you can configure it in the client
using the `auth` key:

```elixir
auth: [
  type: Pulsar.Auth.OAuth2,
  opts: [
    client_id: "<YOUR-OAUTH2-CLIENT-ID>",
    client_secret: "<YOUR-OAUTH2-CLIENT-SECRET>",
    site: "<YOUR-OAUTH2-ISSUER-URL>",
    audience: "<YOUR-OAUTH2-AUDIENCE>"
  ]
]
```


## Testing

> [!IMPORTANT]
> Do not forget to add the following line to your `/etc/hosts` file before running the tests:
>
> ```
> 127.0.0.1 broker1 broker2
> ```

To run the tests, run the following command:

```
mix test
```

If you want to run only a subset of tests, specify the file including the tests you want to run

```
mix test test/integration/consumer_test.exs
```

You can also run individual tests by passing the line number where they are defined

```
mix test test/integration/consumer_test.exs:43
```

The `examples` directory includes a number of examples that demonstrate the use of the Pulsar client.
For example:

```
mix run examples/bingo.exs
```


## Features

The full feature matrix for Apache Pulsar can be found [here](https://pulsar.apache.org/client-feature-matrix/).

| Component | Feature                            | Supported |
|-----------|------------------------------------|-----------|
| Client    | TLS encryption                     | ✅        |
| Client    | Authentication                     | ⚠️        |
| Client    | Transaction                        | ❌        |
| Client    | Statistics                         | ❌        |
| Producer  | Sync send                          | ✅        |
| Producer  | Async send                         | ❌        |
| Producer  | Batching                           | ✅        |
| Producer  | Chunking                           | ✅        |
| Producer  | Compression                        | ✅        |
| Producer  | Schema                             | ✅        |
| Producer  | Partitioned topics                 | ✅        |
| Producer  | Access modes                       | ✅        |
| Consumer  | ACK                                | ✅        |
| Consumer  | Batch-index ACK                    | ✅        |
| Consumer  | NACK                               | ✅        |
| Consumer  | NACK back-off                      | ❌        |
| Consumer  | Batching                           | ✅        |
| Consumer  | Partitioned topics                 | ✅        |
| Consumer  | Chunking                           | ✅        |
| Consumer  | Seek                               | ✅        |
| Consumer  | Subscription types                 | ✅        |
| Consumer  | Subscription modes                 | ✅        |
| Consumer  | Retry letter topic                 | ❌        |
| Consumer  | Dead letter topic                  | ✅        |
| Consumer  | Compression                        | ✅        |
| Consumer  | Compaction                         | ✅        |
| Consumer  | Schema                             | ✅        |
| Consumer  | Configurable flow control settings | ✅        |
| Reader    |                                    | ✅        |
| TableView |                                    | ❌        |
