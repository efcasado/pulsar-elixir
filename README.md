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
    {:pulsar, "~> 2.5.1", hex: :pulsar_elixir}
  ]
end
```


## Quick Start

Assuming you have Pulsar running on `localhost:6650` and that you have implemented a basic
consumer like the one below:

```elixir
defmodule MyPulsarConsumer do
  use Pulsar.Consumer.Callback

  def handle_message(message, state) do
    IO.puts("Received: #{message.payload}")
    {:ok, state}
  end
end
```

You can start producing and consuming messages with the following configuration:

```elixir
config :pulsar,
  host: "pulsar://localhost:6650",
  consumers: [
    my_consumer: [
        topic: "persistent://my-tenant/my-namespace/my-topic",
        subscription_name: "my-subscription",
        callback_module: MyPulsarConsumer
    ]
  ],
  producers: [
    my_producer: [
        topic: "persistent://my-tenant/my-namespace/my-topic"
    ]
  ]
```

Sending a message using the configured producer can be done as follows:

```elixir
Pulsar.send(:my_producer, "Hello, Pulsar!")
```

By default, brokers, consumers and producers are started within the scope of the
`:default` client, but you can also configure multiple clients (which may come in
handy if you need to connect to multiple clusters).

```elixir
clients: [
  client_1: [
      host: "pulsar://host.cluster1.com:6650"
  ],
  client_2: [
      host: "pulsar://host.cluster2.com:6650"
  ]
]
```

Then, you can specify the client in the consumer or producer configuration using
the `client` key, eg. `client: :client_1`.

```elixir
producers: [
  my_producer_1: [
    client: :client_1
    topic: "persistent://my-tenant/my-namespace/my-topic"
  ]
]
```

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
mix run --no-start examples/bingo.exs
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
| Producer  | Batching                           | ❌        |
| Producer  | Chunking                           | ✅        |
| Producer  | Compression                        | ✅        |
| Producer  | Schema                             | ❌        |
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
| Consumer  | Schema                             | ❌        |
| Consumer  | Configurable flow control settings | ✅        |
| Reader    |                                    | ❌        |
| TableView |                                    | ❌        |
