# Elixir Client for Apache Pulsar

> [!CAUTION]
> This project is a prototype in very early development and it is likely to be
> abandoned before it reaches maturity. Don't waste your time on it! ;)

An Elixir client for [Apache Pulsar](https://pulsar.apache.org/).


## Usage

The package can be installed by adding `pulsar` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:pulsar, git: "https://github.com/efcasado/pulsar-elixir"}]
end
```

You can configure the client by adding the following configuration to your `config/config.exs`:

```elixir
config :pulsar,
  host: "pulsar://localhost:6650",
  socket_opts: [verify: :none],
  auth: [
    type: Pulsar.Auth.OAuth2
    settings: [
        client_id: "<YOUR-OAUTH2-CLIENT-ID>",
        client_secret: "<YOUR-OAUTH2-CLIENT-SECRET>",
        site: "<YOUR-OAUTH2-ISSUER-URL>",
        audience: "<YOUR-OAUTH2-AUDIENCE>"
    ]
  ],
  consumers: [
    my_consumer: [
		topic: "persistent://my-tenant/my-namespace/my-topic",
        subscription_name: "my-app-my-consumer-subscription",
        subscription_type: "Exclusive",
		callback_module: MyApp.MyConsumer
    ]
  ]
```

Alternatively, you can start the Pulsar client on demand and add it to your application's supervisor
by calling `Pulsar.start/1` directly, as follows:

```elixir
{:ok, pid} = Pulsar.Application.start(
  host: "pulsar://localhost:6650",
  consumers: [
    {:my_consumer, [
	  topic: "my-topic",
	  subscription_name: "my-subscription",
	  subscription_type: :Shared,
	  callback: MyConsumerCallback
	]}
  ]
)
```
