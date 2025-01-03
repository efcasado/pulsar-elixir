# Pulsar

> [!CAUTION]
> This project is a prototype in very early development and it is likely to be
> abandoned before it reaches maturity. Don't waste your time on it! ;)


`Pulsar` is an Elixir client for [Pulsar](https://pulsar.apache.org/).


## Usage

```elixir
config :pulsar,
  client_version: "My Pulsar Client v1.0.0",
  protocol_version: 21,
  ping_interval: 60_000,
  connections: [
    foo: [
      host: "pulsar://localhost:6650"
    ],
    bar: [
      host: "pulsar+ssl://localhost:6651",
      socket_opts: [verify: :none],
      conn_timeout: 5_000
    ],
    baz: [
      host: "pulsar+ssl://localhost:6651",
      socket_opts: [verify: :verify_none],
      conn_timeout: 5_000,
      auth: [
        type: :oauth2,
        settings: [
          client_id: "<YOUR-OAUTH2-CLIENT-ID>",
          client_secret: "<YOUR-OAUTH2-CLIENT-SECRET>",
          issuer_url: "<YOUR-OAUTH2-ISSUER-URL>",
          audience: "<YOUR-OAUTH2-AUDIENCE>"
        ]
      ]
    ]
  ]
```

