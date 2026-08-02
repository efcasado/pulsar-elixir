defmodule Pulsar do
  @moduledoc """
  An Apache Pulsar client for Elixir.

  The core API is centered on four modules:

  - `Pulsar.Client` — a connection context for one Pulsar cluster
  - `Pulsar.Consumer` — subscribes to a topic and dispatches to a callback module
  - `Pulsar.Producer` — publishes to a topic
  - `Pulsar.Reader` — exposes stream-based, non-durable reading

  ## Getting started

  A client belongs in the host application's supervision tree, and everything else is
  declared on it:

      children = [
        {Pulsar.Client,
         host: "pulsar://localhost:6650",
         producers: [
           [topic: "persistent://public/default/audit", name: :audit]
         ],
         consumers: [
           [topic: "persistent://public/default/orders",
            subscription_name: "order-service",
            callback_module: MyApp.OrderHandler]
         ]}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  Then publish with `Pulsar.Producer.send/3`:

      {:ok, message_id} = Pulsar.Producer.send(:audit, "payload")

  The client is the only thing your tree holds. See `Pulsar.Client` for what that buys, and
  for adding consumers and producers to a running client.

  ## Scripts and IEx

  A client started directly is all a script needs:

      Mix.install([:pulsar_elixir])

      defmodule Tail do
        use Pulsar.Consumer.Callback

        @impl true
        def handle_message(message, state) do
          IO.puts(message.payload)
          {:ok, state}
        end
      end

      {:ok, _pid} =
        Pulsar.Client.start_link(
          host: "pulsar://localhost:6650",
          consumers: [
            [topic: "persistent://public/default/orders",
             subscription_name: "tail",
             callback_module: Tail]
          ]
        )

      Process.sleep(:infinity)

  In IEx, start a bare client and add to it as you go:

      {:ok, _} = Pulsar.Client.start_link(host: "pulsar://localhost:6650")
      {:ok, _} = Pulsar.Producer.start(topic: "persistent://public/default/t", name: :p)

      Pulsar.Producer.send(:p, "hello")

  ## Multiple clusters

  A client is named, and its resources belong to it. `Pulsar.Consumer.start/1` and
  `Pulsar.Producer.start/1` select one with `:client`, defaulting to `:default`:

      children = [
        {Pulsar.Client,
         name: :analytics,
         host: "pulsar://analytics:6650",
         consumers: [
           [topic: topic, subscription_name: "sub", callback_module: MyApp.Handler]
         ]},
        {Pulsar.Client, name: :events, host: "pulsar://events:6650"}
      ]

  Each client keeps its own broker connections, registries and supervisors, so the two are
  fully isolated.

  ## Ownership at a glance

      MyApp.Supervisor
      └── Pulsar.Client
          ├── BrokerRegistry
          ├── BrokerSupervisor
          │   └── broker connection(s)
          └── resources
              ├── consumers
              │   ├── ConsumerRegistry
              │   ├── ConsumerSupervisor
              │   │   └── stable consumer root(s)
              │   │       ├── topology discovery
              │   │       └── partition group(s)
              │   │           └── consumer worker(s)
              │   └── Bootstrap
              └── producers
                  ├── ProducerRegistry
                  ├── ProducerSupervisor
                  │   └── stable producer root(s)
                  │       ├── topology discovery
                  │       └── partition group(s)
                  │           └── producer worker(s)
                  └── Bootstrap

  Non-partitioned resources have one worker group; partitioned resources have one per
  partition. Those groups remain internal—the stable resource root is what the public API
  returns, registers and lists.

  See the [architecture guide](architecture.html) for the ownership and recovery model in
  more detail.

  ## Lifecycle and availability

  A client owns its broker connections, consumers and producers. Partitioned topics need
  nothing special at the call site: each logical consumer or producer keeps one stable root
  while its partition workers are discovered and reconciled in the background.

  Starting a client or resource is not a readiness check. A resource is registered before
  topic discovery and worker initialization complete, so consumer topic and flow operations
  and producer publishing may return `{:error, :not_ready}` until it is usable. Broker and
  worker failures are recovered with retry and backoff where possible.

  Consumers and producers recover independently. Resources declared on a client are recreated
  with it; resources added later with `Pulsar.Consumer.start/1` or `Pulsar.Producer.start/1` are
  runtime state and are not recreated after a client restart. `Pulsar.Client.consumers/1` and
  `Pulsar.Client.producers/1` list their currently running logical roots.

  ## Stream-based reading

  `Pulsar.Reader` exposes a topic as an `Enumerable`, using a temporary non-durable
  subscription for replay, batch processing and one-off jobs.
  """
end
