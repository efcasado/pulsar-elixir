defmodule Pulsar do
  @moduledoc """
  An Apache Pulsar client for Elixir.

  The public API is three modules:

  - `Pulsar.Client` — a connection context for one Pulsar cluster
  - `Pulsar.Consumer` — subscribes to a topic and dispatches to a callback module
  - `Pulsar.Producer` — publishes to a topic

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

  ## Supervision tree

  A consumer or producer is a supervisor in its own right, over one worker per partition and
  per `:consumer_count` / `:producer_count`. Partitioned topics therefore need nothing
  special at the call site.

      MyApp.Supervisor
      └── Pulsar.Client (:default)            :rest_for_one
          ├── ProducerEpochStore (ETS)
          ├── BrokerRegistry
          ├── BrokerSupervisor
          │   ├── Broker 1          monitors: C1, C2, DLQ-P1, P1
          │   └── Broker 2          monitors: C3, C4
          └── resources                       :one_for_one
              ├── consumers                   :rest_for_one
              │   ├── ConsumerRegistry
              │   ├── ConsumerSupervisor
              │   │   ├── Pulsar.Consumer: my-topic
              │   │   │   └── C1 (with DLQ policy)
              │   │   │       └── DLQ-P1 (linked process)
              │   │   └── Pulsar.Consumer: my-partitioned-topic
              │   │       ├── partition-0 → C2
              │   │       ├── partition-1 → C3
              │   │       ├── partition-2 → C4
              │   │       └── Topic.Discovery (polls for new partitions)
              │   └── Bootstrap     (connects Broker 1, starts declared consumers)
              └── producers                   :rest_for_one
                  ├── ProducerRegistry
                  ├── ProducerSupervisor
                  │   └── Pulsar.Producer: my-partitioned-topic
                  │       ├── partition-0 → P2
                  │       ├── partition-1 → P3
                  │       ├── partition-2 → P4
                  │       └── Topic.Discovery (polls for new partitions)
                  └── Bootstrap     (connects Broker 1, starts declared producers)

  The strategies follow the dependencies. Everything resolves topics through the brokers, so
  losing them means starting the resources over — hence `:rest_for_one` at the top. Consumers
  and producers do not depend on each other, so `resources` is `:one_for_one` and a consumer
  failure leaves producers alone. Within a branch, resources register their names in the
  registry as they start, so a registry that came back empty would leave them alive and
  unreachable by name — `:rest_for_one` again.

  The `DynamicSupervisor`s have no static child list and so come back empty when restarted.
  `Bootstrap` is what fills them, and it is static, so it runs again on every restart of its
  branch. A declared resource that fails is logged and retried with backoff, so a consumer
  whose broker is briefly unreachable starts once it is reachable rather than being abandoned.

  A client starts before its bootstrap connection is established: `Pulsar.Client.start_link/1`
  returning does not mean the broker is reachable, only that the connection process exists and
  is retrying. It is not a readiness check.

  ## Reading without a subscription

  `Pulsar.Reader` exposes a topic as an `Enumerable`, for replaying a topic from a position
  rather than consuming it against a subscription.
  """
end
