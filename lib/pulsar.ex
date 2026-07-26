defmodule Pulsar do
  @moduledoc """
  An Apache Pulsar client for Elixir.

  The public API is three modules:

  - `Pulsar.Client` — a connection context for one Pulsar cluster
  - `Pulsar.Consumer` — subscribes to a topic and dispatches to a callback module
  - `Pulsar.Producer` — publishes to a topic

  This module itself is only the application callback. It starts no clients, consumers or
  producers and reads no configuration, so `:pulsar` booting ahead of the applications that
  depend on it cannot open sockets or dispatch messages into an application that is still
  starting up.

  ## Getting started

  Everything belongs in the host application's supervision tree:

      children = [
        {Pulsar.Client, host: "pulsar://localhost:6650"},
        {Pulsar.Producer, topic: "persistent://public/default/audit", name: :audit},
        {Pulsar.Consumer,
         topic: "persistent://public/default/orders",
         subscription_name: "order-service",
         callback_module: MyApp.OrderHandler}
      ]

      Supervisor.start_link(children, strategy: :rest_for_one)

  `:rest_for_one` is deliberate: consumers and producers resolve brokers through registries
  their client owns, so they have to be restarted when the client is.

  Then publish with `Pulsar.Producer.send/3`:

      {:ok, message_id} = Pulsar.Producer.send(:audit, "payload")

  Clients, consumers and producers can also be created at runtime with `Pulsar.Client.start/1`,
  `Pulsar.Consumer.start/1` and `Pulsar.Producer.start/1`, which supervise them under
  `:pulsar` instead of under the caller.

  ## Multiple clusters

  A client is named, and consumers and producers select one with `:client`, defaulting to
  `:default`:

      children = [
        {Pulsar.Client, name: :analytics, host: "pulsar://analytics:6650"},
        {Pulsar.Client, name: :events, host: "pulsar://events:6650"},
        {Pulsar.Consumer,
         topic: topic,
         subscription_name: "sub",
         callback_module: MyApp.Handler,
         client: :analytics}
      ]

  Each client keeps its own broker connections, registries and supervisors, so the two are
  fully isolated.

  ## Supervision tree

  A consumer or producer is a supervisor in its own right, over one worker per partition and
  per `:consumer_count` / `:producer_count`. Partitioned topics therefore need nothing
  special at the call site.

      MyApp.Supervisor
      ├── Pulsar.Client (:default)
      │   ├── BrokerRegistry, ConsumerRegistry, ProducerRegistry
      │   ├── ProducerEpochStore (ETS)
      │   ├── BrokerSupervisor
      │   │   ├── Broker 1          monitors: C1, C2, DLQ-P1, P1
      │   │   └── Broker 2          monitors: C3, C4
      │   ├── ConsumerSupervisor    (for Pulsar.Consumer.start/1)
      │   └── ProducerSupervisor    (for Pulsar.Producer.start/1)
      │
      ├── Pulsar.Consumer: my-topic
      │   └── C1 (with DLQ policy)
      │       └── DLQ-P1 (linked process)
      │
      ├── Pulsar.Consumer: my-partitioned-topic
      │   ├── partition-0 → C2
      │   ├── partition-1 → C3
      │   ├── partition-2 → C4
      │   └── PartitionDiscovery (polls for newly added partitions)
      │
      └── Pulsar.Producer: my-partitioned-topic
          ├── partition-0 → P2
          ├── partition-1 → P3
          ├── partition-2 → P4
          └── PartitionDiscovery (polls for newly added partitions)

  ## Reading without a subscription

  `Pulsar.Reader` exposes a topic as an `Enumerable`, for replaying a topic from a position
  rather than consuming it against a subscription.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: Pulsar.Supervisor}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: __MODULE__)
  end
end
