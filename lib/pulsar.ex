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

  See `Pulsar.Client` for adding consumers and producers to a running client.

  ## Scripts and IEx

  A client started directly is all a script needs:

  <!-- x-release-please-start-version -->
      Mix.install([{:pulsar, "~> 3.1.0", hex: :pulsar_elixir}])
  <!-- x-release-please-end -->

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

  Each client has an isolated connection context and resource set.

  ## Lifecycle and availability

  Resource startup is asynchronous. Use `Pulsar.Consumer.await_ready/2` or
  `Pulsar.Producer.await_ready/2` when an operation must wait for topology discovery and worker
  initialization.

  See the [architecture guide](architecture.html) for the ownership tree, asynchronous startup,
  and recovery model.

  ## Stream-based reading

  `Pulsar.Reader` exposes a topic as an `Enumerable`, using a temporary non-durable
  subscription for replay, batch processing and one-off jobs.
  """
end
