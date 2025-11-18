defmodule Pulsar.Client do
  @moduledoc """
  A client represents an isolated Pulsar connection context.

  Each client maintains:
  - Separate broker connections
  - Independent consumer/producer registries
  - Isolated broker configuration

  ## Usage

  ### Embedded Mode (Automatic)

  When using `Pulsar.start/1`, a default client is automatically created:

      # config.exs
      config :pulsar,
        host: "pulsar://localhost:6650",
        consumers: [...]

      # Uses implicit :default client
      {:ok, consumer} = Pulsar.start_consumer(topic, subscription, MyCallback)

  ### Manual Mode (Explicit)

  You can start multiple clients in your supervision tree:

      children = [
        {Pulsar.Client, name: :analytics_client, host: "pulsar://analytics:6650"},
        {Pulsar.Client, name: :events_client, host: "pulsar://events:6650"}
      ]

      # Explicit client usage
      {:ok, consumer} = Pulsar.start_consumer(
        topic, subscription, MyCallback,
        client: :analytics_client
      )
  """

  use Supervisor

  @supported_broker_opts [
    :auth,
    :conn_timeout,
    :socket_opts
  ]

  ## Public API

  @doc """
  Starts a client with the given options.

  ## Options

  - `:name` - Required. The name of the client (atom)
  - `:host` - Optional. Bootstrap broker URL
  - `:start_delay_ms` - Optional. Delay before starting consumers/producers (default: 500)
  - `:startup_jitter_ms` - Optional. Random jitter for consumer/producer startup (default: 0)
  - `:consumers` - Optional. List of consumers to start automatically
  - `:producers` - Optional. List of producers to start automatically
  - `:auth` - Optional. Authentication configuration
  - `:conn_timeout` - Optional. Connection timeout (default: 5000)
  - `:socket_opts` - Optional. Socket options
  """
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    client_name = Keyword.fetch!(opts, :name)
    broker_opts = build_broker_opts(opts)

    # Store broker opts in client state (passed to children via registry metadata)
    :persistent_term.put({__MODULE__, client_name, :broker_opts}, broker_opts)

    children = [
      {Registry, keys: :unique, name: broker_registry(client_name)},
      {Registry, keys: :unique, name: consumer_registry(client_name)},
      {Registry, keys: :unique, name: producer_registry(client_name)},
      {DynamicSupervisor, strategy: :one_for_one, name: broker_supervisor(client_name)},
      {DynamicSupervisor, strategy: :one_for_one, name: consumer_supervisor(client_name)},
      {DynamicSupervisor, strategy: :one_for_one, name: producer_supervisor(client_name)}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  ## Registry and Supervisor Name Helpers

  @doc false
  def broker_registry(client_name) do
    Module.concat([__MODULE__, client_name, BrokerRegistry])
  end

  @doc false
  def consumer_registry(client_name) do
    Module.concat([__MODULE__, client_name, ConsumerRegistry])
  end

  @doc false
  def producer_registry(client_name) do
    Module.concat([__MODULE__, client_name, ProducerRegistry])
  end

  @doc false
  def broker_supervisor(client_name) do
    Module.concat([__MODULE__, client_name, BrokerSupervisor])
  end

  @doc false
  def consumer_supervisor(client_name) do
    Module.concat([__MODULE__, client_name, ConsumerSupervisor])
  end

  @doc false
  def producer_supervisor(client_name) do
    Module.concat([__MODULE__, client_name, ProducerSupervisor])
  end

  @doc false
  def get_broker_opts(client_name) do
    :persistent_term.get({__MODULE__, client_name, :broker_opts}, [])
  end

  ## Private Functions

  defp build_broker_opts(opts) do
    app_opts =
      @supported_broker_opts
      |> Enum.map(fn key -> {key, Application.get_env(:pulsar, key)} end)
      |> Enum.reject(fn {_, v} -> is_nil(v) end)

    passed_opts =
      opts
      |> Keyword.take(@supported_broker_opts)
      |> Enum.reject(fn {_, v} -> is_nil(v) end)

    Keyword.merge(app_opts, passed_opts)
  end
end
