defmodule Pulsar.Client do
  @moduledoc """
  A client represents an isolated Pulsar connection context.

  Each client maintains:
  - Separate broker connections
  - Independent consumer/producer registries
  - Isolated broker configuration

  ## Usage

  ### Single Client (Implicit)

  When using `Pulsar.start/1`, a default client is automatically created:

      # config.exs
      config :pulsar,
        host: "pulsar://localhost:6650",
        consumers: [...]

      # Uses implicit :default client
      {:ok, consumer} = Pulsar.start_consumer(topic, subscription, MyCallback)

  ### Multiple Clients (Explicit)

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
  - `:host` - Required. Bootstrap broker URL
  - `:auth` - Optional. Authentication configuration
  - `:conn_timeout` - Optional. Connection timeout (default: 5000)
  - `:socket_opts` - Optional. Socket options
  """
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    bootstrap_host = Keyword.fetch!(opts, :host)

    case Supervisor.start_link(__MODULE__, opts, name: name) do
      {:ok, pid} = result ->
        # Start the bootstrap broker after supervisor is running
        case start_broker(bootstrap_host, client: name) do
          {:ok, _broker_pid} ->
            result

          {:error, reason} ->
            Supervisor.stop(pid)
            {:error, {:broker_startup_failed, reason}}
        end

      error ->
        error
    end
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

  @doc """
  Returns a random broker process from the specified client's broker supervisor.

  Defaults to the `:default` client if no client is specified.

  This is useful for operations that need any broker from a client (e.g., service discovery).
  """
  @spec random_broker(atom()) :: pid() | nil
  def random_broker(client_name \\ :default) do
    broker_supervisor = broker_supervisor(client_name)

    case Supervisor.which_children(broker_supervisor) do
      [] ->
        nil

      children ->
        {_id, pid, _, _} = Enum.random(children)
        pid
    end
  end

  @doc """
  Starts a broker connection.

  If a broker for the given URL already exists, returns the existing broker.
  Otherwise, starts a new broker connection with the provided options.

  Returns `{:ok, broker_pid}` if successful, `{:error, reason}` otherwise.
  """
  @spec start_broker(String.t(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start_broker(broker_url, opts \\ []) do
    client = Keyword.get(opts, :client, :default)
    broker_registry = broker_registry(client)
    broker_supervisor = broker_supervisor(client)

    case lookup_broker(broker_url, client: client) do
      {:ok, broker_pid} ->
        {:ok, broker_pid}

      {:error, :not_found} ->
        global_opts = get_broker_opts(client)
        merged_opts = Keyword.merge(global_opts, Keyword.delete(opts, :client))
        registry_opts = [{:name, {:via, Registry, {broker_registry, broker_url}}} | merged_opts]

        child_spec = %{
          id: broker_url,
          start: {Pulsar.Broker, :start_link, [broker_url, registry_opts]},
          restart: :permanent
        }

        case DynamicSupervisor.start_child(broker_supervisor, child_spec) do
          {:ok, broker_pid} ->
            {:ok, broker_pid}

          {:error, {:already_started, broker_pid}} ->
            {:ok, broker_pid}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  @doc """
  Looks up an existing broker connection by broker URL.

  Returns `{:ok, broker_pid}` if found, `{:error, :not_found}` otherwise.
  """
  @spec lookup_broker(String.t(), keyword()) :: {:ok, pid()} | {:error, :not_found}
  def lookup_broker(broker_url, opts \\ []) do
    client = Keyword.get(opts, :client, :default)
    broker_registry = broker_registry(client)

    case Registry.lookup(broker_registry, broker_url) do
      [{pid, _value}] -> {:ok, pid}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Stops a broker connection by broker URL.
  """
  @spec stop_broker(String.t(), keyword()) :: :ok | {:error, :not_found}
  def stop_broker(broker_url, opts \\ []) do
    case lookup_broker(broker_url, opts) do
      {:ok, broker_pid} ->
        Pulsar.Broker.stop(broker_pid)
        :ok

      {:error, :not_found} ->
        {:error, :not_found}
    end
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
