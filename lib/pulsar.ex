defmodule Pulsar do
  @moduledoc """
  Pulsar client for Elixir.

  This module provides a high-level API for interacting with Apache Pulsar,
  including broker management and consumer/producer operations.

  ## Examples

      # Start a broker connection (idempotent)
      {:ok, broker_pid} = Pulsar.start_broker("pulsar://localhost:6650")
      
      # Start a consumer
      {:ok, consumer_pid} = Pulsar.start_consumer(
        "pulsar://localhost:6650",
        "persistent://public/default/my-topic",
        "my-subscription",
        :Exclusive,
        MyApp.MessageHandler
      )
      
      # Service discovery via broker
      {:ok, response} = Pulsar.Broker.lookup_topic(broker_pid, "my-topic")
  """

  require Logger

  @registry_name Pulsar.BrokerRegistry
  @supervisor_name Pulsar.BrokerSupervisor

  ## Broker Management

  @doc """
  Starts a broker connection with default options (idempotent).

  Uses sensible defaults for socket options, connection timeout, and authentication.
  If a broker for the given URL already exists, returns the existing broker.
  Otherwise, starts a new broker connection.

  Returns `{:ok, broker_pid}` if successful, `{:error, reason}` otherwise.

  ## Examples

      iex> Pulsar.start_broker("pulsar://localhost:6650")
      {:ok, #PID<0.123.0>}
      
      # Safe to call again - returns existing broker
      iex> Pulsar.start_broker("pulsar://localhost:6650")
      {:ok, #PID<0.123.0>}
  """
  @spec start_broker(String.t()) :: {:ok, pid()} | {:error, term()}
  def start_broker(broker_url) do
    start_broker(broker_url, default_broker_opts())
  end

  @doc """
  Starts a broker connection with custom options (idempotent).

  If a broker for the given URL already exists, returns the existing broker.
  Otherwise, starts a new broker connection with the provided options.

  Returns `{:ok, broker_pid}` if successful, `{:error, reason}` otherwise.

  ## Examples

      iex> Pulsar.start_broker("pulsar://localhost:6650", socket_opts: [verify: :none])
      {:ok, #PID<0.123.0>}
      
      iex> Pulsar.start_broker("pulsar://localhost:6650", conn_timeout: 10_000)
      {:ok, #PID<0.123.0>}
  """
  @spec start_broker(String.t(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start_broker(broker_url, opts) do
    case lookup_broker(broker_url) do
      {:ok, broker_pid} ->
        {:ok, broker_pid}

      {:error, :not_found} ->
        do_start_broker(broker_url, opts)
    end
  end

  @doc """
  Looks up an existing broker connection by broker URL.

  Returns `{:ok, broker_pid}` if found, `{:error, :not_found}` otherwise.

  ## Examples

      iex> Pulsar.lookup_broker("pulsar://localhost:6650")
      {:ok, #PID<0.123.0>}
      
      iex> Pulsar.lookup_broker("pulsar://unknown:6650")
      {:error, :not_found}
  """
  @spec lookup_broker(String.t()) :: {:ok, pid()} | {:error, :not_found}
  def lookup_broker(broker_url) do
    broker_key = broker_key(broker_url)

    case Registry.lookup(@registry_name, broker_key) do
      [{pid, _value}] -> {:ok, pid}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Stops a broker connection by broker URL.

  ## Examples

      iex> Pulsar.stop_broker("pulsar://localhost:6650")
      :ok
  """
  @spec stop_broker(String.t()) :: :ok | {:error, :not_found}
  def stop_broker(broker_url) do
    case lookup_broker(broker_url) do
      {:ok, broker_pid} ->
        DynamicSupervisor.terminate_child(@supervisor_name, broker_pid)

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  @doc """
  Lists all active broker connections.

  Returns a list of tuples: `[{broker_key, broker_pid}]`

  ## Examples

      iex> Pulsar.list_brokers()
      [{"localhost:6650", #PID<0.123.0>}]
  """
  @spec list_brokers() :: [{String.t(), pid()}]
  def list_brokers do
    Registry.select(@registry_name, [{{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2"}}]}])
  end

  @doc """
  Returns the number of active broker connections.

  ## Examples

      iex> Pulsar.broker_count()
      2
  """
  @spec broker_count() :: non_neg_integer()
  def broker_count do
    Registry.count(@registry_name)
  end

  ## Consumer Management

  @doc """
  Starts a consumer with explicit parameters.

  ## Examples

      iex> Pulsar.start_consumer(
      ...>   "persistent://public/default/my-topic",
      ...>   "my-subscription",
      ...>   :Exclusive,
      ...>   MyApp.MessageHandler
      ...> )
      {:ok, #PID<0.456.0>}
  """
  @spec start_consumer(String.t(), String.t(), atom(), module(), keyword()) ::
          {:ok, pid()} | {:error, term()}
  def start_consumer(topic, subscription_name, subscription_type, callback_module, opts \\ []) do
    Pulsar.Consumer.start_link(topic, subscription_name, subscription_type, callback_module, opts)
  end

  ## Private Functions

  defp default_broker_opts do
    [
      socket_opts: [],
      conn_timeout: 5_000,
      auth: []
    ]
  end

  defp do_start_broker(broker_url, opts) do
    broker_key = broker_key(broker_url)

    # Add registry name to opts for registration
    registry_opts = [{:name, {:via, Registry, {@registry_name, broker_key}}} | opts]

    # Start the broker process
    child_spec = %{
      id: broker_key,
      start: {Pulsar.Broker, :start_link, [broker_url, registry_opts]},
      restart: :temporary
    }

    case DynamicSupervisor.start_child(@supervisor_name, child_spec) do
      {:ok, broker_pid} ->
        Logger.info("Started broker #{broker_key} with PID #{inspect(broker_pid)}")
        {:ok, broker_pid}

      {:error, {:already_started, broker_pid}} ->
        Logger.debug("Broker #{broker_key} already exists with PID #{inspect(broker_pid)}")
        {:ok, broker_pid}

      {:error, reason} ->
        Logger.error("Failed to start broker #{broker_key}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp broker_key(broker_url) do
    %URI{host: host, port: port} = URI.parse(broker_url)
    "#{host}:#{port}"
  end
end
