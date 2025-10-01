defmodule Pulsar do
  @moduledoc """
  Pulsar client for Elixir.

  This module provides a high-level API for interacting with Apache Pulsar,
  including broker management and consumer group operations.

  ## Consumer Architecture

  All consumers are managed as **consumer groups** - supervised collections of 
  consumer processes. Even when you want a single consumer, it's created as a 
  consumer group with `consumer_count: 1` (the default).

  This provides:
  - Automatic restart on failures
  - Easy scaling (just increase consumer_count)
  - Consistent API whether you have 1 or N consumers

  ## Examples

      # Start a broker connection (idempotent)
      {:ok, broker_pid} = Pulsar.start_broker("pulsar://localhost:6650")
      
      # Start a single consumer (creates a consumer group with 1 consumer)
      {:ok, group_pid} = Pulsar.start_consumer(
        "persistent://public/default/my-topic",
        "my-subscription",
        :Exclusive,
        MyApp.MessageHandler
      )
      
      # Start multiple consumers for parallel processing
      {:ok, group_pid} = Pulsar.start_consumer(
        "persistent://public/default/my-topic",
        "my-subscription",
        :Shared,
        MyApp.MessageHandler,
        consumer_count: 3
      )
      
      # Stop the consumer group (using the returned PID)
      Pulsar.stop_consumer(group_pid)
      
      # Or stop by group ID (if you need programmatic access)
      # Pulsar.stop_consumer("my-topic-my-subscription-123456")
      
      # Service discovery via broker
      {:ok, response} = Pulsar.Broker.lookup_topic(broker_pid, "my-topic")
  """

  require Logger

  @registry_name Pulsar.BrokerRegistry
  @supervisor_name Pulsar.BrokerSupervisor
  @consumer_group_supervisor_name Pulsar.ConsumerSupervisor
  @consumer_group_registry_name Pulsar.ConsumerGroupRegistry

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
  Starts a consumer group (supervised for automatic restart).

  Creates a consumer group supervisor that manages one or more consumer processes.
  This is the primary way to consume messages from Pulsar topics. Even for a 
  single consumer, a consumer group is created for consistent supervision and 
  restart behavior.

  ## Parameters

  - `topic` - The topic to subscribe to
  - `subscription_name` - Name of the subscription
  - `subscription_type` - Type of subscription (e.g., :Exclusive, :Shared, :Key_Shared)
  - `callback_module` - Module that implements `Pulsar.Consumer.Callback` behaviour
  - `opts` - Additional options:
    - `:consumer_count` - Number of consumer processes to start (default: 1)
    - `:init_args` - Arguments passed to callback module's init/1 function
    - Other options passed to ConsumerGroup supervisor

  Returns `{:ok, group_pid}` where `group_pid` is the PID of the consumer group supervisor.
  Use this PID with `stop_consumer/1` to stop the entire consumer group.

  ## Examples

      # Start a single consumer (default behavior)
      iex> Pulsar.start_consumer(
      ...>   "persistent://public/default/my-topic",
      ...>   "my-subscription",
      ...>   :Exclusive,
      ...>   MyApp.MessageHandler
      ...> )
      {:ok, #PID<0.456.0>}

      # With initialization arguments for the callback module
      iex> Pulsar.start_consumer(
      ...>   "persistent://public/default/my-topic",
      ...>   "my-subscription",
      ...>   :Exclusive,
      ...>   MyApp.MessageCounter,
      ...>   init_args: [max_messages: 100]
      ...> )
      {:ok, #PID<0.456.0>}

      # With multiple consumers for parallel processing
      iex> Pulsar.start_consumer(
      ...>   "persistent://public/default/my-topic",
      ...>   "my-subscription",
      ...>   :Key_Shared,
      ...>   MyApp.MessageHandler,
      ...>   consumer_count: 3
      ...> )
      {:ok, #PID<0.456.0>}  # Returns consumer group supervisor PID
  """
  @spec start_consumer(String.t(), String.t(), atom(), module(), keyword()) ::
          {:ok, pid()} | {:error, term()}
  def start_consumer(topic, subscription_name, subscription_type, callback_module, opts \\ []) do
    start_consumer_group(
      topic,
      subscription_name,
      subscription_type,
      callback_module,
      opts
    )
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
      restart: :permanent
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

  @doc """
  Lists all active consumer groups.

  Returns a list of tuples: `[{group_id, group_pid}]`
  """
  @spec list_consumer_groups() :: [{String.t(), pid()}]
  def list_consumer_groups do
    Registry.select(@consumer_group_registry_name, [
      {{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2"}}]}
    ])
  end

  @doc """
  Gets information about a specific consumer group by group ID.

  Returns `{:ok, info_map}` if found, `{:error, :not_found}` otherwise.
  """
  @spec get_consumer_group_info(String.t()) :: {:ok, map()} | {:error, :not_found}
  def get_consumer_group_info(group_id) do
    case Registry.lookup(@consumer_group_registry_name, group_id) do
      [{group_pid, _value}] ->
        info = Pulsar.ConsumerGroup.get_info(group_pid)
        {:ok, Map.put(info, :group_id, group_id)}

      [] ->
        {:error, :not_found}
    end
  end

  @doc """
  Stops a consumer group.

  Accepts either:
  - A group PID (returned by `start_consumer/5`)
  - A group ID string (for programmatic access)

  Returns `:ok` if successful, `{:error, :not_found}` if the group doesn't exist.

  ## Examples

      # Stop by PID (most common usage)
      iex> {:ok, group_pid} = Pulsar.start_consumer(topic, subscription, :Shared, MyHandler)
      iex> Pulsar.stop_consumer(group_pid)
      :ok

      # Stop by group ID string
      iex> Pulsar.stop_consumer("my-topic-my-subscription-123456")
      {:ok, :not_found}  # if group doesn't exist
  """
  @spec stop_consumer(pid() | String.t()) :: :ok | {:error, :not_found}
  def stop_consumer(group_pid) when is_pid(group_pid) do
    Pulsar.ConsumerGroup.stop(group_pid)
  end

  def stop_consumer(group_id) when is_binary(group_id) do
    case Registry.lookup(@consumer_group_registry_name, group_id) do
      [{group_pid, _value}] ->
        Pulsar.ConsumerGroup.stop(group_pid)
        :ok

      [] ->
        {:error, :not_found}
    end
  end

  @doc """
  Stops a consumer group by group ID.

  **Deprecated:** Use `stop_consumer/1` instead, which accepts both PIDs and group IDs.
  """
  @deprecated "Use stop_consumer/1 instead"
  @spec stop_consumer_group(String.t()) :: :ok | {:error, :not_found}
  def stop_consumer_group(group_id) do
    stop_consumer(group_id)
  end

  @doc """
  Returns the total number of active consumer groups.
  """
  @spec consumer_group_count() :: non_neg_integer()
  def consumer_group_count do
    Registry.count(@consumer_group_registry_name)
  end

  ## Private Functions

  defp start_consumer_group(topic, subscription_name, subscription_type, callback_module, opts) do
    # Generate unique group ID using the same logic as ConsumerGroup
    group_id = Pulsar.ConsumerGroup.generate_group_id(topic, subscription_name)

    consumer_group_opts =
      [
        topic: topic,
        subscription_name: subscription_name,
        subscription_type: subscription_type,
        callback_module: callback_module,
        name: {:via, Registry, {@consumer_group_registry_name, group_id}}
      ] ++ opts

    child_spec = %{
      id: group_id,
      start: {Pulsar.ConsumerGroup, :start_link, [consumer_group_opts]},
      restart: :permanent,
      type: :supervisor
    }

    case DynamicSupervisor.start_child(@consumer_group_supervisor_name, child_spec) do
      {:ok, group_pid} ->
        Logger.info("Started consumer group #{group_id} with PID #{inspect(group_pid)}")
        {:ok, group_pid}

      {:error, reason} ->
        Logger.error("Failed to start consumer group #{group_id}: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
