defmodule Pulsar do
  @moduledoc """
  Pulsar client for Elixir.

  This module provides a high-level API for interacting with the Elixir client for Apache Pulsar.

  ## Consumer Architecture

  Consumers are managed through supervised processes:
  - **Regular topics**: Consumer groups with configurable process count
  - **Partitioned topics**: PartitionedConsumer supervisor managing consumer groups per partition

  The `start_consumer/1` function always returns a single PID that can be registered with
  a name, regardless of whether the topic is partitioned or not; and the consumer count within.

  ## Examples

      # Start a broker connection (idempotent)
      {:ok, broker_pid} = Pulsar.start_broker("pulsar://localhost:6650")

      # Start a consumer for a regular topic
      {:ok, consumer_pid} = Pulsar.start_consumer(
        "persistent://public/default/my-topic",
        "my-subscription",
        MyApp.MessageHandler,
        subscription_type: :Exclusive
      )

      # Start a consumer for a partitioned topic (single PID returned)
      {:ok, consumer_pid} = Pulsar.start_consumer(
        "persistent://public/default/my-partitioned-topic",
        "my-subscription",
        MyApp.MessageHandler,
        subscription_type: :Shared,
        consumer_count: 2  # 2 consumers per partition
      )

      # Register consumer with custom name
      {:ok, consumer_pid} = Pulsar.start_consumer(
        "persistent://public/default/my-topic",
        "my-subscription",
        MyApp.MessageHandler,
        subscription_type: :Shared,
        name: MyApp.MyConsumer
      )

      # Stop consumer (works for both regular and partitioned)
      Pulsar.stop_consumer(consumer_pid)

      # Or stop by name
      Pulsar.stop_consumer("my-topic-my-subscription")

      # Lookup consumer by name
      {:ok, consumer_pid} = Pulsar.lookup_consumer("my-topic-my-subscription")

      # Get all consumer processes by name
      consumer_pids = Pulsar.get_consumers("my-topic-my-subscription")

      # Service discovery via broker
      {:ok, response} = Pulsar.Broker.lookup_topic(broker_pid, "my-topic")
  """
  use Application
  require Logger

  @supported_broker_opts [
    :auth,
    :conn_timeout,
    :socket_opts
  ]

  @app_supervisor Pulsar.Supervisor
  @broker_registry Pulsar.BrokerRegistry
  @consumer_registry Pulsar.ConsumerRegistry
  @producer_registry Pulsar.ProducerRegistry
  @broker_supervisor Pulsar.BrokerSupervisor
  @consumer_supervisor Pulsar.ConsumerSupervisor
  @producer_supervisor Pulsar.ProducerSupervisor

  @doc """
  Start the Pulsar application with custom configuration.

  ## Examples

      # Start with custom configuration
      {:ok, pid} = Pulsar.Application.start(
        host: "pulsar://localhost:6650",
        consumers: [
          {:my_consumer, [
            topic: "my-topic",
            subscription_name: "my-subscription",
            callback_module: MyConsumerCallback,
            subscription_type: :Shared
          ]}
        ]
      )

      # Later, stop it
      :ok = Pulsar.Application.stop(pid)
  """
  def start(config) do
    start(:normal, config)
  end

  @impl true
  def start(_type, opts) do
    consumers =
      Keyword.get(opts, :consumers, Application.get_env(:pulsar, :consumers, []))

    bootstrap_host =
      Keyword.get(opts, :host, Application.get_env(:pulsar, :host))

    start_delay_ms =
      Keyword.get(opts, :start_delay_ms, 500)

    broker_opts = broker_opts(opts)

    :persistent_term.put({Pulsar, :broker_opts}, broker_opts)

    children =
      [
        {Registry, keys: :unique, name: @broker_registry},
        {Registry, keys: :unique, name: @consumer_registry},
        {Registry, keys: :unique, name: @producer_registry},
        {DynamicSupervisor, strategy: :one_for_one, name: @broker_supervisor},
        {DynamicSupervisor, strategy: :one_for_one, name: @consumer_supervisor},
        {DynamicSupervisor, strategy: :one_for_one, name: @producer_supervisor}
      ]

    opts = [strategy: :one_for_one, name: @app_supervisor]
    {:ok, pid} = Supervisor.start_link(children, opts)

    # a bootstrap host is required
    {:ok, _} = Pulsar.start_broker(bootstrap_host, broker_opts)

    Process.sleep(start_delay_ms)

    consumers
    |> Enum.each(fn {consumer_name, consumer_opts} ->
      topic = Keyword.fetch!(consumer_opts, :topic)
      subscription_name = Keyword.fetch!(consumer_opts, :subscription_name)
      callback_module = Keyword.fetch!(consumer_opts, :callback_module)

      remaining_opts =
        consumer_opts
        |> Keyword.drop([:topic, :subscription_name, :callback_module])
        |> Keyword.put(:name, consumer_name)

      Pulsar.start_consumer(topic, subscription_name, callback_module, remaining_opts)
    end)

    {:ok, pid}
  end

  @impl true
  def stop(pid) when is_pid(pid) do
    Supervisor.stop(pid, :normal)
  end

  @doc """
  Starts a broker connection.

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
  def start_broker(broker_url, opts \\ []) do
    case lookup_broker(broker_url) do
      {:ok, broker_pid} ->
        {:ok, broker_pid}

      {:error, :not_found} ->
        global_opts = :persistent_term.get({Pulsar, :broker_opts}, [])
        merged_opts = Keyword.merge(global_opts, opts)
        registry_opts = [{:name, {:via, Registry, {@broker_registry, broker_url}}} | merged_opts]

        child_spec = %{
          id: broker_url,
          start: {Pulsar.Broker, :start_link, [broker_url, registry_opts]},
          # TO-DO: should be transient?
          restart: :permanent
        }

        case DynamicSupervisor.start_child(@broker_supervisor, child_spec) do
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

  ## Examples

      iex> Pulsar.lookup_broker("pulsar://localhost:6650")
      {:ok, #PID<0.123.0>}

      iex> Pulsar.lookup_broker("pulsar://unknown:6650")
      {:error, :not_found}
  """
  @spec lookup_broker(String.t()) :: {:ok, pid()} | {:error, :not_found}
  def lookup_broker(broker_url) do
    case Registry.lookup(@broker_registry, broker_url) do
      [{pid, _value}] -> {:ok, pid}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Stops a broker connection by broker URL.

  Gracefully stops all connected consumers and producers before shutting down the broker.

  ## Examples

      iex> Pulsar.stop_broker("pulsar://localhost:6650")
      :ok
  """
  @spec stop_broker(String.t()) :: :ok | {:error, :not_found}
  def stop_broker(broker_url) do
    case lookup_broker(broker_url) do
      {:ok, broker_pid} ->
        Pulsar.Broker.stop(broker_pid)
        :ok

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  @doc """
  Starts a consumer for a topic (regular or partitioned).

  This is the primary way to consume messages from Pulsar topics. For regular topics,
  a single consumer group is created. For partitioned topics, a PartitionedConsumer
  supervisor is created that manages individual consumer groups for each partition.

  ## Parameters

  - `topic` - The topic to subscribe to (regular or partitioned)
  - `subscription_name` - Name of the subscription
  - `callback_module` - Module that implements `Pulsar.Consumer.Callback` behaviour
  - `opts` - Optional parameters:
    - `:subscription_type` - Type of subscription (e.g., :Exclusive, :Shared, :Key_Shared, default: :Shared)
    - `:name` - Custom name for the consumer (default: "topic-subscription_name")
    - `:consumer_count` - Number of consumer processes per topic/partition (default: 1)
    - `:init_args` - Arguments passed to callback module's init/1 function
    - `:flow_initial` - Initial flow permits (default: 100)
    - `:flow_threshold` - Flow permits threshold for refill (default: 50)
    - `:flow_refill` - Flow permits refill amount (default: 50)
    - `:initial_position` - Initial position for subscription (`:latest` or `:earliest`, defaults to `:latest`)
    - Other options passed to ConsumerGroup supervisor

  ## Return Values

  - For regular topics: PID of the consumer group supervisor
  - For partitioned topics: PID of the PartitionedConsumer supervisor that manages all partition groups

  ## Partitioned Topics

  When you subscribe to a partitioned topic, the function automatically:
  1. Queries the broker for partition metadata
  2. Creates a PartitionedConsumer supervisor
  3. The PartitionedConsumer creates separate consumer groups for each partition
  4. Returns a single PID for the PartitionedConsumer supervisor

  ## Consumer Naming and Registry

  All consumers are automatically registered in a registry and can be looked up by name:
  - **Default naming**: `"<topic>-<subscription_name>"`
  - **Custom naming**: Provided via the `:name` option
  - **Partitioned topics**: Individual partition groups are named `"<base_name>-partition-<index>"`

  This allows you to manage consumers by name without keeping track of PIDs.

  ## Examples

      # Regular topic - returns single PID
      iex> {:ok, consumer_pid} = Pulsar.start_consumer(
      ...>   "persistent://public/default/my-topic",
      ...>   "my-subscription",
      ...>   MyApp.MessageHandler,
      ...>   subscription_type: :Exclusive
      ...> )
      {:ok, #PID<0.456.0>}

      # Partitioned topic
      iex> {:ok, consumer_pid} = Pulsar.start_consumer(
      ...>   "persistent://public/default/my-partitioned-topic",
      ...>   "my-subscription",
      ...>   MyApp.MessageHandler,
      ...>   subscription_type: :Shared
      ...> )
      {:ok, #PID<0.456.0>}

      # Register with custom name
      iex> {:ok, consumer_pid} = Pulsar.start_consumer(
      ...>   "persistent://public/default/my-partitioned-topic",
      ...>   "my-subscription",
      ...>   MyApp.MessageHandler,
      ...>   subscription_type: :Key_Shared,
      ...>   consumer_count: 2,
      ...>   name: MyApp.MyConsumer
      ...> )
      {:ok, #PID<0.456.0>}
  """

  @spec start_consumer(String.t(), String.t(), module(), keyword()) :: {:ok, pid} | {:error, term}
  def start_consumer(topic, subscription_name, callback_module, opts \\ []) do
    subscription_type = Keyword.get(opts, :subscription_type, :Shared)
    name = Keyword.get(opts, :name, topic <> "-" <> subscription_name)

    case check_partitioned_topic(topic) do
      {:ok, 0} ->
        # Regular topic - create single consumer group
        child_spec = %{
          id: name,
          start: {
            Pulsar.ConsumerGroup,
            :start_link,
            [name, topic, subscription_name, subscription_type, callback_module, opts]
          },
          restart: :permanent,
          type: :supervisor
        }

        DynamicSupervisor.start_child(@consumer_supervisor, child_spec)

      {:ok, partitions} when partitions > 0 ->
        # Partitioned topic - create PartitionedConsumer supervisor
        child_spec = %{
          id: name,
          start: {
            Pulsar.PartitionedConsumer,
            :start_link,
            [name, topic, partitions, subscription_name, subscription_type, callback_module, opts]
          },
          restart: :permanent,
          type: :supervisor
        }

        DynamicSupervisor.start_child(@consumer_supervisor, child_spec)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Stops a consumer (regular or partitioned).

  Accepts either:
  - A consumer PID (returned by `start_consumer/1`)
  - A consumer ID string (for programmatic access)

  For partitioned topics, this stops the PartitionedConsumer supervisor,
  which automatically stops all partition consumer groups.

  Returns `:ok` if successful, `{:error, :not_found}` if the consumer doesn't exist.

  ## Examples

      # Stop regular consumer
      iex> {:ok, consumer_pid} = Pulsar.start_consumer(...)
      iex> Pulsar.stop_consumer(consumer_pid)
      :ok

      # Stop partitioned consumer (stops all partitions)
      iex> {:ok, consumer_pid} = Pulsar.start_consumer(...)  # partitioned topic
      iex> Pulsar.stop_consumer(consumer_pid)
      :ok

      # Stop by consumer ID string
      iex> Pulsar.stop_consumer("my-topic-my-subscription")
      :ok
  """
  @spec stop_consumer(pid() | String.t()) :: :ok | {:error, :not_found}
  def stop_consumer(group_pid) when is_pid(group_pid) do
    Supervisor.stop(group_pid)
  end

  def stop_consumer(group_id) when is_binary(group_id) do
    case Registry.lookup(@consumer_registry, group_id) do
      [{group_pid, _value}] ->
        stop_consumer(group_pid)

      [] ->
        {:error, :not_found}
    end
  end

  @doc """
  Looks up a consumer by name.

  Returns `{:ok, consumer_pid}` if found, `{:error, :not_found}` otherwise.

  ## Examples

      iex> {:ok, consumer_pid} = Pulsar.start_consumer("my-topic", "my-subscription", MyHandler, name: "my-consumer")
      iex> Pulsar.lookup_consumer("my-consumer")
      {:ok, consumer_pid}

      iex> Pulsar.lookup_consumer("nonexistent")
      {:error, :not_found}
  """
  @spec lookup_consumer(String.t()) :: {:ok, pid()} | {:error, :not_found}
  def lookup_consumer(name) do
    case Registry.lookup(@consumer_registry, name) do
      [{consumer_pid, _value}] -> {:ok, consumer_pid}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Gets all consumer processes managed by a consumer manager.

  Works with both ConsumerGroup and PartitionedConsumer supervisors.
  Returns a flat list of consumer process PIDs.
  """
  @spec get_consumers(pid() | String.t()) :: [pid()] | {:error, :not_found}
  def get_consumers(group_pid) when is_pid(group_pid) do
    case Supervisor.which_children(group_pid) do
      [] ->
        []

      [{_id, _child_pid, :worker, [Pulsar.Consumer]} | _] ->
        # This is a ConsumerGroup - children are Consumer processes
        Pulsar.ConsumerGroup.get_consumers(group_pid)

      [{_id, _child_pid, :supervisor, _modules} | _] ->
        # This is a PartitionedConsumer - children are ConsumerGroup supervisors
        Pulsar.PartitionedConsumer.get_consumers(group_pid)

      _ ->
        {:error, :unknown_supervisor_type}
    end
  end

  def get_consumers(group_id) when is_binary(group_id) do
    case Registry.lookup(@consumer_registry, group_id) do
      [{group_pid, _value}] ->
        get_consumers(group_pid)

      [] ->
        {:error, :not_found}
    end
  end

  @doc """
  Starts a producer for the given topic.

  The broker will assign a unique producer name automatically.

  ## Parameters

  - `args` - Keyword list with:
    - `:topic` - The topic to publish to (required)
    - `:access_mode` - Producer access mode (optional, default: :Shared)

  ## Examples

      iex> Pulsar.start_producer(topic: "persistent://public/default/my-topic")
      {:ok, #PID<0.789.0>}
  """
  @spec start_producer(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_producer(args) do
    topic = Keyword.fetch!(args, :topic)
    Pulsar.Producer.start_link(topic, args)
  end

  @doc """
  Stops a producer.

  ## Examples

      iex> Pulsar.stop_producer(producer_pid)
      :ok
  """
  @spec stop_producer(pid()) :: :ok
  def stop_producer(producer) do
    Pulsar.Producer.stop(producer)
  end

  @spec check_partitioned_topic(String.t()) :: {:ok, integer()} | {:error, term()}
  defp check_partitioned_topic(topic) do
    broker = Pulsar.Utils.broker()

    case Pulsar.Broker.partitioned_topic_metadata(broker, topic) do
      {:ok, %{response: :Success, partitions: partitions}} ->
        {:ok, partitions}

      {:ok, %{response: :Failed, error: error}} ->
        {:error, {:partition_metadata_check_failed, error}}
    end
  end

  defp broker_opts(opts) do
    opts
    |> Keyword.take(@supported_broker_opts)
    |> Enum.reject(fn {_, v} -> is_nil(v) end)
  end
end
