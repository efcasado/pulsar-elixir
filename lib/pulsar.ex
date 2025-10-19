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
      {:ok, [group_pid]} = Pulsar.start_consumer(
        topic: "persistent://public/default/my-topic",
        subscription_name: "my-subscription",
        subscription_type: :Exclusive,
        callback_module: MyApp.MessageHandler
      )

      # Start consumers for a partitioned topic (returns one PID per partition)
      {:ok, group_pids} = Pulsar.start_consumer(
        topic: "persistent://public/default/my-partitioned-topic",
        subscription_name: "my-subscription",
        subscription_type: :Shared,
        callback_module: MyApp.MessageHandler,
        opts: [consumer_count: 2]  # 2 consumers per partition
      )

      # Stop individual consumer groups (one PID at a time)
      Pulsar.stop_consumer(group_pid)

      # Stop all partitions
      Enum.each(group_pids, &Pulsar.stop_consumer/1)

      # Service discovery via broker
      {:ok, response} = Pulsar.Broker.lookup_topic(broker_pid, "my-topic")
  """
  use Application
  require Logger

  @app_supervisor Pulsar.Supervisor
  @broker_registry Pulsar.BrokerRegistry
  @consumer_registry Pulsar.ConsumerRegistry
  @broker_supervisor Pulsar.BrokerSupervisor
  @consumer_supervisor Pulsar.ConsumerSupervisor

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
            subscription_type: :Shared,
            callback: MyConsumerCallback
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
    socket_opts =
      Keyword.get(opts, :socket_opts, Application.get_env(:pulsar, :socket_opts, []))

    conn_timeout =
      Keyword.get(opts, :conn_timeout, Application.get_env(:pulsar, :conn_timeout, 5_000))

    auth =
      Keyword.get(opts, :auth, Application.get_env(:pulsar, :auth, []))

    consumers =
      Keyword.get(opts, :consumers, Application.get_env(:pulsar, :consumers, []))

    bootstrap_host =
      Keyword.get(opts, :host, Application.get_env(:pulsar, :host))

    start_delay_ms =
      Keyword.get(opts, :start_delay_ms, 500)

    broker_opts = [
      socket_opts: socket_opts,
      conn_timeout: conn_timeout,
      auth: auth
    ]

    :persistent_term.put({Pulsar, :broker_opts}, broker_opts)

    children =
      [
        {Registry, keys: :unique, name: @broker_registry},
        {Registry, keys: :unique, name: @consumer_registry},
        {DynamicSupervisor, strategy: :one_for_one, name: @broker_supervisor},
        {DynamicSupervisor, strategy: :one_for_one, name: @consumer_supervisor}
      ]

    opts = [strategy: :one_for_one, name: @app_supervisor]
    {:ok, pid} = Supervisor.start_link(children, opts)

    # a bootstrap host is required
    {:ok, _} = Pulsar.start_broker(bootstrap_host, broker_opts)

    Process.sleep(start_delay_ms)

    consumers
    |> Enum.each(fn {_, consumer_opts} -> Pulsar.start_consumer(consumer_opts) end)

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
  Starts consumer groups for a topic (regular or partitioned).

  This is the primary way to consume messages from Pulsar topics. For regular topics,
  a single consumer group is created. For partitioned topics, individual consumer
  groups are created for each partition automatically.

  ## Parameters

  - `topic` - The topic to subscribe to (regular or partitioned)
  - `subscription_name` - Name of the subscription
  - `subscription_type` - Type of subscription (e.g., :Exclusive, :Shared, :Key_Shared)
  - `callback_module` - Module that implements `Pulsar.Consumer.Callback` behaviour
  - `opts` - Additional options:
    - `:consumer_count` - Number of consumer processes per topic/partition (default: 1)
    - `:init_args` - Arguments passed to callback module's init/1 function
    - `:flow_initial` - Initial flow permits (default: 100)
    - `:flow_threshold` - Flow permits threshold for refill (default: 50)
    - `:flow_refill` - Flow permits refill amount (default: 50)
    - `:initial_position` - Initial position for subscription (`:latest` or `:earliest`, defaults to `:latest`)
  - Other options passed to ConsumerGroup supervisor

  ## Return Values

  Returns:
  - `{:ok, [pid()]}` - List of consumer group supervisor PIDs when all succeed:
    - For regular topics: List with one PID
    - For partitioned topics: List with one PID per partition
  - `{:error, [reason()]}` - List of error reasons when one or more consumers fail to start

  ## Partitioned Topics

  When you subscribe to a partitioned topic, the function automatically:
  1. Queries the broker for partition metadata
  2. Creates separate consumer groups for each partition (named `topic-partition-N`)
  3. Returns a list of consumer group PIDs (one per partition)

  Each partition is treated as an independent consumer group, allowing for:
  - Independent scaling per partition
  - Fault isolation between partitions
  - Consistent supervision model

  ## Examples

      # Regular topic - returns list with single PID
      iex> Pulsar.start_consumer(
      ...>   topic: "persistent://public/default/my-topic",
      ...>   subscription_name: "my-subscription",
      ...>   subscription_type: :Exclusive,
      ...>   callback_module: MyApp.MessageHandler
      ...> )
      {:ok, [#PID<0.456.0>]}

      # Partitioned topic - returns list with one PID per partition
      iex> Pulsar.start_consumer(
      ...>   topic: "persistent://public/default/my-partitioned-topic",
      ...>   subscription_name: "my-subscription",
      ...>   subscription_type: :Shared,
      ...>   callback_module: MyApp.MessageHandler
      ...> )
      {:ok, [#PID<0.456.0>, #PID<0.457.0>, #PID<0.458.0>]}  # 3 partitions

      # With multiple consumers per partition
      iex> Pulsar.start_consumer(
      ...>   topic: "persistent://public/default/my-partitioned-topic",
      ...>   subscription_name: "my-subscription",
      ...>   subscription_type: :Key_Shared,
      ...>   callback_module: MyApp.MessageHandler,
      ...>   opts: [consumer_count: 2]  # 2 consumers per partition
      ...> )
      {:ok, #PID<0.456.0>}

      # With custom flow control settings
      iex> Pulsar.start_consumer(
      ...>   topic: "persistent://public/default/my-topic",
      ...>   subscription_name: "my-subscription",
      ...>   subscription_type: :Shared,
      ...>   callback_module: MyApp.MessageHandler,
      ...>   opts: [
      ...>     flow_initial: 200,
      ...>     flow_threshold: 100,
      ...>     flow_refill: 100
      ...>   ]
      ...> )
      {:ok, #PID<0.456.0>}
  """

  @spec start_consumer(keyword()) :: {:ok, [pid()]} | {:error, [term()]}
  def start_consumer(args) do
    {:ok, topic} = Keyword.fetch(args, :topic)
    {:ok, subscription_name} = Keyword.fetch(args, :subscription_name)
    {:ok, subscription_type} = Keyword.fetch(args, :subscription_type)
    {:ok, callback_module} = Keyword.fetch(args, :callback_module)
    opts = Keyword.get(args, :opts, [])

    topics =
      case check_partitioned_topic(topic) do
        {:ok, 0} -> [topic]
        {:ok, partitions} -> Range.new(0, partitions - 1) |> Enum.map(&"#{topic}-partition-#{&1}")
      end

    consumers =
      topics
      |> Enum.map(fn topic ->
        {:ok, pid} =
          do_start_consumer(
            # TO-DO: we should be able to pass the name from the app configuration
            topic <> "-" <> subscription_name,
            topic,
            subscription_name,
            subscription_type,
            callback_module,
            opts
          )

        pid
      end)

    {:ok, consumers}
  end

  defp do_start_consumer(name, topic, subscription_name, subscription_type, callback_module, opts) do
    consumer_count = Keyword.get(opts, :consumer_count, 1)

    children =
      for i <- 1..consumer_count do
        consumer_id = "#{name}-consumer-#{i}"

        %{
          id: consumer_id,
          start: {
            Pulsar.Consumer,
            :start_link,
            [topic, subscription_name, subscription_type, callback_module, opts]
          },
          restart: :transient,
          type: :worker
        }
      end

    consumer_group_spec = %{
      id: name,
      start:
        {Supervisor, :start_link,
         [
           children,
           [
             strategy: :one_for_one,
             # TO-DO: should be configurable
             max_restarts: 10,
             name: {:via, Registry, {@consumer_registry, name}}
           ]
         ]},
      # TO-DO: should be transient?
      restart: :permanent,
      type: :supervisor
    }

    DynamicSupervisor.start_child(@consumer_supervisor, consumer_group_spec)
  end

  @doc """
  Stops a single consumer group.

  Accepts either:
  - A group PID (from the list returned by `start_consumer/1`)
  - A group ID string (for programmatic access)

  For partitioned topics, you'll need to call this function for each PID
  in the list returned by `start_consumer/1`.

  Returns `:ok` if successful, `{:error, :not_found}` if the group doesn't exist.

  ## Examples

      # Stop single consumer group
      iex> {:ok, [group_pid]} = Pulsar.start_consumer(...)
      iex> Pulsar.stop_consumer(group_pid)
      :ok

      # Stop all partitions of a partitioned topic
      iex> {:ok, group_pids} = Pulsar.start_consumer(...)  # partitioned topic
      iex> Enum.each(group_pids, &Pulsar.stop_consumer/1)
      :ok

      # Stop by group ID string
      iex> Pulsar.stop_consumer("my-topic-partition-0-my-subscription-123456")
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

  @spec stop_consumer(pid() | String.t()) :: :ok | {:error, :not_found}
  def consumers_for_group(group_pid) when is_pid(group_pid) do
    group_pid
    |> Supervisor.which_children()
    |> Enum.map(fn {_, child_pid, _, _} -> child_pid end)
  end

  def consumers_for_group(group_id) when is_binary(group_id) do
    case Registry.lookup(@consumer_registry, group_id) do
      [{group_pid, _value}] ->
        consumers_for_group(group_pid)

      [] ->
        {:error, :not_found}
    end
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
end
