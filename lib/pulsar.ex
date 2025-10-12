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
        registry_opts = [{:name, {:via, Registry, {@registry_name, broker_url}}} | opts]

        child_spec = %{
          id: broker_url,
          start: {Pulsar.Broker, :start_link, [broker_url, registry_opts]},
          # TO-DO: should be transient?
          restart: :permanent
        }

        case DynamicSupervisor.start_child(@supervisor_name, child_spec) do
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
    case Registry.lookup(@registry_name, broker_url) do
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
  Starts consumer group that manages one or more consumer processes.

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

  Returns `{:ok, pid}` where `pid` is the PID of the consumer group supervisor.
  Use this PID with `stop_consumer/1` to stop the entire consumer group.

  ## Examples

      # Start a single consumer (default behavior)
      iex> Pulsar.start_consumer(
      ...>   topic: "persistent://public/default/my-topic",
      ...>   subscription_name: "my-subscription",
      ...>   subscription_type: :Exclusive,
      ...>   callback_module: MyApp.MessageHandler
      ...> )
      {:ok, #PID<0.456.0>}

      # With initialization arguments for the callback module
      iex> Pulsar.start_consumer(
      ...>   topic: "persistent://public/default/my-topic",
      ...>   subscription_name: "my-subscription",
      ...>   subscription_type: :Exclusive,
      ...>   callback_module: MyApp.MessageCounter,
      ...>   opts: [init_args: [max_messages: 100]]
      ...> )
      {:ok, #PID<0.456.0>}

      # With multiple consumers for parallel processing
      iex> Pulsar.start_consumer(
      ...>   topic: "persistent://public/default/my-topic",
      ...>   subscription_name: "my-subscription",
      ...>   subscription_type: :Key_Shared,
      ...>   callback_module: MyApp.MessageHandler,
      ...>   opts: [consumer_count: 3]
      ...> )
      {:ok, #PID<0.456.0>}
  """

  @spec start_consumer(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_consumer(args) do
    {:ok, topic} = Keyword.fetch(args, :topic)
    {:ok, subscription_name} = Keyword.fetch(args, :subscription_name)
    {:ok, subscription_type} = Keyword.fetch(args, :subscription_type)
    {:ok, callback_module} = Keyword.fetch(args, :callback_module)
    opts = Keyword.get(args, :opts, [])

    consumer_count = Keyword.get(opts, :consumer_count, 1)
    init_args = Keyword.get(opts, :init_args, [])

    group_id = unique_group_id(topic, subscription_name)

    children =
      for i <- 1..consumer_count do
        consumer_id = "#{group_id}-consumer-#{i}"

        %{
          id: consumer_id,
          start: {
            Pulsar.Consumer,
            :start_link,
            [topic, subscription_name, subscription_type, callback_module, [init_args: init_args]]
          },
          restart: :transient,
          type: :worker
        }
      end

    consumer_group_spec = %{
      id: group_id,
      start:
        {Supervisor, :start_link,
         [
           children,
           [
             strategy: :one_for_one,
             # TO-DO: should be configurable
             max_restarts: 10,
             name: {:via, Registry, {@consumer_group_registry_name, group_id}}
           ]
         ]},
      # TO-DO: should be transient?
      restart: :permanent,
      type: :supervisor
    }

    DynamicSupervisor.start_child(@consumer_group_supervisor_name, consumer_group_spec)
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
    Supervisor.stop(group_pid)
  end

  def stop_consumer(group_id) when is_binary(group_id) do
    case Registry.lookup(@consumer_group_registry_name, group_id) do
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
    case Registry.lookup(@consumer_group_registry_name, group_id) do
      [{group_pid, _value}] ->
        consumers_for_group(group_pid)

      [] ->
        {:error, :not_found}
    end
  end

  @spec unique_group_id(String.t(), String.t()) :: String.t()
  defp unique_group_id(topic, subscription_name) do
    timestamp = System.unique_integer([:positive])
    "#{topic}-#{subscription_name}-#{timestamp}"
  end
end
