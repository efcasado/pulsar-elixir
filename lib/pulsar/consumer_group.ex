defmodule Pulsar.ConsumerGroup do
  @moduledoc """
  Consumer Group supervisor that manages multiple consumer instances for a single subscription.

  A consumer group represents a logical grouping of consumer processes that share the same:
  - Topic
  - Subscription name  
  - Subscription type
  - Callback module

  The consumer group acts as a supervisor for these consumer instances, providing:
  - Fault isolation per consumer group
  - Simplified management (start/stop the group as a unit)
  - Independent restart strategies per group
  - Easy scaling (add/remove consumers from a group)

  ## Examples

      # Start a consumer group with multiple consumers
      {:ok, group_pid} = Pulsar.ConsumerGroup.start_link(
        topic: "my-topic",
        subscription_name: "my-subscription", 
        subscription_type: :Shared,
        callback_module: MyApp.MessageHandler,
        consumer_count: 3
      )

      # Get information about the consumer group
      info = Pulsar.ConsumerGroup.get_info(group_pid)

      # Stop the entire consumer group
      Pulsar.ConsumerGroup.stop(group_pid)
  """

  use Supervisor
  require Logger

  defstruct [
    :topic,
    :subscription_name,
    :subscription_type,
    :callback_module,
    :consumer_count,
    :init_args,
    :group_id
  ]

  @type t :: %__MODULE__{
          topic: String.t(),
          subscription_name: String.t(),
          subscription_type: atom(),
          callback_module: module(),
          consumer_count: pos_integer(),
          init_args: term(),
          group_id: String.t()
        }

  ## Public API

  @doc """
  Starts a consumer group supervisor.

  ## Options

  - `:topic` - The topic to subscribe to (required)
  - `:subscription_name` - Name of the subscription (required)
  - `:subscription_type` - Type of subscription (required, e.g., :Exclusive, :Shared)
  - `:callback_module` - Module that implements `Pulsar.Consumer.Callback` behaviour (required)
  - `:consumer_count` - Number of consumer processes to start (default: 1)
  - `:init_args` - Arguments passed to callback module's init/1 function (default: [])
  - `:name` - Name for the supervisor process (optional)

  Returns `{:ok, pid()}` on success, `{:error, reason}` on failure.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    # Extract required parameters
    topic = Keyword.fetch!(opts, :topic)
    subscription_name = Keyword.fetch!(opts, :subscription_name)
    subscription_type = Keyword.fetch!(opts, :subscription_type)
    callback_module = Keyword.fetch!(opts, :callback_module)

    # Extract optional parameters
    consumer_count = Keyword.get(opts, :consumer_count, 1)
    init_args = Keyword.get(opts, :init_args, [])
    name = Keyword.get(opts, :name)

    # Generate unique group ID
    group_id = generate_group_id(topic, subscription_name)

    config = %__MODULE__{
      topic: topic,
      subscription_name: subscription_name,
      subscription_type: subscription_type,
      callback_module: callback_module,
      consumer_count: consumer_count,
      init_args: init_args,
      group_id: group_id
    }

    supervisor_opts = if name, do: [name: name], else: []
    Supervisor.start_link(__MODULE__, config, supervisor_opts)
  end

  @doc """
  Gets information about a consumer group.

  Returns a map with details about the consumer group including:
  - Consumer group configuration
  - List of consumer PIDs
  - Consumer count
  """
  @spec get_info(pid()) :: map()
  def get_info(group_pid) do
    children = Supervisor.which_children(group_pid)
    consumer_pids = Enum.map(children, fn {_id, pid, _type, _modules} -> pid end)

    %{
      group_pid: group_pid,
      consumer_pids: consumer_pids,
      consumer_count: length(consumer_pids),
      active_consumers: length(Enum.filter(consumer_pids, &Process.alive?/1))
    }
  end

  @doc """
  Lists all consumer PIDs in the group.
  """
  @spec list_consumers(pid()) :: [pid()]
  def list_consumers(group_pid) do
    group_pid
    |> Supervisor.which_children()
    |> Enum.map(fn {_id, pid, _type, _modules} -> pid end)
  end

  @doc """
  Gets the count of consumers in the group.
  """
  @spec consumer_count(pid()) :: non_neg_integer()
  def consumer_count(group_pid) do
    group_pid
    |> Supervisor.count_children()
    |> Map.get(:active, 0)
  end

  @doc """
  Stops a consumer group and all its consumers.
  """
  @spec stop(pid()) :: :ok
  def stop(group_pid) do
    Process.exit(group_pid, :shutdown)
    :ok
  end

  ## Supervisor Callbacks

  @impl Supervisor
  def init(config) do
    %__MODULE__{
      topic: topic,
      subscription_name: subscription_name,
      subscription_type: subscription_type,
      callback_module: callback_module,
      consumer_count: consumer_count,
      init_args: init_args,
      group_id: group_id
    } = config

    Logger.info("Starting consumer group #{group_id} with #{consumer_count} consumers")

    # Create child specifications for each consumer
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

    # Use :one_for_one strategy - if one consumer fails, only restart that consumer
    opts = [strategy: :one_for_one]

    Supervisor.init(children, opts)
  end

  ## Public Utility Functions

  @doc """
  Generates a unique group ID based on topic and subscription name.

  Used internally and by the main Pulsar module for consistency.
  """
  @spec generate_group_id(String.t(), String.t()) :: String.t()
  def generate_group_id(topic, subscription_name) do
    timestamp = System.unique_integer([:positive])
    "#{topic}-#{subscription_name}-#{timestamp}"
  end
end
