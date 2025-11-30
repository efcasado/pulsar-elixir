defmodule Pulsar.PartitionedConsumer do
  @moduledoc """
  A supervisor that manages individual consumer groups for partitioned topics.

  This module provides a logical abstraction over multiple consumer groups,
  allowing the `start_consumer` API to return a single PID for partitioned topics
  while maintaining the individual consumer group architecture underneath.

  The supervisor manages one consumer group per partition, with the number of
  partitions provided by the caller.
  """

  use Supervisor

  require Logger

  @default_client :default

  @doc """
  Starts a partitioned consumer supervisor.

  ## Parameters

  - `name` - Unique name for this partitioned consumer
  - `topic` - The base partitioned topic name (without partition suffix)
  - `partitions` - Number of partitions for this topic
  - `subscription_name` - Name of the subscription
  - `subscription_type` - Type of subscription (e.g., :Exclusive, :Shared, :Key_Shared)
  - `callback_module` - Module that implements `Pulsar.Consumer.Callback` behaviour
  - `opts` - Additional options passed to individual consumer groups

  ## Returns

  `{:ok, pid}` - The supervisor PID that manages all partition consumer groups
  `{:error, reason}` - Error if the supervisor failed to start
  """
  def start_link(name, topic, partitions, subscription_name, subscription_type, callback_module, opts \\ []) do
    client = Keyword.get(opts, :client, @default_client)
    consumer_registry = Pulsar.Client.consumer_registry(client)

    Supervisor.start_link(
      __MODULE__,
      {name, topic, partitions, subscription_name, subscription_type, callback_module, opts},
      name: {:via, Registry, {consumer_registry, name}}
    )
  end

  @doc """
  Stops a partitioned consumer supervisor and all its child consumer groups.
  """
  def stop(supervisor_pid, reason \\ :normal, timeout \\ :infinity) do
    Supervisor.stop(supervisor_pid, reason, timeout)
  end

  @doc """
  Gets information about all child consumer groups managed by this supervisor.

  Returns a list of `{partition_topic, group_pid}` tuples.
  """
  def get_partition_groups(supervisor_pid) do
    supervisor_pid
    |> Supervisor.which_children()
    |> Enum.map(fn {partition_topic, group_pid, :supervisor, _modules} ->
      {partition_topic, group_pid}
    end)
  end

  @doc """
  Gets all consumer processes from all partition groups managed by this supervisor.

  Returns a flat list of consumer process PIDs from all partitions.
  """
  def get_consumers(supervisor_pid) do
    supervisor_pid
    |> get_partition_groups()
    |> Enum.flat_map(fn {_partition_topic, group_pid} ->
      Pulsar.ConsumerGroup.get_consumers(group_pid)
    end)
  end

  @impl true
  def init({name, topic, partitions, subscription_name, subscription_type, callback_module, opts}) do
    Logger.info("Starting partitioned consumer for topic #{topic} with #{partitions} partitions")

    children =
      0
      |> Range.new(partitions - 1)
      |> Enum.map(fn partition_index ->
        partition_topic = "#{topic}-partition-#{partition_index}"
        partition_group_name = "#{name}-partition-#{partition_index}"

        # Create ConsumerGroup spec for this partition
        %{
          id: partition_topic,
          start: {
            Pulsar.ConsumerGroup,
            :start_link,
            [
              partition_group_name,
              partition_topic,
              subscription_name,
              subscription_type,
              callback_module,
              opts
            ]
          },
          restart: :permanent,
          type: :supervisor
        }
      end)

    Supervisor.init(children, strategy: :one_for_one)
  end
end
