defmodule Pulsar.PartitionedProducer do
  @moduledoc """
  A supervisor that manages individual producer groups for partitioned topics.

  This module provides a logical abstraction over multiple producer groups,
  allowing the `start_producer` API to return a single PID for partitioned topics
  while maintaining the individual producer group architecture underneath.

  The supervisor manages one producer group per partition, with the number of
  partitions provided by the caller.

  ## Message Routing

  Messages are routed to partitions based on the `:partition_key` option:
  - With `:partition_key` - Consistent hash routing via `:erlang.phash2(partition_key, num_partitions)`
  - Without `:partition_key` - Random selection
  """

  use Supervisor

  require Logger

  @default_client :default

  @doc """
  Starts a partitioned producer supervisor.

  ## Parameters

  - `name` - Unique name for this partitioned producer
  - `topic` - The base partitioned topic name (without partition suffix)
  - `partitions` - Number of partitions for this topic
  - `opts` - Additional options passed to individual producer groups

  ## Returns

  `{:ok, pid}` - The supervisor PID that manages all partition producer groups
  `{:error, reason}` - Error if the supervisor failed to start
  """
  def start_link(name, topic, partitions, opts \\ []) do
    client = Keyword.get(opts, :client, @default_client)
    producer_registry = Pulsar.Client.producer_registry(client)

    Supervisor.start_link(
      __MODULE__,
      {name, topic, partitions, opts},
      name: {:via, Registry, {producer_registry, name}}
    )
  end

  @doc """
  Stops a partitioned producer supervisor and all its child producer groups.
  """
  def stop(supervisor_pid, reason \\ :normal, timeout \\ :infinity) do
    Supervisor.stop(supervisor_pid, reason, timeout)
  end

  @doc """
  Gets information about all child producer groups managed by this supervisor.

  Returns a list of `{partition_topic, group_pid}` tuples.
  """
  def get_partition_groups(supervisor_pid) do
    supervisor_pid
    |> Supervisor.which_children()
    |> Enum.filter(fn {_id, _pid, type, _modules} -> type == :supervisor end)
    |> Enum.map(fn {partition_topic, group_pid, _type, _modules} ->
      {partition_topic, group_pid}
    end)
  end

  @doc """
  Gets all producer processes from all partition groups managed by this supervisor.

  Returns a flat list of producer process PIDs from all partitions.
  """
  def get_producers(supervisor_pid) do
    supervisor_pid
    |> get_partition_groups()
    |> Enum.flat_map(fn {_partition_topic, group_pid} ->
      Pulsar.ProducerGroup.get_producers(group_pid)
    end)
  end

  @doc """
  Sends a message through the partitioned producer.

  Routes the message to a partition based on the `:partition_key` option:
  - With `:partition_key` - Uses consistent hashing to route to a specific partition
  - Without `:partition_key` - Uses random partition selection

  ## Parameters

  - `supervisor_pid` - The partitioned producer supervisor PID
  - `message` - Binary message payload
  - `opts` - Optional parameters (same as `Pulsar.ProducerGroup.send_message/3`)

  ## Returns

  `{:ok, message_id}` on success
  `{:error, reason}` on failure
  """
  @spec send_message(pid(), binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def send_message(supervisor_pid, message, opts \\ []) do
    partition_groups = get_partition_groups(supervisor_pid)
    num_partitions = length(partition_groups)

    case route_partition(partition_groups, num_partitions, opts) do
      {:ok, group_pid} -> Pulsar.ProducerGroup.send_message(group_pid, message, opts)
      {:error, reason} -> {:error, reason}
    end
  end

  # Selects the target partition group for a message.
  #
  # Routing resolves the partition's actual index, parsed from its topic suffix,
  # rather than its position in a sorted list. Sorting topic names
  # lexicographically would misorder partitions once there are 10+
  # (e.g. "...-partition-10" sorts before "...-partition-2").
  defp route_partition(_partition_groups, 0, _opts), do: {:error, :no_producers_available}

  defp route_partition(partition_groups, num_partitions, opts) do
    partition_index = select_partition(opts, num_partitions)

    case Enum.find(partition_groups, fn {topic, _pid} -> Pulsar.PartitionTopic.index(topic) == partition_index end) do
      {_topic, group_pid} -> {:ok, group_pid}
      nil -> {:error, {:partition_not_found, partition_index}}
    end
  end

  @impl true
  def init({name, topic, partitions, opts}) do
    Logger.info("Starting partitioned producer for topic #{topic} with #{partitions} partitions")

    build_child_spec = fn partition_index ->
      partition_child_spec(partition_index, name, topic, opts)
    end

    partition_children = Enum.map(0..(partitions - 1), build_child_spec)

    discovery_children =
      Pulsar.PartitionDiscovery.child_specs(self(),
        topic: topic,
        client: Keyword.get(opts, :client, @default_client),
        interval_ms: Keyword.get(opts, :partition_discovery_interval_ms, Pulsar.Config.partition_discovery_interval()),
        build_child_spec: build_child_spec
      )

    Supervisor.init(partition_children ++ discovery_children, strategy: :one_for_one)
  end

  # Builds the ProducerGroup child spec for a single partition.
  defp partition_child_spec(partition_index, name, topic, opts) do
    partition_topic = Pulsar.PartitionTopic.name(topic, partition_index)
    partition_group_name = Pulsar.PartitionTopic.name(name, partition_index)

    %{
      id: partition_topic,
      start: {
        Pulsar.ProducerGroup,
        :start_link,
        [
          partition_group_name,
          partition_topic,
          opts
        ]
      },
      restart: :permanent,
      type: :supervisor
    }
  end

  defp select_partition(opts, num_partitions) do
    case Keyword.get(opts, :partition_key) do
      nil ->
        Enum.random(0..(num_partitions - 1))

      partition_key ->
        :erlang.phash2(partition_key, num_partitions)
    end
  end
end
