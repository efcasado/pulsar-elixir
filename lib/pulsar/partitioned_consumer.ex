defmodule Pulsar.PartitionedConsumer do
  @moduledoc false

  # Supervises one consumer group per partition of a partitioned topic. Started by
  # Pulsar.Consumer, which owns the option surface; a partition's group differs only in
  # its :topic and :name, so the rest is threaded through untouched.

  use Supervisor

  require Logger

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    client = Keyword.fetch!(opts, :client)

    Supervisor.start_link(__MODULE__, opts, name: {:via, Registry, {Pulsar.Client.consumer_registry(client), name}})
  end

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
    |> Enum.filter(fn {_id, _pid, type, _modules} -> type == :supervisor end)
    |> Enum.map(fn {partition_topic, group_pid, _type, _modules} ->
      {partition_topic, group_pid}
    end)
  end

  @doc """
  Gets all consumer processes from all partition groups managed by this supervisor.
  """
  def get_consumers(supervisor_pid) do
    supervisor_pid
    |> get_partition_groups()
    |> Enum.flat_map(fn {_partition_topic, group_pid} ->
      Pulsar.ConsumerGroup.get_consumers(group_pid)
    end)
  end

  @impl true
  def init(opts) do
    topic = Keyword.fetch!(opts, :topic)
    partitions = Keyword.fetch!(opts, :partitions)

    Logger.info("Starting partitioned consumer for topic #{topic} with #{partitions} partitions")

    build_child_spec = &partition_child_spec(&1, opts)

    discovery_children =
      Pulsar.PartitionDiscovery.child_specs(self(),
        topic: topic,
        client: Keyword.fetch!(opts, :client),
        interval_ms: Keyword.get(opts, :partition_discovery_interval_ms, Pulsar.Config.partition_discovery_interval()),
        build_child_spec: build_child_spec
      )

    partition_children = Enum.map(0..(partitions - 1), build_child_spec)

    Supervisor.init(partition_children ++ discovery_children, strategy: :one_for_one)
  end

  defp partition_child_spec(partition_index, opts) do
    partition_topic = Pulsar.PartitionTopic.name(Keyword.fetch!(opts, :topic), partition_index)

    partition_opts =
      opts
      |> Keyword.put(:topic, partition_topic)
      |> Keyword.put(:name, Pulsar.PartitionTopic.name(Keyword.fetch!(opts, :name), partition_index))

    %{
      id: partition_topic,
      start: {Pulsar.ConsumerGroup, :start_link, [partition_opts]},
      restart: :permanent,
      type: :supervisor
    }
  end
end
