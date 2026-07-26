defmodule Pulsar.Producer.Partitioned do
  @moduledoc false

  # Supervises one producer group per partition of a partitioned topic.
  # Messages are routed to a partition by :partition_key, hashed with
  # :erlang.phash2/2, or at random when no key is given.

  use Supervisor

  require Logger

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    client = Keyword.fetch!(opts, :client)

    Supervisor.start_link(__MODULE__, opts, name: {:via, Registry, {Pulsar.Client.producer_registry(client), name}})
  end

  def stop(supervisor_pid, reason \\ :normal, timeout \\ :infinity) do
    Supervisor.stop(supervisor_pid, reason, timeout)
  end

  @impl true
  def init(opts) do
    topic = Keyword.fetch!(opts, :topic)
    partitions = Keyword.fetch!(opts, :partitions)

    Logger.info("Starting partitioned producer for topic #{topic} with #{partitions} partitions")

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
      start: {Pulsar.Producer.Group, :start_link, [partition_opts]},
      restart: :permanent,
      type: :supervisor
    }
  end
end
