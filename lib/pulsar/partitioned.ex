defmodule Pulsar.Partitioned do
  @moduledoc false

  # Supervises one group per partition of a partitioned topic, for consumers and for
  # producers alike: a partition's group differs from its siblings only in its :topic and
  # :name, which is true whichever kind of group it is.
  #
  # Started by Pulsar.Consumer and Pulsar.Producer, which own the option surface and pass
  # the group module and the registry to register under.

  use Supervisor

  require Logger

  @spec start_link(module(), atom(), atom(), keyword()) :: Supervisor.on_start()
  def start_link(worker, registry, count_key, opts) do
    name = Keyword.fetch!(opts, :name)

    Supervisor.start_link(__MODULE__, {worker, registry, count_key, opts}, name: {:via, Registry, {registry, name}})
  end

  def stop(supervisor_pid, reason \\ :normal, timeout \\ :infinity) do
    Supervisor.stop(supervisor_pid, reason, timeout)
  end

  @impl true
  def init({worker, registry, count_key, opts}) do
    topic = Keyword.fetch!(opts, :topic)
    partitions = Keyword.fetch!(opts, :partitions)

    Logger.info("Starting partitioned #{inspect(worker)} for topic #{topic} with #{partitions} partitions")

    build_child_spec = &partition_child_spec(&1, worker, registry, count_key, opts)

    discovery_children =
      Pulsar.PartitionDiscovery.child_specs(self(),
        topic: topic,
        client: Keyword.fetch!(opts, :client),
        interval_ms: Keyword.fetch!(opts, :partition_discovery_interval_ms),
        build_child_spec: build_child_spec
      )

    partition_children = Enum.map(0..(partitions - 1), build_child_spec)

    Supervisor.init(partition_children ++ discovery_children, strategy: :one_for_one)
  end

  defp partition_child_spec(partition_index, worker, registry, count_key, opts) do
    partition_topic = Pulsar.PartitionTopic.name(Keyword.fetch!(opts, :topic), partition_index)

    partition_opts =
      opts
      |> Keyword.put(:topic, partition_topic)
      |> Keyword.put(:name, Pulsar.PartitionTopic.name(Keyword.fetch!(opts, :name), partition_index))

    %{
      id: partition_topic,
      start: {Pulsar.Group, :start_link, [worker, registry, count_key, partition_opts]},
      restart: :permanent,
      type: :supervisor
    }
  end
end
