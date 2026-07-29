defmodule Pulsar.Topic do
  @moduledoc false

  # Everything a consumer or producer needs for one topic: how wide it is, the naming
  # convention its partitions follow, and the supervisor over a `Pulsar.Group` per partition.
  #
  # `start_link/4` is the single entry point — it resolves the width and starts the shape that
  # fits, so `Pulsar.Consumer` and `Pulsar.Producer` do not each carry that decision.

  use Supervisor

  alias Pulsar.Group
  alias Pulsar.ServiceDiscovery
  alias Pulsar.Topic.Discovery

  require Logger

  @separator "-partition-"

  @doc """
  Builds the partition name for `base` (a topic or group name) at `index`.

      iex> Pulsar.Topic.partition("persistent://public/default/t", 3)
      "persistent://public/default/t-partition-3"
  """
  @spec partition(String.t(), non_neg_integer()) :: String.t()
  def partition(base, index), do: "#{base}#{@separator}#{index}"

  @doc """
  Extracts the numeric partition index from a partition topic/group name.

      iex> Pulsar.Topic.index("persistent://public/default/t-partition-3")
      3
  """
  @spec index(String.t() | atom()) :: non_neg_integer()
  def index(partition_name) do
    partition_name
    |> to_string()
    |> String.split(@separator)
    |> List.last()
    |> String.to_integer()
  end

  @doc """
  Strips the partition suffix, returning the name a partition belongs to. A name without
  one is returned unchanged.

      iex> Pulsar.Topic.base("persistent://public/default/t-partition-3")
      "persistent://public/default/t"

      iex> Pulsar.Topic.base("persistent://public/default/t")
      "persistent://public/default/t"

  Only a trailing index is a partition suffix; the convention is not reserved, so a topic
  may contain the separator itself:

      iex> Pulsar.Topic.base("persistent://public/default/events-partition-archive-partition-0")
      "persistent://public/default/events-partition-archive"
  """
  @spec base(String.t() | atom()) :: String.t()
  def base(partition_name) do
    partition_name
    |> to_string()
    |> String.replace(~r/#{Regex.escape(@separator)}\d+$/, "")
  end

  @doc """
  Starts the supervision shape a topic needs: one `Pulsar.Group` when it is not partitioned,
  or one per partition plus a poller for partitions added later.
  """
  @spec start_link(module(), atom(), atom(), keyword()) :: Supervisor.on_start()
  def start_link(worker, registry, count_key, opts) do
    with {:ok, partitions} <- width(opts) do
      start_link(worker, registry, count_key, opts, partitions)
    end
  end

  defp start_link(worker, registry, count_key, opts, 0) do
    Group.start_link(worker, registry, count_key, opts)
  end

  defp start_link(worker, registry, count_key, opts, partitions) do
    opts = Keyword.put(opts, :partitions, partitions)
    name = Keyword.fetch!(opts, :name)

    Supervisor.start_link(__MODULE__, {worker, registry, count_key, opts}, name: {:via, Registry, {registry, name}})
  end

  # Resolved by the caller for Pulsar.Consumer.start/4 and Pulsar.Producer.start/2, so that
  # the lookup and its retries do not run inside the client's supervisor.
  defp width(opts) do
    case Keyword.fetch(opts, :partitions) do
      {:ok, partitions} ->
        {:ok, partitions}

      :error ->
        ServiceDiscovery.partition_count_with_retry(Keyword.fetch!(opts, :topic), client: Keyword.fetch!(opts, :client))
    end
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
      Discovery.child_specs(self(),
        topic: topic,
        client: Keyword.fetch!(opts, :client),
        interval_ms: Keyword.fetch!(opts, :partition_discovery_interval_ms),
        build_child_spec: build_child_spec
      )

    partition_children = Enum.map(0..(partitions - 1), build_child_spec)

    Supervisor.init(partition_children ++ discovery_children, strategy: :one_for_one)
  end

  defp partition_child_spec(partition_index, worker, registry, count_key, opts) do
    partition_topic = partition(Keyword.fetch!(opts, :topic), partition_index)

    partition_opts =
      opts
      |> Keyword.put(:topic, partition_topic)
      |> Keyword.put(:name, partition(Keyword.fetch!(opts, :name), partition_index))

    %{
      id: partition_topic,
      start: {Group, :start_link, [worker, registry, count_key, partition_opts]},
      restart: :permanent,
      type: :supervisor
    }
  end
end
