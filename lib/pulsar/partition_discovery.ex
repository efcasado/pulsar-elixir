defmodule Pulsar.PartitionDiscovery do
  @moduledoc """
  Periodically polls a partitioned topic's metadata and grows a partitioned
  consumer/producer supervisor when new partitions are added to the topic.

  Pulsar only ever increases a topic's partition count, so discovery is
  grow-only: any partition indices that are missing from the supervisor are
  added, existing partitions are left untouched, and a reported count that is
  not larger than the current one (including a transient lookup error) is
  ignored.

  This runs as a `:worker` child of the `Pulsar.PartitionedConsumer` /
  `Pulsar.PartitionedProducer` supervisor it manages, and adds new partition
  children to that same supervisor via `Supervisor.start_child/2`.
  """

  use GenServer

  require Logger

  @default_interval_ms 60_000

  @doc "The default poll interval, in milliseconds, when none is configured."
  @spec default_interval_ms() :: pos_integer()
  def default_interval_ms, do: @default_interval_ms

  @doc """
  Returns the child spec(s) for a discovery poller attached to `supervisor`.

  Returns a single-element list when discovery is enabled, or an empty list when
  it is disabled, so the supervisor never starts a poller process it doesn't
  need.

  `opts`:
    * `:topic` - base partitioned topic name (required)
    * `:client` - client name (required)
    * `:build_child_spec` - 1-arity fun mapping a partition index to the child
      spec for that partition's consumer/producer group (required)
    * `:interval_ms` - poll interval in milliseconds (required), or `false` to
      disable discovery.
  """
  @spec child_specs(pid(), keyword()) :: [Supervisor.child_spec()]
  def child_specs(supervisor, opts) do
    if enabled?(Keyword.fetch!(opts, :interval_ms)) do
      [
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [supervisor, opts]},
          restart: :permanent,
          type: :worker
        }
      ]
    else
      []
    end
  end

  def start_link(supervisor, opts) do
    GenServer.start_link(__MODULE__, {supervisor, opts})
  end

  @impl true
  def init({supervisor, opts}) do
    interval = Keyword.fetch!(opts, :interval_ms)

    state = %{
      supervisor: supervisor,
      topic: Keyword.fetch!(opts, :topic),
      client: Keyword.fetch!(opts, :client),
      interval: interval,
      build_child_spec: Keyword.fetch!(opts, :build_child_spec)
    }

    schedule(interval)

    {:ok, state}
  end

  @impl true
  def handle_info(:discover, state) do
    discover(state)
    schedule(state.interval)
    {:noreply, state}
  end

  defp discover(state) do
    case Pulsar.ServiceDiscovery.partition_count(state.topic, client: state.client) do
      {:ok, desired} ->
        grow(state, desired)

      {:error, reason} ->
        Logger.warning("Partition discovery for #{state.topic} skipped: #{inspect(reason)}")
    end
  end

  defp grow(state, desired) when desired > 0 do
    existing = existing_partition_indices(state.supervisor)
    missing = Enum.reject(0..(desired - 1), &MapSet.member?(existing, &1))
    add_partitions(state, missing)
  end

  defp grow(_state, _desired), do: :ok

  defp add_partitions(_state, []), do: :ok

  defp add_partitions(state, missing) do
    Logger.info("Partition discovery: adding partition(s) #{inspect(missing)} for #{state.topic}")

    Enum.each(missing, fn index ->
      case Supervisor.start_child(state.supervisor, state.build_child_spec.(index)) do
        {:ok, _pid} ->
          :ok

        {:ok, _pid, _info} ->
          :ok

        {:error, {:already_started, _pid}} ->
          :ok

        {:error, reason} ->
          Logger.error("Partition discovery: failed to start partition #{index} for #{state.topic}: #{inspect(reason)}")
      end
    end)
  end

  defp existing_partition_indices(supervisor) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.filter(fn {_id, _pid, type, _modules} -> type == :supervisor end)
    |> MapSet.new(fn {id, _pid, _type, _modules} -> Pulsar.PartitionTopic.index(id) end)
  end

  defp schedule(interval), do: Process.send_after(self(), :discover, interval)

  defp enabled?(false), do: false
  defp enabled?(interval) when is_integer(interval) and interval > 0, do: true
  defp enabled?(_interval), do: false
end
