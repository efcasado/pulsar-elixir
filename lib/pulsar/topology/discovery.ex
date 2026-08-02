defmodule Pulsar.Topology.Discovery do
  @moduledoc false

  # Initializes a topology from broker metadata, then keeps a partitioned one reconciled as
  # its partition count grows. Resolution and retry happen in this process so the stable
  # Pulsar.Topology root can start even while no broker is available.

  use GenServer

  alias Pulsar.Backoff
  alias Pulsar.ServiceDiscovery
  alias Pulsar.Topology

  require Logger

  @spec start_link({pid(), map(), keyword()}) :: GenServer.on_start()
  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init({topology, config, controller_opts}) do
    opts = config.opts
    initial_partitions = Keyword.get(opts, :partitions)

    state = %{
      topology: topology,
      config: config,
      topic: Keyword.fetch!(opts, :topic),
      client: Keyword.fetch!(opts, :client),
      count: Keyword.fetch!(opts, config.count_key),
      interval: Keyword.fetch!(opts, :partition_discovery_interval_ms),
      resolver: Keyword.get(controller_opts, :resolver, &ServiceDiscovery.partition_count/2),
      status: status(initial_partitions),
      expected_workers: expected_workers(initial_partitions, Keyword.fetch!(opts, config.count_key)),
      waiters: [],
      backoff: 0
    }

    # Topology.init/1 has already constructed groups when its caller supplied a partition
    # hint. Without one, this controller must perform the initial metadata lookup itself.
    case Keyword.fetch(opts, :partitions) do
      {:ok, _partitions} -> {:ok, schedule_poll(state)}
      :error -> {:ok, state, {:continue, :discover}}
    end
  end

  @impl true
  def handle_continue(:discover, state), do: discover(state)

  @impl true
  def handle_call(:status, _from, state), do: {:reply, state.status, state}

  def handle_call(:expected_workers, _from, %{expected_workers: nil} = state) do
    {:reply, {:error, :not_ready}, state}
  end

  def handle_call(:expected_workers, _from, state) do
    {:reply, {:ok, state.expected_workers}, state}
  end

  def handle_call(:await_initialized, from, %{expected_workers: nil} = state) do
    {:noreply, %{state | waiters: [from | state.waiters]}}
  end

  def handle_call(:await_initialized, _from, state) do
    {:reply, {:ok, state.expected_workers}, state}
  end

  @impl true
  def handle_info(:discover, state), do: discover(state)

  defp discover(state) do
    case resolve_metadata(state) do
      {:ok, partitions} ->
        ready(state, partitions)

      {:error, reason} ->
        wait = Backoff.next(state.backoff)

        Logger.warning("Topology discovery for #{state.topic} failed: #{inspect(reason)}; retrying in #{wait}ms")

        Process.send_after(self(), :discover, wait)
        {:noreply, %{state | backoff: wait}}
    end
  end

  defp resolve_metadata(state) do
    case state.resolver.(state.topic, client: state.client) do
      {:ok, desired} when is_integer(desired) and desired >= 0 ->
        Topology.reconcile(state.topology, desired, state.config)

      {:ok, invalid} ->
        {:error, {:invalid_partition_count, invalid}}

      {:error, _reason} = error ->
        error
    end
  catch
    kind, reason -> {:error, {:resolver_failed, kind, reason}}
  end

  defp ready(state, desired) do
    expected_workers = expected_workers(desired, state.count)
    Enum.each(state.waiters, &GenServer.reply(&1, {:ok, expected_workers}))

    status = status(desired)
    state = %{state | status: status, expected_workers: expected_workers, waiters: [], backoff: 0}
    {:noreply, schedule_poll(state)}
  end

  defp schedule_poll(%{interval: false} = state), do: state

  defp schedule_poll(state) do
    Process.send_after(self(), :discover, state.interval)
    state
  end

  defp status(nil), do: :initializing
  defp status(0), do: {:ready, :non_partitioned}
  defp status(partitions), do: {:ready, {:partitioned, partitions}}

  defp expected_workers(nil, _count), do: nil
  defp expected_workers(0, count), do: count
  defp expected_workers(partitions, count), do: partitions * count
end
