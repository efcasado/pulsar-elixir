defmodule Pulsar.Topology.Discovery do
  @moduledoc false

  # Initializes a topology from broker metadata, polls for partition growth when enabled,
  # and reconciles known groups locally. Resolution and retry happen in this process so the
  # stable Pulsar.Topology root can start even while no broker is available.

  use GenServer

  alias Pulsar.Backoff
  alias Pulsar.Topology
  alias Pulsar.Topology.Resolver

  require Logger

  @spec start_link({pid(), map(), keyword()}) :: GenServer.on_start()
  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init({topology, config, controller_opts}) do
    opts = config.opts

    state = %{
      topology: topology,
      config: config,
      topic: Keyword.fetch!(opts, :topic),
      client: Keyword.fetch!(opts, :client),
      discovery_interval: Keyword.fetch!(opts, :partition_discovery_interval_ms),
      resolver: Keyword.get(controller_opts, :resolver, &Resolver.partition_count/2),
      status: :initializing,
      desired: nil,
      discovery_backoff: 0
    }

    {:ok, state, {:continue, :discover}}
  end

  @impl true
  def handle_continue(:discover, state), do: discover(state)

  @impl true
  def handle_call(:status, _from, state), do: {:reply, state.status, state}

  @impl true
  def handle_info(:discover, state), do: discover(state)

  defp discover(state) do
    Logger.debug("Discovering topology metadata for #{state.topic}")

    case resolve_metadata(state) do
      {:ok, desired} ->
        Logger.debug("Topology metadata for #{state.topic} reports #{desired} partitions")

        case reconcile(state, desired) do
          {:ok, outcome} -> ready(state, outcome)
          {:error, reason} -> retry_discovery(state, :reconciliation, reason)
        end

      {:error, reason} ->
        retry_discovery(state, :metadata, reason)
    end
  end

  defp retry_discovery(state, stage, reason) do
    wait = Backoff.next(state.discovery_backoff)

    Logger.warning("Topology #{stage_label(stage)} for #{state.topic} failed: #{inspect(reason)}; retrying in #{wait}ms")

    Process.send_after(self(), :discover, wait)
    {:noreply, %{state | discovery_backoff: wait}}
  end

  defp stage_label(:metadata), do: "metadata discovery"
  defp stage_label(:reconciliation), do: "reconciliation after metadata discovery"

  defp resolve_metadata(state) do
    span(:discovery, state, %{}, fn -> do_resolve_metadata(state) end)
  end

  defp do_resolve_metadata(state) do
    case state.resolver.(state.topic, client: state.client) do
      {:ok, desired} when is_integer(desired) and desired >= 0 ->
        {:ok, desired}

      {:ok, invalid} ->
        {:error, {:invalid_partition_count, invalid}}

      {:error, _reason} = error ->
        error
    end
  catch
    kind, reason -> {:error, {:resolver_failed, kind, reason}}
  end

  defp reconcile(state, desired) do
    Logger.debug("Reconciling topology for #{state.topic}: desired_partitions=#{desired}")

    case span(:reconciliation, state, %{desired_partition_count: desired}, fn ->
           reconcile_topology(state, desired)
         end) do
      {:ok, outcome} = result ->
        summary = "partitions=#{outcome.partition_count}, added_groups=#{inspect(outcome.added_groups)}"

        cond do
          state.status == :initializing ->
            Logger.info("Topology for #{state.topic} is ready: #{summary}")

          outcome.added_groups != [] ->
            Logger.info("Topology reconciliation for #{state.topic} changed topology: #{summary}")

          true ->
            Logger.debug("Topology reconciliation for #{state.topic} completed: #{summary}")
        end

        result

      {:error, _reason} = error ->
        error
    end
  end

  defp reconcile_topology(state, desired) do
    case Topology.reconcile(state.topology, desired, state.config) do
      {:ok, outcome} ->
        {:ok, Map.put(outcome, :desired_partition_count, desired)}

      {:error, _reason} = error ->
        error
    end
  end

  defp ready(state, outcome) do
    state = %{
      state
      | status: status(outcome.partition_count),
        desired: outcome.partition_count,
        discovery_backoff: 0
    }

    {:noreply, schedule_discovery(state)}
  end

  defp schedule_discovery(%{desired: 0} = state), do: state
  defp schedule_discovery(%{discovery_interval: false} = state), do: state

  defp schedule_discovery(state) do
    Process.send_after(self(), :discover, state.discovery_interval)
    state
  end

  defp span(event, state, metadata, fun) do
    metadata = Map.merge(%{topic: state.topic, client: state.client, status: state.status}, metadata)

    :telemetry.span([:pulsar, :topology, event], metadata, fn ->
      result = fun.()
      {result, Map.merge(metadata, result_metadata(result))}
    end)
  end

  defp result_metadata({:ok, partitions}) when is_integer(partitions) do
    %{success: true, partition_count: partitions}
  end

  defp result_metadata({:ok, outcome}), do: Map.put(outcome, :success, true)
  defp result_metadata({:error, reason}), do: %{success: false, error: reason}

  defp status(0), do: {:ready, :non_partitioned}
  defp status(partitions), do: {:ready, {:partitioned, partitions}}
end
