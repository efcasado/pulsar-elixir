defmodule Pulsar.Client.Bootstrap do
  @moduledoc false

  use GenServer

  alias Pulsar.Backoff
  alias Pulsar.Client

  require Logger

  @settle_ms 100

  def start_link({kind, opts}) do
    GenServer.start_link(__MODULE__, {kind, opts})
  end

  defp module_for(:consumers), do: Pulsar.Consumer
  defp module_for(:producers), do: Pulsar.Producer

  @impl true
  def init({kind, opts}) do
    client = Keyword.fetch!(opts, :name)

    {:ok, _broker} = Client.start_broker(Keyword.fetch!(opts, :host), client: client)

    pending = Enum.map(Keyword.fetch!(opts, kind), &{module_for(kind), &1})

    state = %{client: client, kind: kind, pending: pending, declared: length(pending), backoff: 0}

    {:ok, state, {:continue, :start_declared}}
  end

  @impl true
  def handle_continue(:start_declared, state) do
    {:noreply, attempt(state)}
  end

  @impl true
  def handle_info(:retry, state) do
    {:noreply, attempt(state)}
  end

  defp attempt(%{pending: []} = state), do: state

  defp attempt(state) do
    outcomes =
      state.pending
      |> Enum.map(fn {module, opts} = resource -> {resource, start(module, opts)} end)
      |> settle_contested()

    {pending, last_error} =
      Enum.reduce(outcomes, {[], nil}, fn
        {_resource, :started}, acc -> acc
        {resource, {:pending, reason}}, {pending, _previous} -> {[resource | pending], reason}
      end)

    reschedule(%{state | pending: Enum.reverse(pending)}, last_error)
  end

  defp start(module, opts) do
    case module.start(opts) do
      {:ok, _pid} -> :started
      {:ok, _pid, _info} -> :started
      {:error, {:already_started, pid}} -> {:contested, pid}
      {:error, reason} -> {:pending, reason}
    end
  end

  # A name can still be registered to a process on its way out, and Process.alive?/1 stays true
  # until that exit is processed. Waiting for a DOWN tells a survivor from a straggler — once
  # for the whole set, so the wait does not grow with the number of declarations.
  defp settle_contested(outcomes) do
    case for {_resource, {:contested, pid}} <- outcomes, do: pid do
      [] -> outcomes
      pids -> settle_all(outcomes, await_exits(pids))
    end
  end

  defp settle_all(outcomes, gone), do: Enum.map(outcomes, &settle(&1, gone))

  defp settle({resource, {:contested, pid}}, gone) do
    if pid in gone,
      do: {resource, {:pending, {:already_started, pid}}},
      else: {resource, :started}
  end

  defp settle(settled, _gone), do: settled

  defp await_exits(pids) do
    refs = Map.new(pids, fn pid -> {Process.monitor(pid), pid} end)
    gone = collect_exits(refs, [], System.monotonic_time(:millisecond) + @settle_ms)
    Enum.each(Map.keys(refs), &Process.demonitor(&1, [:flush]))
    gone
  end

  defp collect_exits(refs, gone, deadline) do
    if length(gone) == map_size(refs) do
      gone
    else
      receive do
        {:DOWN, ref, :process, pid, _reason} when is_map_key(refs, ref) ->
          collect_exits(refs, [pid | gone], deadline)
      after
        max(deadline - System.monotonic_time(:millisecond), 0) -> gone
      end
    end
  end

  defp reschedule(%{pending: []} = state, _last_error) do
    if state.backoff > 0 do
      Logger.info("Pulsar client #{inspect(state.client)}: all #{state.declared} declared #{state.kind} are running")
    end

    %{state | backoff: 0}
  end

  defp reschedule(state, last_error) do
    wait = Backoff.next(state.backoff)
    running = state.declared - length(state.pending)

    Logger.error(
      "Pulsar client #{inspect(state.client)}: #{running} of #{state.declared} declared " <>
        "#{state.kind} running (#{inspect(last_error)}); retrying in #{wait}ms"
    )

    Process.send_after(self(), :retry, wait)

    %{state | backoff: wait}
  end
end
