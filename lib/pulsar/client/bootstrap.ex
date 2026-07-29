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

    # Idempotent, and each branch runs it, so the connection is re-established whichever
    # branch a restart brings back.
    {:ok, _broker} = Client.start_broker(Keyword.fetch!(opts, :host), client: client)

    pending = Enum.map(Keyword.fetch!(opts, kind), &{module_for(kind), &1})

    state = %{client: client, kind: kind, pending: pending, declared: length(pending), backoff: 0}

    # Off init/1 so resolving a topic against an unreachable broker cannot block the host's boot.
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
    {pending, last_error} =
      Enum.reduce(state.pending, {[], nil}, fn {module, opts}, {pending, last_error} ->
        case module.start(opts) do
          {:ok, _pid} ->
            {pending, last_error}

          {:ok, _pid, _info} ->
            {pending, last_error}

          {:error, {:already_started, pid}} ->
            if running?(pid),
              do: {pending, last_error},
              else: {[{module, opts} | pending], {:already_started, pid}}

          {:error, reason} ->
            {[{module, opts} | pending], reason}
        end
      end)

    reschedule(%{state | pending: Enum.reverse(pending)}, last_error)
  end

  # Process.alive?/1 stays true until a dying process's exit is processed, so it cannot tell a
  # resource that survived this restart from one on its way out. Waiting for a DOWN can.
  defp running?(pid) do
    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, :process, _pid, _reason} -> false
    after
      @settle_ms ->
        Process.demonitor(ref, [:flush])
        true
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
