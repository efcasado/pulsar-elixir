defmodule Pulsar.Client.Bootstrap do
  @moduledoc false

  use GenServer

  alias Pulsar.Backoff
  alias Pulsar.Client

  require Logger

  def start_link({kind, opts}) do
    GenServer.start_link(__MODULE__, {kind, opts})
  end

  defp module_for(:consumers), do: Pulsar.Consumer
  defp module_for(:producers), do: Pulsar.Producer

  defp supervisor_for(:consumers, client), do: Client.consumer_supervisor(client)
  defp supervisor_for(:producers, client), do: Client.producer_supervisor(client)

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
      |> settle_contested(supervisor_for(state.kind, state.client))

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

  defp settle_contested(outcomes, supervisor) do
    case for {_resource, {:contested, pid}} <- outcomes, do: pid do
      [] ->
        outcomes

      _pids ->
        # A current child is already supervised; any other registered pid belongs to a
        # predecessor still exiting and must stay pending until it releases the name.
        owned =
          for {_id, pid, _type, _modules} <- DynamicSupervisor.which_children(supervisor),
              is_pid(pid),
              into: MapSet.new(),
              do: pid

        Enum.map(outcomes, &settle_outcome(&1, owned))
    end
  end

  defp settle_outcome({resource, {:contested, pid}}, owned) do
    if pid in owned,
      do: {resource, :started},
      else: {resource, {:pending, {:already_started, pid}}}
  end

  defp settle_outcome(settled, _owned), do: settled

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
