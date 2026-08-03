defmodule Pulsar.Client.Bootstrap do
  @moduledoc false

  use GenServer

  alias Pulsar.Backoff
  alias Pulsar.Client

  require Logger

  def start_link({kind, opts}) do
    GenServer.start_link(__MODULE__, {kind, opts})
  end

  @impl true
  def init({kind, opts}) do
    client = Keyword.fetch!(opts, :name)
    pending = Enum.map(Keyword.fetch!(opts, kind), &{Client.resource_module(kind), &1})

    state = %{client: client, kind: kind, pending: pending, declared: length(pending), backoff: 0}

    {:ok, attempt(state)}
  end

  @impl true
  def handle_info(:retry, state) do
    {:noreply, attempt(state)}
  end

  defp attempt(%{pending: []} = state), do: state

  defp attempt(state) do
    supervisor = Client.resource_supervisor(state.kind, state.client)

    outcomes =
      Enum.map(state.pending, fn {module, opts} = resource ->
        {resource, start(module, opts, supervisor)}
      end)

    {pending, last_error} =
      Enum.reduce(outcomes, {[], nil}, fn
        {_resource, :started}, acc -> acc
        {resource, {:pending, reason}}, {pending, _previous} -> {[resource | pending], reason}
      end)

    reschedule(%{state | pending: Enum.reverse(pending)}, last_error)
  end

  defp start(module, opts, supervisor) do
    case module.start(opts) do
      {:ok, _pid} ->
        :started

      {:ok, _pid, _info} ->
        :started

      {:error, {:already_started, pid}} ->
        if owned_by?(pid, supervisor), do: :started, else: {:pending, {:already_started, pid}}

      {:error, reason} ->
        {:pending, reason}
    end
  end

  # Only a child of this resource supervisor satisfies the declaration. A registered pid
  # owned elsewhere is a name collision, so keep retrying until the name becomes available.
  defp owned_by?(pid, supervisor) do
    Enum.any?(DynamicSupervisor.which_children(supervisor), fn
      {_id, child, _type, _modules} -> child == pid
    end)
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
