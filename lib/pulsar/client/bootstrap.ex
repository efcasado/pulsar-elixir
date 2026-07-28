defmodule Pulsar.Client.Bootstrap do
  @moduledoc false

  # The client's last static child: it connects the bootstrap broker into BrokerSupervisor,
  # alongside every broker discovered later, and then starts the consumers and producers
  # declared on the client. Being static, it runs again on every restart of the client, which
  # is how both come back where a DynamicSupervisor, having no static child list, cannot
  # bring them back itself.
  #
  # The connection is required and the declared resources are not; see `Pulsar.Client`'s
  # `:consumers` option for what that means for a caller.

  use GenServer

  alias Pulsar.Backoff
  alias Pulsar.Client

  require Logger

  @default_max_backoff 30_000
  @settle_ms 100

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    client = Keyword.fetch!(opts, :name)

    {:ok, _broker} = Client.start_broker(Keyword.fetch!(opts, :host), client: client)

    pending =
      Enum.map(Keyword.fetch!(opts, :producers), &{Pulsar.Producer, &1}) ++
        Enum.map(Keyword.fetch!(opts, :consumers), &{Pulsar.Consumer, &1})

    state = %{client: client, pending: pending, declared: length(pending), backoff: 0}

    # Starting the resources here rather than in init/1 keeps them off the client's boot:
    # resolving a topic's partitions takes seconds against a broker that is not up yet.
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

  # A name can still be registered to a process on its way out, and Process.alive?/1 stays
  # true until that exit is processed. Waiting briefly for a DOWN separates the two: a
  # resource that survived this restart is running, a dying one stays pending for the retry.
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
      Logger.info("Pulsar client #{inspect(state.client)}: all #{state.declared} declared resources are running")
    end

    %{state | backoff: 0}
  end

  defp reschedule(state, last_error) do
    wait = Backoff.next(state.backoff, max_backoff(state.client))
    running = state.declared - length(state.pending)

    Logger.error(
      "Pulsar client #{inspect(state.client)}: #{running} of #{state.declared} declared resources " <>
        "running (#{inspect(last_error)}); retrying in #{wait}ms"
    )

    Process.send_after(self(), :retry, wait)

    %{state | backoff: wait}
  end

  defp max_backoff(client) do
    client
    |> Client.get_broker_opts()
    |> Keyword.get(:max_backoff, @default_max_backoff)
  end
end
