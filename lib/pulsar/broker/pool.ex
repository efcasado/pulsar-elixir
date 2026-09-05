defmodule Pulsar.Broker.Pool do
  @moduledoc false

  # One stable supervisor per broker URL. A worker is assigned a numbered slot before it starts
  # and retains it across restarts. Stateless metadata operations use any live child process and
  # let that broker report whether its socket is currently usable.

  use Supervisor

  @doc false
  def child_spec({broker_url, opts}) do
    %{
      id: {:broker_pool, broker_url},
      start: {__MODULE__, :start_link, [broker_url, opts]},
      restart: :permanent,
      type: :supervisor
    }
  end

  @spec start_link(String.t(), keyword()) :: Supervisor.on_start()
  def start_link(broker_url, opts) do
    {name, broker_opts} = Keyword.pop!(opts, :name)
    Supervisor.start_link(__MODULE__, {broker_url, broker_opts}, name: name)
  end

  @doc """
  Selects a live connection from `pool`.

  An integer selects that exact numbered slot.
  A slot that is restarting is unavailable rather than silently moving the caller to a sibling.
  """
  @spec checkout(Supervisor.supervisor(), non_neg_integer() | :random) ::
          {:ok, pid()} | {:error, :disconnected}
  def checkout(pool, connection_slot) when is_integer(connection_slot) and connection_slot >= 0 do
    case List.keyfind(connection_entries(pool), connection_slot, 0) do
      {^connection_slot, pid} -> {:ok, pid}
      _unavailable -> {:error, :disconnected}
    end
  end

  def checkout(pool, :random) do
    case connection_entries(pool) do
      [] ->
        {:error, :disconnected}

      entries ->
        {_slot, connection} = Enum.random(entries)
        {:ok, connection}
    end
  end

  @doc false
  @spec connections(Supervisor.supervisor()) :: [pid()]
  def connections(pool) do
    pool
    |> connection_entries()
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
  end

  defp connection_entries(pool) do
    for {{:connection, slot}, pid, :worker, _modules} <- children(pool), is_pid(pid), do: {slot, pid}
  end

  # Once a public lookup has selected a pool, any exit of that exact supervisor call means the
  # selected pool disappeared in the lookup/use race and is therefore disconnected. Resource
  # listings use a different policy because they have no retryable error result.
  defp children(pool) do
    Supervisor.which_children(pool)
  catch
    :exit, {_reason, {GenServer, :call, _call}} -> []
  end

  @impl true
  def init({broker_url, opts}) do
    {connections_per_broker, broker_opts} = Keyword.pop!(opts, :connections_per_broker)

    children =
      for slot <- 0..(connections_per_broker - 1) do
        %{
          id: {:connection, slot},
          start: {Pulsar.Broker, :start_link, [broker_url, Keyword.put(broker_opts, :connection_slot, slot)]},
          restart: :permanent
        }
      end

    # Socket failures reconnect inside Broker and never reach this supervisor. Scale OTP's default
    # restart budget by pool size so process exits retain the same per-connection tolerance.
    Supervisor.init(children, strategy: :one_for_one, max_restarts: 3 * connections_per_broker)
  end
end
