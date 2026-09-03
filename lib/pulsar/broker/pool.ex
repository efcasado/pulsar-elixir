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
    start_opts = Keyword.take(opts, [:name])
    Supervisor.start_link(__MODULE__, {broker_url, opts}, start_opts)
  end

  @doc """
  Selects a live connection from `pool`.

  An integer selects that exact numbered slot.
  A slot that is restarting is unavailable rather than silently moving the caller to a sibling.
  """
  @spec checkout(Supervisor.supervisor(), non_neg_integer() | :random) ::
          {:ok, pid()} | {:error, :not_found | :disconnected}
  def checkout(pool, connection_slot) when is_integer(connection_slot) and connection_slot >= 0 do
    child_id = {:connection, connection_slot}

    case List.keyfind(children(pool), child_id, 0) do
      {^child_id, pid, :worker, _modules} when is_pid(pid) -> {:ok, pid}
      _unavailable -> {:error, :disconnected}
    end
  end

  def checkout(pool, :random) do
    case connections(pool) do
      [] -> {:error, :not_found}
      connections -> {:ok, Enum.random(connections)}
    end
  end

  @doc false
  @spec connections(Supervisor.supervisor()) :: [pid()]
  def connections(pool) do
    pool
    |> children()
    |> Enum.filter(fn
      {{:connection, _slot}, pid, :worker, _modules} when is_pid(pid) -> true
      _child -> false
    end)
    |> Enum.sort_by(fn {{:connection, slot}, _pid, :worker, _modules} -> slot end)
    |> Enum.map(fn {{:connection, _slot}, pid, :worker, _modules} -> pid end)
  end

  defp children(pool) do
    Supervisor.which_children(pool)
  catch
    :exit, {reason, {GenServer, :call, _call}} when reason in [:noproc, :normal, :shutdown] -> []
    :exit, {{:shutdown, _reason}, {GenServer, :call, _call}} -> []
  end

  @impl true
  def init({broker_url, opts}) do
    {connections_per_broker, opts} = Keyword.pop!(opts, :connections_per_broker)
    broker_opts = Keyword.delete(opts, :name)

    children =
      for slot <- 0..(connections_per_broker - 1) do
        %{
          id: {:connection, slot},
          start: {Pulsar.Broker, :start_link, [broker_url, broker_opts]},
          restart: :permanent
        }
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
