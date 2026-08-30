defmodule Pulsar.Broker.Pool do
  @moduledoc false

  # One stable supervisor per broker URL. Workers check out a concrete connection and keep
  # using it for the lifetime of their broker registration, since producer and consumer ids
  # are scoped to that connection.

  use Supervisor

  @doc false
  def child_spec({broker_url, opts}) do
    %{
      id: {:broker_pool, broker_url},
      start: {__MODULE__, :start_link, [broker_url, opts]},
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

  The selection key keeps repeated lookups by one worker on the same connection while the
  pool membership is unchanged. Callers retain the returned pid; this is not a per-command
  checkout.
  """
  @spec checkout(Supervisor.supervisor(), term()) :: {:ok, pid()} | {:error, :not_found}
  def checkout(pool, key \\ self()) do
    case connections(pool) do
      [] -> {:error, :not_found}
      connections -> {:ok, Enum.at(connections, :erlang.phash2(key, length(connections)))}
    end
  end

  @doc false
  @spec connections(Supervisor.supervisor()) :: [pid()]
  def connections(pool) do
    pool
    |> Supervisor.which_children()
    |> Enum.filter(fn
      {{:connection, _slot}, pid, :worker, _modules} when is_pid(pid) -> true
      _child -> false
    end)
    |> Enum.sort_by(fn {{:connection, slot}, _pid, :worker, _modules} -> slot end)
    |> Enum.map(fn {{:connection, _slot}, pid, :worker, _modules} -> pid end)
  catch
    :exit, _reason -> []
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
