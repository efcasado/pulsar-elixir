defmodule Pulsar.Topology.Group do
  @moduledoc false

  @behaviour Supervisor

  alias Pulsar.Client

  require Logger

  @spec start_link(module(), keyword()) :: Supervisor.on_start()
  def start_link(worker, opts) do
    Supervisor.start_link(__MODULE__, {worker, opts})
  end

  @impl true
  def init({worker, opts}) do
    name = Keyword.fetch!(opts, :name)
    client = Keyword.fetch!(opts, :client)
    {[_ | _] = connection_slots, worker_opts} = Keyword.pop!(opts, :connection_slots)
    count = length(connection_slots)

    Logger.debug(
      "Starting #{inspect(worker)} group #{name} for topic #{Keyword.fetch!(opts, :topic)} with #{count} workers"
    )

    children =
      connection_slots
      |> Enum.with_index(1)
      |> Enum.map(fn {connection_slot, i} ->
        # Workers need distinct names within a group; producer epochs are keyed by this identity.
        worker_name = "#{name}-#{i}"

        worker_opts =
          Keyword.merge(worker_opts, name: worker_name, connection_slot: connection_slot)

        %{
          id: worker_name,
          start: {worker, :start_link, [worker_opts]},
          restart: worker_restart(worker, worker_opts),
          type: :worker
        }
      end)

    # Consumer workers are transient, so a callback can finish one normally without spending a
    # restart. Groups and every boundary above them remain permanent, which lets abnormal worker
    # failures exhaust their way upward.
    Supervisor.init(children, [strategy: :one_for_one] ++ restart_intensity(client, count))
  end

  # Honour the worker module's lifecycle while retaining the explicit ids and names a group owns.
  # A module without child_spec/1 keeps the historical permanent default used by test and custom
  # workers.
  defp worker_restart(worker, opts) do
    if Code.ensure_loaded?(worker) and function_exported?(worker, :child_spec, 1) do
      {worker, opts} |> Supervisor.child_spec([]) |> Map.get(:restart, :permanent)
    else
      :permanent
    end
  end

  # A broker outage can exit every worker at once, so a group of `count` sees `count` restarts for
  # one failure. See docs/architecture.md for what scaling trades.
  @doc false
  @spec restart_intensity(atom(), pos_integer()) :: keyword()
  def restart_intensity(client, count) do
    client
    |> Client.restart_intensity(:worker)
    |> Keyword.update!(:max_restarts, &(&1 * count))
  end
end
