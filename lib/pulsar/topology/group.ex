defmodule Pulsar.Topology.Group do
  @moduledoc false

  @behaviour Supervisor

  alias Pulsar.Client

  require Logger

  @spec start_link(module(), pos_integer(), keyword()) :: Supervisor.on_start()
  def start_link(worker, count, opts) when count > 0 do
    Supervisor.start_link(__MODULE__, {worker, count, opts})
  end

  @impl true
  def init({worker, count, opts}) do
    name = Keyword.fetch!(opts, :name)
    client = Keyword.fetch!(opts, :client)

    Logger.debug(
      "Starting #{inspect(worker)} group #{name} for topic #{Keyword.fetch!(opts, :topic)} with #{count} workers"
    )

    children =
      for i <- 1..count do
        # Workers need distinct names within a group; producer epochs are keyed by this identity.
        worker_name = "#{name}-#{i}"

        %{
          id: worker_name,
          start: {worker, :start_link, [Keyword.put(opts, :name, worker_name)]},
          restart: :permanent,
          type: :worker
        }
      end

    # A worker that is finished is stopped rather than exiting, so every exit reaching here is a
    # failure.
    Supervisor.init(children, [strategy: :one_for_one] ++ restart_intensity(client, count))
  end

  # A broker dropping its connection exits every worker registered with it at once, so a group of
  # `count` sees `count` restarts for one failure. Scaling keeps the budget meaning that many
  # rounds of correlated failure, rather than dividing it by :consumer_count.
  @doc false
  @spec restart_intensity(atom() | pid(), pos_integer()) :: keyword()
  def restart_intensity(client, count) do
    client
    |> Client.restart_intensity(:worker)
    |> Keyword.update!(:max_restarts, &(&1 * count))
  end
end
