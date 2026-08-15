defmodule Pulsar.Topology.Group do
  @moduledoc false

  @behaviour Supervisor

  alias Pulsar.Topology

  require Logger

  @spec start_link(module(), pos_integer(), keyword()) :: Supervisor.on_start()
  def start_link(worker, count, opts), do: Supervisor.start_link(__MODULE__, {worker, count, opts})

  @impl true
  def init({worker, count, opts}) do
    name = Keyword.fetch!(opts, :name)

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
          restart: :transient,
          significant: true,
          type: :worker
        }
      end

    # A worker that stops cleanly hit something retrying cannot fix, so it is not brought back.
    # :all_significant then keeps the group running while any sibling still has the topic, and
    # shuts it down once the last one is gone rather than leaving a group with nothing in it.
    Supervisor.init(children, [strategy: :one_for_one, auto_shutdown: :all_significant] ++ Topology.restart_intensity())
  end
end
