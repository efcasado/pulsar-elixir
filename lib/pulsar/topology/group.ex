defmodule Pulsar.Topology.Group do
  @moduledoc false

  # Supervises the worker processes for one topic, or for one partition of it, for
  # consumers and for producers alike.
  #
  # Started beneath Pulsar.Topology, after Pulsar.Consumer or Pulsar.Producer has validated
  # the option surface, so options are threaded through untouched apart from the per-worker
  # :name. Each worker gets its own name because the broker identifies a producer by it and
  # the topic epoch is stored against it, so sharing one name across a group would have them
  # overwrite each other.

  use Supervisor

  alias Pulsar.Topology

  require Logger

  @spec start_link(module(), atom(), keyword()) :: Supervisor.on_start()
  def start_link(worker, count_key, opts), do: Supervisor.start_link(__MODULE__, {worker, count_key, opts})

  @impl true
  def init({worker, count_key, opts}) do
    name = Keyword.fetch!(opts, :name)
    count = Keyword.fetch!(opts, count_key)

    Logger.info(
      "Starting #{inspect(worker)} group #{name} for topic #{Keyword.fetch!(opts, :topic)} with #{count} workers"
    )

    children =
      for i <- 1..count do
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
