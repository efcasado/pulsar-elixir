defmodule Pulsar.Group do
  @moduledoc false

  # Supervises the worker processes for one topic, or for one partition of it, for
  # consumers and for producers alike.
  #
  # Started by Pulsar.Consumer and Pulsar.Producer, which own the option surface and have
  # already validated these options, so they are threaded through untouched apart from
  # the per-worker :name. Each worker gets its own name because the broker identifies a
  # producer by it and the topic epoch is stored against it, so sharing one name across a
  # group would have them overwrite each other.

  use Supervisor

  require Logger

  @spec start_link(module(), atom(), atom(), keyword()) :: Supervisor.on_start()
  def start_link(worker, registry, count_key, opts) do
    name = Keyword.fetch!(opts, :name)

    Supervisor.start_link(__MODULE__, {worker, count_key, opts}, name: {:via, Registry, {registry, name}})
  end

  def stop(supervisor_pid, reason \\ :normal, timeout \\ :infinity) do
    Supervisor.stop(supervisor_pid, reason, timeout)
  end

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
          type: :worker
        }
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
