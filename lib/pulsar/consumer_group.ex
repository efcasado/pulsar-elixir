defmodule Pulsar.ConsumerGroup do
  @moduledoc false

  # Supervises the worker processes for one topic or one partition of it. Started by
  # Pulsar.Consumer, which owns the option surface and has already validated these
  # options, so they are threaded through untouched apart from the per-worker :name.

  use Supervisor

  require Logger

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    client = Keyword.fetch!(opts, :client)

    Supervisor.start_link(__MODULE__, opts, name: {:via, Registry, {Pulsar.Client.consumer_registry(client), name}})
  end

  def stop(supervisor_pid, reason \\ :normal, timeout \\ :infinity) do
    Supervisor.stop(supervisor_pid, reason, timeout)
  end

  @doc """
  Gets all consumer process PIDs managed by this consumer group.
  """
  def get_consumers(supervisor_pid) do
    supervisor_pid
    |> Supervisor.which_children()
    |> Enum.map(fn {_id, child_pid, :worker, _modules} -> child_pid end)
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    consumer_count = Keyword.fetch!(opts, :consumer_count)

    Logger.info(
      "Starting consumer group #{name} for topic #{Keyword.fetch!(opts, :topic)} with #{consumer_count} consumers"
    )

    children =
      for i <- 1..consumer_count do
        worker_name = "#{name}-consumer-#{i}"

        %{
          id: worker_name,
          start: {Pulsar.Consumer.Worker, :start_link, [Keyword.put(opts, :name, worker_name)]},
          restart: :transient,
          type: :worker
        }
      end

    Supervisor.init(children, strategy: :one_for_one, max_restarts: Keyword.fetch!(opts, :max_restarts))
  end
end
