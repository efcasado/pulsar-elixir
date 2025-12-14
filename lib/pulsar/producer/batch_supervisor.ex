defmodule Pulsar.Producer.BatchSupervisor do
  @moduledoc """
  Supervisor to manage batch-related processes (Collector and Flusher).
  """

  use Supervisor

  alias Pulsar.Producer.Collector
  alias Pulsar.Producer.Flusher

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, [])
  end

  @doc """
  Gets the collector pid from this supervisor.
  """
  def get_collector(supervisor_pid) do
    supervisor_pid
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {:collector, pid, :worker, _} when is_pid(pid) -> pid
      _ -> nil
    end)
  end

  @impl true
  def init(opts) do
    collector_name = Keyword.fetch!(opts, :collector_name)
    flusher_name = Keyword.fetch!(opts, :flusher_name)
    batch_size = Keyword.fetch!(opts, :batch_size)
    flush_interval = Keyword.fetch!(opts, :flush_interval)
    group_pid = Keyword.fetch!(opts, :group_pid)

    children = [
      %{
        id: :collector,
        start:
          {Collector, :start_link,
           [
             [
               name: collector_name,
               batch_size: batch_size,
               flusher_pid: flusher_name
             ]
           ]},
        restart: :permanent,
        type: :worker
      },
      %{
        id: :flusher,
        start:
          {Flusher, :start_link,
           [
             [
               name: flusher_name,
               flush_interval: flush_interval,
               collector_pid: collector_name,
               group_pid: group_pid
             ]
           ]},
        restart: :permanent,
        type: :worker
      }
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
