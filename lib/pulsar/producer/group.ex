defmodule Pulsar.Producer.Group do
  @moduledoc false

  # Supervises the producer processes for one topic. Started by Pulsar.Producer.start/2,
  # which owns the option surface.

  use Supervisor

  alias Pulsar.Producer.Worker

  require Logger

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    client = Keyword.fetch!(opts, :client)

    Supervisor.start_link(__MODULE__, opts, name: {:via, Registry, {Pulsar.Client.producer_registry(client), name}})
  end

  def stop(supervisor_pid, reason \\ :normal, timeout \\ :infinity) do
    Logger.debug("Closing producer group.")
    Supervisor.stop(supervisor_pid, reason, timeout)
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    producer_count = Keyword.fetch!(opts, :producer_count)

    Logger.info(
      "Starting producer group #{name} for topic #{Keyword.fetch!(opts, :topic)} with #{producer_count} producers " <>
        "(access: #{Keyword.fetch!(opts, :access_mode)})"
    )

    children =
      for i <- 1..producer_count do
        %{
          id: "#{name}-producer-#{i}",
          start: {Worker, :start_link, [opts]},
          restart: :transient,
          type: :worker
        }
      end

    # Many restarts on purpose: producers can fail repeatedly while a broker reconnects.
    Supervisor.init(children,
      strategy: :one_for_one,
      max_restarts: Keyword.fetch!(opts, :max_restarts),
      max_seconds: 60
    )
  end
end
