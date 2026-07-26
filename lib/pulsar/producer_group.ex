defmodule Pulsar.ProducerGroup do
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

  @doc """
  Gets all producer process PIDs managed by this producer group.

  Returns a list of producer PIDs that are currently alive.
  Filters out producers that are restarting or undefined.
  """
  def get_producers(supervisor_pid) do
    supervisor_pid
    |> Supervisor.which_children()
    |> Enum.map(fn {_id, child_pid, :worker, _modules} -> child_pid end)
    |> Enum.filter(&is_pid/1)
  end

  @doc """
  Sends a message through a producer in this group.

  ## Parameters

  - `group_pid` - The producer group supervisor PID
  - `message` - Binary message payload
  - `opts` - Optional parameters:
    - `:timeout` - Timeout in milliseconds (default: 5000)
    - `:partition_key` - Partition routing key (string)
    - `:ordering_key` - Key for ordering in Key_Shared subscriptions (binary)
    - `:properties` - Custom message metadata as a map
    - `:event_time` - Application event timestamp (DateTime or milliseconds)
    - `:deliver_at_time` - Absolute delayed delivery time (DateTime or milliseconds)
    - `:deliver_after` - Relative delayed delivery in milliseconds from now

  Returns `{:error, :no_producers_available}` if all producers in the group are dead or restarting.
  Returns `{:error, :producer_died}` if the producer crashes during the send operation.
  """
  @spec send_message(pid(), binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def send_message(group_pid, message, opts \\ []) do
    case get_producers(group_pid) do
      [] ->
        {:error, :no_producers_available}

      [producer_pid | _] ->
        try do
          Worker.send_message(producer_pid, message, opts)
        catch
          :exit, reason ->
            {:error, {:producer_died, reason}}
        end
    end
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
