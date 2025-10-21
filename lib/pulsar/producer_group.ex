defmodule Pulsar.ProducerGroup do
  @moduledoc """
  A supervisor that manages a group of producer processes for a single topic.

  ## Examples

      # Start a producer group with default settings (1 producer)
      {:ok, group_pid} = ProducerGroup.start_link(
        "my-topic-producer",
        "persistent://public/default/my-topic"
      )

      # Start a producer group with 3 producers
      {:ok, group_pid} = ProducerGroup.start_link(
        "my-topic-producer",
        "persistent://public/default/my-topic",
        producer_count: 3
      )

      # Get all producer PIDs from the group
      producer_pids = ProducerGroup.get_producers(group_pid)
  """

  use Supervisor
  require Logger

  @producer_registry Pulsar.ProducerRegistry

  @doc """
  Starts a producer group supervisor.

  ## Parameters

  - `name` - Unique name for this producer group
  - `topic` - The topic to publish to
  - `opts` - Additional options:
    - `:producer_count` - Number of producer processes in this group (default: 1)
    - `:access_mode` - Producer access mode (default: :Shared)
    - Other options passed to individual producer processes

  ## Returns

  `{:ok, pid}` - The producer group supervisor PID
  `{:error, reason}` - Error if the supervisor failed to start
  """
  def start_link(name, topic, opts \\ []) do
    Supervisor.start_link(
      __MODULE__,
      {name, topic, opts},
      name: {:via, Registry, {@producer_registry, name}}
    )
  end

  def stop(supervisor_pid, reason \\ :normal, timeout \\ :infinity) do
    Logger.debug("Closing producer group.")
    Supervisor.stop(supervisor_pid, reason, timeout)
  end

  @doc """
  Gets all producer process PIDs managed by this producer group.

  Returns a list of producer PIDs.
  """
  def get_producers(supervisor_pid) do
    Supervisor.which_children(supervisor_pid)
    |> Enum.map(fn {_id, child_pid, :worker, _modules} -> child_pid end)
  end

  @doc """
  Sends a message through the producer in this group.
  """
  @spec send_message(pid(), binary(), timeout()) :: {:ok, map()} | {:error, term()}
  def send_message(group_pid, payload, timeout \\ 5000) do
    [producer_pid] = get_producers(group_pid)
    Pulsar.Producer.send_message(producer_pid, payload, timeout)
  end

  @impl true
  def init({name, topic, opts}) do
    producer_count = Keyword.get(opts, :producer_count, 1)

    Logger.info(
      "Starting producer group #{name} for topic #{topic} with #{producer_count} producers"
    )

    # Create child specs for each producer in the group
    children = create_producer_children(name, topic, opts, producer_count)

    supervisor_opts = [
      strategy: :one_for_one,
      max_restarts: Keyword.get(opts, :max_restarts, 10)
    ]

    Supervisor.init(children, supervisor_opts)
  end

  # Private functions

  defp create_producer_children(group_name, topic, opts, producer_count) do
    for i <- 1..producer_count do
      producer_id = "#{group_name}-producer-#{i}"

      %{
        id: producer_id,
        start: {
          Pulsar.Producer,
          :start_link,
          [topic, opts]
        },
        restart: :transient,
        type: :worker
      }
    end
  end
end
