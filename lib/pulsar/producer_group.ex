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

      # Start with batching enabled
      {:ok, group_pid} = ProducerGroup.start_link(
        "my-topic-producer",
        "persistent://public/default/my-topic",
        batch_enabled: true,
        batch_size: 100,
        flush_interval: 10
      )

      # Get all producer PIDs from the group
      producer_pids = ProducerGroup.get_producers(group_pid)
  """

  use Supervisor

  alias Pulsar.Producer.BatchSupervisor
  alias Pulsar.Producer.Collector

  require Logger

  @default_client :default
  @default_batch_size 100
  @default_flush_interval 10

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
    client = Keyword.get(opts, :client, @default_client)
    producer_registry = Pulsar.Client.producer_registry(client)

    Supervisor.start_link(
      __MODULE__,
      {name, topic, opts},
      name: {:via, Registry, {producer_registry, name}}
    )
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
    |> Enum.filter(fn {_id, _pid, type, _modules} -> type == :worker end)
    |> Enum.map(fn {_id, child_pid, :worker, _modules} -> child_pid end)
    |> Enum.filter(&is_pid/1)
  end

  @doc """
  Sends a message through the producer in this group.

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
    case get_collector(group_pid) do
      nil ->
        send_direct(group_pid, message, opts)

      collector_pid ->
        # Batching enabled - send to collector
        # Build message map for batch encoding
        msg = %{
          payload: message,
          partition_key: Keyword.get(opts, :partition_key),
          ordering_key: Keyword.get(opts, :ordering_key),
          properties: Keyword.get(opts, :properties),
          event_time: Keyword.get(opts, :event_time)
        }

        timeout = Keyword.get(opts, :timeout, 5000)

        try do
          Collector.add_sync(collector_pid, msg, timeout)
        catch
          :exit, reason ->
            {:error, {:collector_died, reason}}
        end
    end
  end

  defp send_direct(group_pid, message, opts) do
    case get_producers(group_pid) do
      [] ->
        {:error, :no_producers_available}

      [producer_pid | _] ->
        try do
          Pulsar.Producer.send_message(producer_pid, message, opts)
        catch
          :exit, reason ->
            {:error, {:producer_died, reason}}
        end
    end
  end

  @doc """
  Gets the collector pid if batching is enabled.
  """
  def get_collector(supervisor_pid) do
    case get_batch_supervisor(supervisor_pid) do
      nil -> nil
      batch_sup -> BatchSupervisor.get_collector(batch_sup)
    end
  end

  defp get_batch_supervisor(supervisor_pid) do
    supervisor_pid
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {:batch_supervisor, pid, :supervisor, _} when is_pid(pid) -> pid
      _ -> nil
    end)
  end

  @impl true
  def init({name, topic, opts}) do
    producer_count = Keyword.get(opts, :producer_count, 1)
    batch_enabled = Keyword.get(opts, :batch_enabled, false)

    Logger.info(
      "Starting producer group #{name} for topic #{topic} with #{producer_count} producers (access: #{Keyword.get(opts, :access_mode, :Shared)}, batching: #{batch_enabled})"
    )

    # Create child specs for producers
    producer_children = create_producer_children(name, topic, opts, producer_count)

    children =
      if batch_enabled do
        batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
        flush_interval = Keyword.get(opts, :flush_interval, @default_flush_interval)

        batch_child = create_batch_supervisor_child(name, batch_size, flush_interval, self())
        [batch_child | producer_children]
      else
        producer_children
      end

    supervisor_opts = [
      strategy: :one_for_one,
      # Allow many restarts to handle broker disconnection scenarios
      # where producers may fail multiple times while broker reconnects
      max_restarts: Keyword.get(opts, :max_restarts, 100),
      max_seconds: 60
    ]

    Supervisor.init(children, supervisor_opts)
  end

  # Private functions

  defp create_batch_supervisor_child(group_name, batch_size, flush_interval, group_pid) do
    collector_name = :"#{group_name}-collector"
    flusher_name = :"#{group_name}-flusher"

    %{
      id: :batch_supervisor,
      start: {
        BatchSupervisor,
        :start_link,
        [
          [
            collector_name: collector_name,
            flusher_name: flusher_name,
            batch_size: batch_size,
            flush_interval: flush_interval,
            group_pid: group_pid
          ]
        ]
      },
      restart: :permanent,
      type: :supervisor
    }
  end

  defp create_producer_children(group_name, topic, opts, producer_count) do
    for i <- 1..producer_count do
      producer_id = "#{group_name}-producer-#{i}"

      %{
        id: producer_id,
        start: {
          Pulsar.Producer,
          :start_link,
          [topic, [name: group_name] ++ opts]
        },
        restart: :transient,
        type: :worker
      }
    end
  end
end
