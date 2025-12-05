defmodule Pulsar.PartitionedProducer do
  @moduledoc """
  A supervisor that manages individual producer groups for partitioned topics.

  This module provides a logical abstraction over multiple producer groups,
  allowing the `start_producer` API to return a single PID for partitioned topics
  while maintaining the individual producer group architecture underneath.

  The supervisor manages one producer group per partition, with the number of
  partitions provided by the caller.

  ## Message Routing

  Messages are routed to partitions based on the `:partition_key` option:
  - With `:partition_key` - Consistent hash routing via `:erlang.phash2(partition_key, num_partitions)`
  - Without `:partition_key` - Round-robin distribution across partitions
  """

  use Supervisor

  require Logger

  @default_client :default

  @doc """
  Starts a partitioned producer supervisor.

  ## Parameters

  - `name` - Unique name for this partitioned producer
  - `topic` - The base partitioned topic name (without partition suffix)
  - `partitions` - Number of partitions for this topic
  - `opts` - Additional options passed to individual producer groups

  ## Returns

  `{:ok, pid}` - The supervisor PID that manages all partition producer groups
  `{:error, reason}` - Error if the supervisor failed to start
  """
  def start_link(name, topic, partitions, opts \\ []) do
    client = Keyword.get(opts, :client, @default_client)
    producer_registry = Pulsar.Client.producer_registry(client)

    Supervisor.start_link(
      __MODULE__,
      {name, topic, partitions, opts},
      name: {:via, Registry, {producer_registry, name}}
    )
  end

  @doc """
  Stops a partitioned producer supervisor and all its child producer groups.
  """
  def stop(supervisor_pid, reason \\ :normal, timeout \\ :infinity) do
    Supervisor.stop(supervisor_pid, reason, timeout)
  end

  @doc """
  Gets information about all child producer groups managed by this supervisor.

  Returns a list of `{partition_topic, group_pid}` tuples.
  """
  def get_partition_groups(supervisor_pid) do
    supervisor_pid
    |> Supervisor.which_children()
    |> Enum.map(fn {partition_topic, group_pid, :supervisor, _modules} ->
      {partition_topic, group_pid}
    end)
  end

  @doc """
  Gets all producer processes from all partition groups managed by this supervisor.

  Returns a flat list of producer process PIDs from all partitions.
  """
  def get_producers(supervisor_pid) do
    supervisor_pid
    |> get_partition_groups()
    |> Enum.flat_map(fn {_partition_topic, group_pid} ->
      Pulsar.ProducerGroup.get_producers(group_pid)
    end)
  end

  @doc """
  Sends a message through the partitioned producer.

  Routes the message to a partition based on the `:partition_key` option:
  - With `:partition_key` - Uses consistent hashing to route to a specific partition
  - Without `:partition_key` - Uses round-robin distribution

  ## Parameters

  - `supervisor_pid` - The partitioned producer supervisor PID
  - `message` - Binary message payload
  - `opts` - Optional parameters (same as `Pulsar.ProducerGroup.send_message/3`)

  ## Returns

  `{:ok, message_id}` on success
  `{:error, reason}` on failure
  """
  @spec send_message(pid(), binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def send_message(supervisor_pid, message, opts \\ []) do
    partition_groups = get_partition_groups(supervisor_pid)
    num_partitions = length(partition_groups)

    if num_partitions == 0 do
      {:error, :no_producers_available}
    else
      partition_index = select_partition(supervisor_pid, opts, num_partitions)
      # Sort by topic name to ensure consistent ordering, then get by index
      {_topic, group_pid} = partition_groups |> Enum.sort() |> Enum.at(partition_index)

      Pulsar.ProducerGroup.send_message(group_pid, message, opts)
    end
  end

  @impl true
  def init({name, topic, partitions, opts}) do
    # Create ETS table owned by this supervisor for round-robin counter
    :ets.new(ets_table_name(self()), [:named_table, :public, :set])

    Logger.info("Starting partitioned producer for topic #{topic} with #{partitions} partitions")

    children =
      0
      |> Range.new(partitions - 1)
      |> Enum.map(fn partition_index ->
        partition_topic = "#{topic}-partition-#{partition_index}"
        partition_group_name = "#{name}-partition-#{partition_index}"

        %{
          id: partition_topic,
          start: {
            Pulsar.ProducerGroup,
            :start_link,
            [
              partition_group_name,
              partition_topic,
              opts
            ]
          },
          restart: :permanent,
          type: :supervisor
        }
      end)

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp select_partition(supervisor_pid, opts, num_partitions) do
    case Keyword.get(opts, :partition_key) do
      nil ->
        # Round-robin: increment and wrap at num_partitions
        ets_table = ets_table_name(supervisor_pid)
        :ets.update_counter(ets_table, :counter, {2, 1, num_partitions - 1, 0}, {:counter, -1})

      partition_key ->
        :erlang.phash2(partition_key, num_partitions)
    end
  end

  defp ets_table_name(supervisor_pid) do
    :"partitioned_producer_#{:erlang.pid_to_list(supervisor_pid)}"
  end
end
