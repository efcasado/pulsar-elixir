defmodule Pulsar.Producer do
  @moduledoc """
  A producer publishes messages to a topic.

  Start one from your own supervision tree:

      children = [
        {Pulsar.Client, host: "pulsar://localhost:6650"},
        {Pulsar.Producer, topic: "persistent://public/default/audit", name: :audit}
      ]

      Supervisor.start_link(children, strategy: :rest_for_one)

  `:rest_for_one` is deliberate. A producer resolves its brokers through the registries
  its client owns, so it has to be restarted when the client is.

  Then publish through the name it was registered under:

      {:ok, message_id} = Pulsar.Producer.send(:audit, "payload")

  A producer is a supervisor over one worker per partition and per `:producer_count`, so
  a partitioned topic needs nothing special at the call site. Messages are routed across
  partitions, honouring a message's partition key when one is set.

  Producers can also be started and stopped at runtime with `Pulsar.Producer.start/2`
  and `Pulsar.Producer.stop/2`, which put them under their client's supervisor instead of
  yours.

  ## Options

  #{Pulsar.Producer.Options.docs()}
  """

  alias Pulsar.Producer.Group
  alias Pulsar.Producer.Options
  alias Pulsar.Producer.Partitioned
  alias Pulsar.Producer.Worker
  alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
  alias Pulsar.ServiceDiscovery

  @default_client :default

  @doc false
  def child_spec(opts) do
    %{
      id: id(opts),
      start: {__MODULE__, :start_link, [opts]},
      restart: :transient,
      type: :supervisor
    }
  end

  @doc """
  Starts a producer, linked to the calling process.

  Returns the pid of the supervisor owning the producer's workers. See the module
  documentation for the options.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    opts = Options.validate!(opts)
    topic = Keyword.fetch!(opts, :topic)
    client = Keyword.fetch!(opts, :client)
    opts = Keyword.put_new(opts, :name, default_name(topic))

    case partition_count(opts, topic, client) do
      {:ok, 0} -> Group.start_link(opts)
      {:ok, partitions} -> Partitioned.start_link(Keyword.put(opts, :partitions, partitions))
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Starts a producer under its client's supervisor rather than the caller's.

  For producers created at runtime. Prefer `{Pulsar.Producer, opts}` in a supervision tree
  otherwise, so that the producer's lifetime is tied to the code depending on it.
  """
  @spec start(keyword()) :: DynamicSupervisor.on_start_child()
  def start(opts) when is_list(opts) do
    opts = Options.validate!(opts)
    client = Keyword.fetch!(opts, :client)
    topic = Keyword.fetch!(opts, :topic)

    # See Pulsar.Consumer.start/1: keeping the lookup out of the client's supervisor.
    with {:ok, partitions} <- ServiceDiscovery.partition_count_with_retry(topic, client: client) do
      DynamicSupervisor.start_child(
        Pulsar.Client.producer_supervisor(client),
        {__MODULE__, Keyword.put(opts, :partitions, partitions)}
      )
    end
  end

  @doc """
  Same as `start/1`, with the topic given positionally.
  """
  @spec start(String.t(), keyword()) :: DynamicSupervisor.on_start_child()
  def start(topic, opts) when is_binary(topic), do: start(Keyword.put(opts, :topic, topic))

  @doc """
  Publishes a message, given a producer's pid or name.

  ## Options

  - `:key` - partition key, which decides the partition of a partitioned topic
  - `:properties` - a map of user properties carried with the message
  - `:event_time` - the message's event time, in milliseconds
  - `:deliver_at_time` / `:deliver_after` - delayed delivery
  - `:client` - the client to resolve a producer name against

  ## Examples

      {:ok, message_id} = Pulsar.Producer.send(:audit, "payload")
      {:ok, message_id} = Pulsar.Producer.send(:audit, "payload", key: "tenant-1")
  """
  @spec send(pid() | String.t() | atom(), binary(), keyword()) ::
          {:ok, MessageIdData.t()} | {:error, term()}
  def send(producer, message, opts \\ [])

  def send(producer, message, opts) when is_pid(producer), do: publish(producer, message, opts)

  def send(name, message, opts) when is_binary(message) do
    case lookup(name, opts) do
      {:ok, pid} -> publish(pid, message, opts)
      {:error, :not_found} -> {:error, :producer_not_found}
    end
  end

  @doc """
  Stops a producer, given its pid or its name.

  A producer in a supervision tree will be restarted by its supervisor; stop those by
  removing them from the tree.
  """
  @spec stop(pid() | String.t(), keyword()) :: :ok | {:error, :not_found}
  def stop(producer, opts \\ [])

  def stop(producer, _opts) when is_pid(producer), do: Supervisor.stop(producer)

  def stop(name, opts) when is_binary(name) do
    with {:ok, pid} <- lookup(name, opts), do: stop(pid)
  end

  @doc """
  Looks up a producer by name, returning `{:ok, pid}` or `{:error, :not_found}`.
  """
  @spec lookup(String.t() | atom(), keyword()) :: {:ok, pid()} | {:error, :not_found}
  def lookup(name, opts \\ []) do
    client = Keyword.get(opts, :client, @default_client)

    case Registry.lookup(Pulsar.Client.producer_registry(client), name) do
      [{pid, _value}] -> {:ok, pid}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Returns the worker processes behind a producer, across every partition.
  """
  @spec workers(pid() | String.t(), keyword()) :: [pid()] | {:error, :not_found}
  def workers(producer, opts \\ [])

  def workers(producer, _opts) when is_pid(producer), do: collect_workers(producer)

  def workers(name, opts) when is_binary(name) do
    with {:ok, pid} <- lookup(name, opts), do: workers(pid)
  end

  @doc """
  Returns how many partitions a producer covers, or `0` for a non-partitioned topic.
  """
  @spec partitions(pid() | String.t(), keyword()) :: non_neg_integer() | {:error, :not_found}
  def partitions(producer, opts \\ [])

  def partitions(producer, _opts) when is_pid(producer) do
    producer
    |> Supervisor.which_children()
    |> Enum.count(fn {_id, pid, type, _modules} -> type == :supervisor and is_pid(pid) end)
  end

  def partitions(name, opts) when is_binary(name) do
    with {:ok, pid} <- lookup(name, opts), do: partitions(pid)
  end

  # Resolving the partition here keeps topology knowledge in one module: the partition
  # supervisors below only build child specs.
  defp publish(producer, message, opts) do
    case partition_groups(producer) do
      [] -> send_to_worker(collect_workers(producer), message, opts)
      groups -> route(groups, message, opts)
    end
  end

  defp route(groups, message, opts) do
    # Routing uses the partition index parsed from the topic suffix rather than a
    # position in a sorted list: sorting names lexicographically misorders partitions
    # once there are ten or more ("...-partition-10" before "...-partition-2").
    index = select_partition(opts, length(groups))

    case Enum.find(groups, fn {topic, _pid} -> Pulsar.PartitionTopic.index(topic) == index end) do
      {_topic, group} -> send_to_worker(collect_workers(group), message, opts)
      nil -> {:error, {:partition_not_found, index}}
    end
  end

  defp select_partition(opts, partitions) do
    case Keyword.get(opts, :partition_key) do
      nil -> Enum.random(0..(partitions - 1))
      partition_key -> :erlang.phash2(partition_key, partitions)
    end
  end

  defp send_to_worker([], _message, _opts), do: {:error, :no_producers_available}

  defp send_to_worker([worker | _rest], message, opts) do
    Worker.send_message(worker, message, opts)
  catch
    :exit, reason -> {:error, {:producer_died, reason}}
  end

  defp partition_groups(producer) do
    producer
    |> Supervisor.which_children()
    |> Enum.filter(fn {_id, pid, type, _modules} -> type == :supervisor and is_pid(pid) end)
    |> Enum.map(fn {topic, pid, _type, _modules} -> {topic, pid} end)
  end

  # Descends through the partition supervisors, if any. Matching on the worker module
  # skips the partition discovery process, which is also a `:worker` child.
  defp collect_workers(supervisor) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.flat_map(fn
      {_id, pid, :worker, [Worker]} when is_pid(pid) -> [pid]
      {_id, pid, :supervisor, _modules} when is_pid(pid) -> collect_workers(pid)
      _child -> []
    end)
  end

  # Resolved by the caller for Pulsar.Producer.start/2, so that the lookup and its retries
  # do not run inside the client's supervisor and block every other producer start.
  defp partition_count(opts, topic, client) do
    case Keyword.fetch(opts, :partitions) do
      {:ok, partitions} -> {:ok, partitions}
      :error -> ServiceDiscovery.partition_count_with_retry(topic, client: client)
    end
  end

  # Two producers in one static supervision tree need distinct ids, so the id follows
  # the same default as the producer's name.
  defp id(opts), do: Keyword.get_lazy(opts, :name, fn -> default_name(Keyword.get(opts, :topic)) end)

  defp default_name(topic), do: "#{topic}-producer"
end
