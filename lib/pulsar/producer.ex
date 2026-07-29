defmodule Pulsar.Producer do
  @moduledoc """
  A producer publishes messages to a topic.

  This module is how you add, publish through, inspect and stop producers. To declare them
  on a client instead, so they start and restart with it, see `Pulsar.Client`.

  `send/3` publishes, taking a producer's pid or the name it was registered under:

      {:ok, message_id} = Pulsar.Producer.send(:audit, "payload")

  A partitioned topic needs nothing special at the call site: messages are routed across
  partitions, honouring a message's `:partition_key` when one is set.

  `start/1` adds a producer to a running client and `stop/2` removes it. A producer is a
  supervisor over one worker per partition and per `:producer_count`, so `workers/2` and
  `partitions/2` report what it is made of, and `lookup/2` finds one by name.

  ## Options

  #{Pulsar.Producer.Options.docs()}
  """

  alias Pulsar.Producer.Options
  alias Pulsar.Producer.Worker
  alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
  alias Pulsar.ServiceDiscovery
  alias Pulsar.Topic
  alias Pulsar.Topology

  @default_client :default

  @doc false
  def child_spec(opts) do
    %{
      id: {__MODULE__, id(opts)},
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
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

    Topology.start_link(Worker, Pulsar.Client.producer_registry(client), :producer_count, opts)
  end

  @doc """
  Adds a producer to a running client.

  For producers whose set is only known at runtime. Prefer the client's `:producers` for
  ones known up front: a producer added here is not recreated if the client restarts.
  """
  @spec start(keyword() | String.t()) :: DynamicSupervisor.on_start_child()
  def start(topic) when is_binary(topic), do: start(topic: topic)

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

  - `:partition_key` - decides the partition of a partitioned topic, and is carried
    with the message so a `Key_Shared` subscription can use it
  - `:properties` - a map of user properties carried with the message
  - `:event_time` - the message's event time, in milliseconds
  - `:deliver_at_time` / `:deliver_after` - delayed delivery
  - `:client` - the client to resolve a producer name against

  ## Examples

      {:ok, message_id} = Pulsar.Producer.send(:audit, "payload")
      {:ok, message_id} = Pulsar.Producer.send(:audit, "payload", partition_key: "tenant-1")
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
  @spec stop(pid() | String.t() | atom(), keyword()) :: :ok | {:error, :not_found}
  def stop(producer, opts \\ [])

  def stop(producer, _opts) when is_pid(producer), do: Topology.remove(producer)

  def stop(name, opts) when is_binary(name) or is_atom(name) do
    with {:ok, pid} <- lookup(name, opts), do: stop(pid, opts)
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
  @spec workers(pid() | String.t() | atom(), keyword()) :: [pid()] | {:error, :not_found}
  def workers(producer, opts \\ [])

  def workers(producer, _opts) when is_pid(producer), do: Topology.workers(producer, Worker)

  def workers(name, opts) when is_binary(name) or is_atom(name) do
    with {:ok, pid} <- lookup(name, opts), do: workers(pid)
  end

  @doc """
  Returns how many partitions a producer covers, or `0` for a non-partitioned topic.
  """
  @spec partitions(pid() | String.t() | atom(), keyword()) :: non_neg_integer() | {:error, :not_found}
  def partitions(producer, opts \\ [])

  def partitions(producer, _opts) when is_pid(producer), do: Topology.partitions(producer)

  def partitions(name, opts) when is_binary(name) or is_atom(name) do
    with {:ok, pid} <- lookup(name, opts), do: partitions(pid)
  end

  # Resolving the partition here keeps topology knowledge in one module: the partition
  # supervisors below only build child specs.
  defp publish(producer, message, opts) do
    case partition_groups(producer) do
      [] -> send_to_worker(Topology.workers(producer, Worker), message, opts)
      groups -> route(groups, message, opts)
    end
  catch
    # The producer went away while we were looking at it, which is what a caller holding
    # a stale pid sees; it reads the same as a worker dying mid-send.
    :exit, reason -> {:error, {:producer_died, reason}}
  end

  defp route(groups, message, opts) do
    # The modulus is every configured partition, including any whose group is currently
    # restarting: hashing over only the live ones would move a key to another partition
    # for the duration of a restart, breaking per-key ordering.
    #
    # Routing then resolves the partition index parsed from the topic suffix rather than a
    # position in a sorted list: sorting names lexicographically misorders partitions once
    # there are ten or more ("...-partition-10" before "...-partition-2").
    index = select_partition(opts, length(groups))

    case Enum.find(groups, fn {topic, _pid} -> Topic.index(topic) == index end) do
      {_topic, group} when is_pid(group) -> send_to_worker(Topology.workers(group, Worker), message, opts)
      {_topic, _restarting} -> {:error, :no_producers_available}
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
  end

  defp partition_groups(producer) do
    producer
    |> Supervisor.which_children()
    |> Enum.filter(fn {_id, _pid, type, _modules} -> type == :supervisor end)
    |> Enum.map(fn {topic, pid, _type, _modules} -> {topic, pid} end)
  end

  # Two producers in one static supervision tree need distinct ids, so the id follows
  # the same default as the producer's name.
  @doc false
  def id(opts), do: Keyword.get_lazy(opts, :name, fn -> default_name(Keyword.get(opts, :topic)) end)

  defp default_name(topic), do: "#{topic}-producer"
end
