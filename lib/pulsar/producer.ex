defmodule Pulsar.Producer do
  @moduledoc """
  A producer publishes messages to a topic.

  This module is how you add, publish through and stop producers. To declare them
  on a client instead, so they start and restart with it, see `Pulsar.Client`.

  `send/3` publishes, taking a producer's pid or the name it was registered under:

      {:ok, message_id} = Pulsar.Producer.send(:audit, "payload")

  A partitioned topic needs nothing special at the call site: messages are routed across
  partitions, honouring a message's `:partition_key` when one is set.

  `start/1` adds a producer to a running client and `stop/2` removes it. Operations target
  the logical producer by its stable root or registered name without exposing its partition
  workers.

  ## Options

  #{Pulsar.Producer.Options.docs()}
  """

  alias Pulsar.Producer.Options
  alias Pulsar.Producer.Worker
  alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
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

  Returns the stable producer root. See the module documentation for the options.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    opts = Options.validate!(opts)
    topic = Keyword.fetch!(opts, :topic)
    client = Keyword.fetch!(opts, :client)
    opts = Keyword.put_new(opts, :name, default_name(topic))

    Topology.start_link(Worker, Pulsar.Client.registry(:producers, client), :producer_count, opts)
  end

  @doc """
  Adds a producer to a running client.

  For producers whose set is only known at runtime. Prefer the client's `:producers` for
  ones known up front: a producer added here is not recreated if the client restarts.

  Returns once the stable producer supervisor has been registered. Topic discovery and
  worker initialization continue asynchronously; publishing returns `{:error, :not_ready}`
  until discovery completes.
  """
  @spec start(keyword() | String.t()) :: DynamicSupervisor.on_start_child()
  def start(topic) when is_binary(topic), do: start(topic: topic)

  def start(opts) when is_list(opts) do
    opts = Options.validate!(opts)
    client = Keyword.fetch!(opts, :client)

    Pulsar.Client.start_resource(Pulsar.Client.resource_supervisor(:producers, client), {__MODULE__, opts})
  end

  @doc """
  Same as `start/1`, with the topic given positionally.
  """
  @spec start(String.t(), keyword()) :: DynamicSupervisor.on_start_child()
  def start(topic, opts) when is_binary(topic), do: start(Keyword.put(opts, :topic, topic))

  @doc """
  Publishes a message, given a producer's pid or name.

  Returns `{:error, :not_ready}` while its topic topology is being discovered.

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

  def send(producer, message, opts) when is_pid(producer) and is_binary(message), do: publish(producer, message, opts)

  def send(name, message, opts) when (is_binary(name) or is_atom(name)) and is_binary(message) do
    case resolve(name, opts) do
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

  def stop(producer, opts) when is_pid(producer) do
    client = Keyword.get(opts, :client, @default_client)
    Topology.remove(producer, Pulsar.Client.resource_supervisor(:producers, client))
  end

  def stop(name, opts) when is_binary(name) or is_atom(name) do
    case resolve(name, opts) do
      {:ok, producer} -> stop(producer, opts)
      {:error, :not_found} = error -> error
    end
  end

  # Resolving the partition here keeps topology knowledge in one module: the partition
  # supervisors below only build child specs.
  defp publish(producer, message, opts) do
    case Topology.kind(producer) do
      :worker ->
        Worker.send_message(producer, message, opts)

      :group ->
        route(Topology.groups(producer), message, opts)

      :topology ->
        if Topology.initialized?(producer),
          do: route(Topology.groups(producer), message, opts),
          else: {:error, :not_ready}
    end
  catch
    # The producer went away while we were looking at it, which is what a caller holding
    # a stale pid sees; it reads the same as a worker dying mid-send.
    :exit, reason -> {:error, {:producer_died, reason}}
  end

  defp route([], _message, _opts), do: {:error, :not_ready}

  defp route(groups, message, opts) do
    # The modulus is every configured partition, including any whose group is currently
    # restarting: hashing over only the live ones would move a key to another partition
    # for the duration of a restart, breaking per-key ordering.
    index = select_partition(opts, length(groups))

    case List.keyfind(groups, index, 0) do
      {_index, group} when is_pid(group) -> send_to_worker(Topology.workers(group), message, opts)
      {_index, _restarting} -> {:error, :no_producers_available}
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

  defp resolve(name, opts) do
    client = Keyword.get(opts, :client, @default_client)

    Pulsar.Client.lookup(Pulsar.Client.registry(:producers, client), name)
  end

  # Two producers in one static supervision tree need distinct ids, so the id follows
  # the same default as the producer's name.
  @doc false
  def id(opts), do: Keyword.get_lazy(opts, :name, fn -> default_name(Keyword.get(opts, :topic)) end)

  defp default_name(topic), do: "#{topic}-producer"
end
