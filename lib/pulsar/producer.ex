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
  workers. `await_ready/2` waits for its initial topic topology when an operation must not
  observe asynchronous startup.

  ## Options

  #{Pulsar.Producer.Options.docs()}
  """

  alias Pulsar.Client
  alias Pulsar.Producer.Options
  alias Pulsar.Producer.Worker
  alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
  alias Pulsar.Topology

  @doc false
  def child_spec(opts), do: Topology.child_spec(__MODULE__, id(opts), opts)

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

    Topology.start_link(Worker, Client.registry(:producers, client), :producer_count, opts)
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

    Client.start_resource(Client.resource_supervisor(:producers, client), {__MODULE__, opts})
  end

  @doc """
  Same as `start/1`, with the topic given positionally.
  """
  @spec start(String.t(), keyword()) :: DynamicSupervisor.on_start_child()
  def start(topic, opts) when is_binary(topic), do: start(Keyword.put(opts, :topic, topic))

  @doc """
  Waits for a producer's initial topic topology to be ready.

  Takes the stable root returned by `start/1` or its registered name. A named producer is
  resolved repeatedly, so this can be called immediately after starting a client whose
  resources are declared asynchronously.

  Readiness means initial topic discovery and topology construction have completed — the
  point after which publishing no longer returns `{:error, :not_ready}`. It does not guarantee
  continued broker availability or prevent a worker from restarting immediately afterward.

  Options:

  - `:timeout` - maximum time to wait in milliseconds, or `:infinity`; defaults to 5 seconds
  - `:client` - client name or pid used to resolve a producer name; defaults to `:default`
  """
  @spec await_ready(pid() | String.t() | atom(), keyword()) ::
          :ok | {:error, :not_found | :timeout}
  def await_ready(producer, opts \\ []), do: Topology.await_ready(producer, :producers, opts)

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
    case Client.lookup(:producers, name, Keyword.get(opts, :client, :default)) do
      {:ok, pid} -> publish(pid, message, opts)
      {:error, :not_found} = error -> error
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
    client = Keyword.get(opts, :client, :default)
    Topology.remove(producer, Client.resource_supervisor(:producers, client))
  end

  def stop(name, opts) when is_binary(name) or is_atom(name) do
    case Client.lookup(:producers, name, Keyword.get(opts, :client, :default)) do
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

      kind when kind in [:group, :topology] ->
        route(Topology.groups(producer), message, opts)
    end
  catch
    # A stale root and a worker that dies mid-send have the same public result.
    :exit, reason -> {:error, {:producer_died, reason}}
  end

  defp route([], _message, _opts), do: {:error, :not_ready}

  defp route(groups, message, opts) do
    # Missing partitions are added highest-first, so the contiguous width stays at the old
    # modulus until every new slot exists. Restarting groups remain present and retain their
    # slot, avoiding a temporary key remap during either growth or recovery.
    case routing_width(groups) do
      0 ->
        {:error, :not_ready}

      partitions ->
        index = select_partition(opts, partitions)

        case List.keyfind(groups, index, 0) do
          {_index, group} when is_pid(group) -> send_to_worker(Topology.workers(group), message, opts)
          {_index, _restarting} -> {:error, :no_producers_available}
          nil -> {:error, {:partition_not_found, index}}
        end
    end
  end

  defp routing_width(groups) do
    groups
    |> Enum.map(&elem(&1, 0))
    |> Enum.sort()
    |> Enum.reduce_while(0, fn index, width ->
      if index == width, do: {:cont, width + 1}, else: {:halt, width}
    end)
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

  # Two producers in one static supervision tree need distinct ids, so the id follows
  # the same default as the producer's name.
  @doc false
  def id(opts), do: Keyword.get_lazy(opts, :name, fn -> default_name(Keyword.get(opts, :topic)) end)

  defp default_name(topic), do: "#{topic}-producer"
end
