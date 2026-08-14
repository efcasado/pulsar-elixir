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
  workers. `await_ready/2` waits for its topology and configured workers when an operation
  must not observe asynchronous startup.

  ## Options

  #{Pulsar.Producer.Options.docs()}
  """

  alias Pulsar.Client
  alias Pulsar.Hash
  alias Pulsar.Producer.Options
  alias Pulsar.Producer.Worker
  alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
  alias Pulsar.Topology

  @typedoc """
  What a chunked send is answered with, since its message spans several broker messages.
  """
  @type chunked_message_id :: %{
          first_chunk_message_id: MessageIdData.t(),
          last_chunk_message_id: MessageIdData.t(),
          uuid: String.t(),
          num_chunks: pos_integer()
        }

  @typedoc """
  What `send/3` is answered with. `:deduplicated` carries no message id because the broker
  assigned none; see `send/3`.
  """
  @type send_result :: MessageIdData.t() | chunked_message_id() | :deduplicated

  @doc false
  def child_spec(opts), do: Topology.child_spec(__MODULE__, id(opts), opts)

  @doc """
  Starts a producer, linked to the calling process.

  Returns the stable producer root. See the module documentation for the options.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    opts = Options.validate!(opts)
    client = Keyword.fetch!(opts, :client)

    start_topology(opts, Client.registry(:producers, client))
  end

  @doc false
  # For a producer owned by another resource: nothing outside that owner should resolve or stop it,
  # and it must not depend on a registry belonging to a branch that restarts separately from it.
  @spec start_link_unregistered(keyword()) :: Supervisor.on_start()
  def start_link_unregistered(opts), do: opts |> Options.validate!() |> start_topology(nil)

  defp start_topology(opts, registry) do
    topic = Keyword.fetch!(opts, :topic)
    opts = Keyword.put_new_lazy(opts, :name, fn -> default_name(topic) end)

    Topology.start_link(Worker, registry, :producer_count, opts)
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
  Waits for a producer and all its configured workers to be ready.

  Takes the stable root returned by `start/1` or its registered name. A named producer is
  resolved repeatedly, so the wait also tolerates its client or resource branch restarting.

  Readiness means initial topic discovery and topology construction have completed, and every
  configured worker has registered with its broker. A worker that repeatedly fails registration
  causes the wait to time out. Readiness is a snapshot: it does not guarantee continued broker
  availability or prevent a worker from restarting immediately afterward.

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

  A message too large for the broker is refused here rather than sent, since the broker would
  answer it by closing a connection shared with every other producer and consumer:

  - `{:error, :message_too_large}` for a message, or a whole batch, that does not fit. With
    `:chunking_enabled` the payload is split to fit, so this only surfaces if the broker's
    limit is not yet known to the producer
  - `{:error, :metadata_too_large}` with `:chunking_enabled`, when `:properties` and the rest
    of the metadata leave no room for a payload to be split into

  A producer already carrying `:max_pending_messages` refuses with
  `{:error, :producer_queue_full}` rather than taking on more.

  `{:error, :send_timeout}` is the other shape: the broker did not acknowledge the message
  within `:send_timeout`. **It does not say the message was not published**, only that nothing
  came back in time. A retry publishes under a fresh sequence id, which the broker's
  deduplication does not match against the first attempt, so it can duplicate the message.

  A successful send answers with the broker's message id, or with a `t:chunked_message_id/0`
  when `:chunking_enabled` split the payload.

  On a topic with deduplication enabled it can instead answer `{:ok, :deduplicated}`: the broker
  recognised the sequence id as one it had already stored, kept the message it had, and assigned
  this call no message id. Deduplication matches on the sequence id alone and never on the
  payload, so the message it kept is not necessarily the one passed here. Before 3.0 this was
  reported as `{:ok, message_id}`, with a message id referring to nothing.

  ## Options

  - `:partition_key` - decides the partition of a partitioned topic, hashed under the
    producer's `:hashing_scheme`, and is carried with the message so a `:key_shared`
    subscription can use it. Must be a binary; before 3.0 any term was accepted here
  - `:properties` - a map of user properties carried with the message
  - `:event_time` - the message's event time, in milliseconds
  - `:deliver_at_time` / `:deliver_after` - delayed delivery. The broker delays whole entries, so
    a delayed message is published on its own rather than joining a batch
  - `:timeout` - how long to wait for the call itself, in milliseconds. Defaults to `:infinity`,
    leaving `:send_timeout` as the deadline. `:send_timeout` is counted from when the producer
    takes the message, so a producer that has not finished registering has not started it: this
    is what bounds that wait
  - `:client` - the client to resolve a producer name against

  ## Examples

      {:ok, message_id} = Pulsar.Producer.send(:audit, "payload")
      {:ok, message_id} = Pulsar.Producer.send(:audit, "payload", partition_key: "tenant-1")
  """
  @spec send(pid() | String.t() | atom(), binary(), keyword()) ::
          {:ok, send_result()} | {:error, term()}
  def send(producer, message, opts \\ [])

  def send(producer, message, opts) when is_pid(producer) and is_binary(message), do: publish(producer, message, opts)

  def send(name, message, opts) when (is_binary(name) or is_atom(name)) and is_binary(message) do
    case Client.lookup(:producers, name, Keyword.get(opts, :client, :default)) do
      {:ok, pid} -> publish(pid, message, opts)
      {:error, :not_found} = error -> error
    end
  end

  @doc """
  Publishes without waiting for the broker, answering with a reference to await on.

  Takes the same options as `send/3`, other than `:timeout`, which belongs to the wait rather
  than the send. The message is published in the order it was handed over, so two sends from one
  process to one partition keep theirs.

      {:ok, first} = Pulsar.Producer.send_async(:audit, "one")
      {:ok, second} = Pulsar.Producer.send_async(:audit, "two")

      {:ok, _message_id} = Pulsar.Producer.await(first)
      {:ok, _message_id} = Pulsar.Producer.await(second)

  Errors that `send/3` returns are delivered to `await/2` instead, so nothing is lost by not
  waiting — but a reference that is never awaited leaves its answer unread in the mailbox.

  Whether a producer will take the message at all is answered here: a full one refuses with
  `{:error, :producer_queue_full}` rather than handing back a reference.
  """
  @spec send_async(pid() | String.t() | atom(), binary(), keyword()) ::
          {:ok, reference()} | {:error, term()}
  def send_async(producer, message, opts \\ [])

  def send_async(producer, message, opts) when is_pid(producer) and is_binary(message) do
    publish_async(producer, message, opts)
  end

  def send_async(name, message, opts) when (is_binary(name) or is_atom(name)) and is_binary(message) do
    case Client.lookup(:producers, name, Keyword.get(opts, :client, :default)) do
      {:ok, pid} -> publish_async(pid, message, opts)
      {:error, :not_found} = error -> error
    end
  end

  @doc """
  Waits for a send started by `send_async/3`, answering as `send/3` would have.

  A producer that goes down before answering is reported as `{:error, {:producer_died, reason}}`,
  the same shape `send/3` reports it under.

  `:send_timeout` already bounds how long a producer holds a send, so the default here is to wait
  for it. A timeout given instead abandons the wait without cancelling the send: the message may
  still be published, and its answer stays in the mailbox unread.
  """
  @spec await(reference(), timeout()) :: {:ok, send_result()} | {:error, term()}
  def await(ref, timeout \\ :infinity) when is_reference(ref) do
    receive do
      {^ref, reply} ->
        Process.demonitor(ref, [:flush])
        reply

      {:DOWN, ^ref, :process, _pid, reason} ->
        {:error, {:producer_died, reason}}
    after
      timeout ->
        Process.demonitor(ref, [:flush])
        {:error, :timeout}
    end
  end

  @doc """
  Stops a producer, given its pid or its name.

  A pid must be the stable root returned by `start/1` or `start_link/1`. Group and worker pids
  are not producer roots and return `{:error, :not_found}` here.

  A root started as a static child will be restarted by its supervisor; remove that child
  from the supervision tree instead.
  """
  @spec stop(pid() | String.t() | atom(), keyword()) :: :ok | {:error, :not_found}
  def stop(producer, opts \\ [])

  def stop(producer, opts) when is_pid(producer) do
    if Topology.resource?(producer, :producers) do
      client = Keyword.get(opts, :client, :default)
      Topology.remove(producer, Client.resource_supervisor(:producers, client))
    else
      {:error, :not_found}
    end
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
    case resolve_worker(producer, opts) do
      {:ok, worker} -> Worker.send_message(worker, message, opts)
      {:error, _reason} = error -> error
    end
  catch
    # A stale root and a worker that dies mid-send have the same public result.
    :exit, reason -> {:error, {:producer_died, reason}}
  end

  # The monitor's reference doubles as the tag the reply carries, the way `GenServer.call/3`
  # does it, so `await/2` selects on either the reply or the producer going down.
  defp publish_async(producer, message, opts) do
    case resolve_worker(producer, opts) do
      {:ok, worker} ->
        ref = Process.monitor(worker)
        GenServer.cast(worker, {:send_message, message, opts, {self(), ref}})

        {:ok, ref}

      {:error, _reason} = error ->
        error
    end
  catch
    :exit, reason -> {:error, {:producer_died, reason}}
  end

  defp resolve_worker(producer, opts) do
    case Topology.kind(producer) do
      :worker ->
        {:ok, producer}

      :group ->
        {:error, :not_found}

      :topology ->
        {groups, hashing_scheme} = Topology.routing(producer)
        route(groups, hashing_scheme, opts)
    end
  end

  defp route([], _hashing_scheme, _opts), do: {:error, :not_ready}

  defp route(groups, hashing_scheme, opts) do
    # Missing partitions are added highest-first, so the contiguous width stays at the old
    # modulus until every new slot exists. Restarting groups remain present and retain their
    # slot, avoiding a temporary key remap during either growth or recovery.
    case routing_width(groups) do
      0 ->
        {:error, :not_ready}

      partitions ->
        index = select_partition(opts, hashing_scheme, partitions)

        case List.keyfind(groups, index, 0) do
          {_index, group} when is_pid(group) -> pick_worker(Topology.workers(group))
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

  defp select_partition(opts, hashing_scheme, partitions) do
    case Keyword.get(opts, :partition_key) do
      nil -> Enum.random(0..(partitions - 1))
      partition_key -> Hash.partition(hashing_scheme, partition_key, partitions)
    end
  end

  defp pick_worker([]), do: {:error, :no_producers_available}
  defp pick_worker([worker | _rest]), do: {:ok, worker}

  # Two producers in one static supervision tree need distinct ids, so the id follows
  # the same default as the producer's name.
  @doc false
  def id(opts), do: Keyword.get_lazy(opts, :name, fn -> default_name(Keyword.get(opts, :topic)) end)

  defp default_name(topic), do: "#{topic}-producer"
end
