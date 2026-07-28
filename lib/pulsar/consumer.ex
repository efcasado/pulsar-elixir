defmodule Pulsar.Consumer do
  @moduledoc """
  A consumer subscribes to a topic and hands each message to a callback module.

  Declare one on the client it belongs to:

      children = [
        {Pulsar.Client,
         host: "pulsar://localhost:6650",
         consumers: [
           [topic: "persistent://public/default/orders",
            subscription_name: "order-service",
            callback_module: MyApp.OrderHandler]
         ]}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  See `Pulsar.Client` for how declared and runtime resources differ, and for `start/1`.

  A consumer is a supervisor over one worker per partition and per `:consumer_count`, so
  a partitioned topic needs nothing special at the call site. Partition count is resolved
  at startup, and new partitions are picked up by `:partition_discovery_interval_ms`.

  ## Callback module

  The callback module implements `Pulsar.Consumer.Callback`:

      defmodule MyApp.OrderHandler do
        use Pulsar.Consumer.Callback

        @impl true
        def handle_message(message, state) do
          IO.puts(message.payload)
          {:ok, state}
        end
      end

  Returning `{:ok, state}` acknowledges the message and `{:error, reason}` negatively
  acknowledges it. To acknowledge outside the callback's return value — when handing the
  message to another process, say — return `{:noreply, state}` and call `ack/2` or
  `nack/2` with the consumer's own pid.

  ## Options

  #{Pulsar.Consumer.Options.docs()}
  """

  alias Pulsar.Consumer.Options
  alias Pulsar.Consumer.Worker
  alias Pulsar.PartitionTopic
  alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
  alias Pulsar.ServiceDiscovery

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
  Starts a consumer, linked to the calling process.

  Returns the pid of the supervisor owning the consumer's workers. See the module
  documentation for the options.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    opts = Options.validate!(opts)
    topic = Keyword.fetch!(opts, :topic)
    client = Keyword.fetch!(opts, :client)

    opts =
      Keyword.put_new_lazy(opts, :name, fn ->
        default_name(topic, Keyword.fetch!(opts, :subscription_name))
      end)

    case partition_count(opts, topic, client) do
      {:ok, 0} ->
        Pulsar.Group.start_link(Worker, Pulsar.Client.consumer_registry(client), :consumer_count, opts)

      {:ok, partitions} ->
        Pulsar.Partitioned.start_link(
          Worker,
          Pulsar.Client.consumer_registry(client),
          :consumer_count,
          Keyword.put(opts, :partitions, partitions)
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Adds a consumer to a running client.

  For consumers whose set is only known at runtime. Prefer the client's `:consumers` for
  ones known up front: a consumer added here is not recreated if the client restarts.
  """
  @spec start(keyword()) :: DynamicSupervisor.on_start_child()
  def start(opts) when is_list(opts) do
    opts = Options.validate!(opts)
    client = Keyword.fetch!(opts, :client)
    topic = Keyword.fetch!(opts, :topic)

    # Resolved here rather than in start_link/1, which the supervisor runs in its own
    # process: the lookup and its retries would block every other consumer start.
    with {:ok, partitions} <- ServiceDiscovery.partition_count_with_retry(topic, client: client) do
      DynamicSupervisor.start_child(
        Pulsar.Client.consumer_supervisor(client),
        {__MODULE__, Keyword.put(opts, :partitions, partitions)}
      )
    end
  end

  @doc """
  Same as `start/1`, with the three required options given positionally.
  """
  @spec start(String.t(), String.t(), module(), keyword()) :: DynamicSupervisor.on_start_child()
  def start(topic, subscription_name, callback_module, opts \\ []) do
    start(
      Keyword.merge(opts,
        topic: topic,
        subscription_name: subscription_name,
        callback_module: callback_module
      )
    )
  end

  @doc """
  Stops a consumer, given its pid or its name.

  A consumer in a supervision tree will be restarted by its supervisor; stop those by
  removing them from the tree.
  """
  @spec stop(pid() | String.t() | atom(), keyword()) :: :ok | {:error, :not_found}
  def stop(consumer, opts \\ [])

  def stop(consumer, _opts) when is_pid(consumer) do
    case remove_from(owning_supervisor(consumer), consumer) do
      :ok -> :ok
      {:error, :not_found} -> stop_directly(consumer)
    end
  end

  def stop(name, opts) when is_binary(name) or is_atom(name) do
    with {:ok, pid} <- lookup(name, opts), do: stop(pid, opts)
  end

  @doc """
  Looks up a consumer by name, returning `{:ok, pid}` or `{:error, :not_found}`.
  """
  @spec lookup(String.t() | atom(), keyword()) :: {:ok, pid()} | {:error, :not_found}
  def lookup(name, opts \\ []) do
    client = Keyword.get(opts, :client, @default_client)

    case Registry.lookup(Pulsar.Client.consumer_registry(client), name) do
      [{pid, _value}] -> {:ok, pid}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Returns the worker processes behind a consumer, across every partition.
  """
  @spec workers(pid() | String.t() | atom(), keyword()) :: [pid()] | {:error, :not_found}
  def workers(consumer, opts \\ [])

  def workers(consumer, _opts) when is_pid(consumer) do
    collect_workers(consumer)
  end

  def workers(name, opts) when is_binary(name) or is_atom(name) do
    with {:ok, pid} <- lookup(name, opts), do: workers(pid)
  end

  @doc """
  Returns how many partitions a consumer covers, or `0` for a non-partitioned topic.
  """
  @spec partitions(pid() | String.t() | atom(), keyword()) :: non_neg_integer() | {:error, :not_found}
  def partitions(consumer, opts \\ [])

  def partitions(consumer, _opts) when is_pid(consumer) do
    consumer
    |> Supervisor.which_children()
    |> Enum.count(fn {_id, pid, type, _modules} -> type == :supervisor and is_pid(pid) end)
  end

  def partitions(name, opts) when is_binary(name) or is_atom(name) do
    with {:ok, pid} <- lookup(name, opts), do: partitions(pid)
  end

  @doc """
  Acknowledges one or more messages, marking them as processed.

  Takes the pid of the worker that delivered them, which inside a callback is `self()`.
  Not a group or a name: an acknowledgement carries the consumer id of the worker the
  broker sent the message to, so no other worker can answer for it.
  """
  @spec ack(pid(), MessageIdData.t() | [MessageIdData.t()]) :: :ok | {:error, term()}
  def ack(consumer, message_ids) when is_pid(consumer), do: Worker.ack(consumer, message_ids)

  @doc """
  Negatively acknowledges one or more messages, asking the broker to redeliver them.

  Takes the pid of the worker that delivered them, on the same terms as `ack/2`.

  Redelivered messages that exceed `:max_redelivery` go to the dead letter topic when
  `:dead_letter_policy` is configured, whether they were acknowledged manually or not.
  """
  @spec nack(pid(), MessageIdData.t() | [MessageIdData.t()]) :: :ok | {:error, term()}
  def nack(consumer, message_ids) when is_pid(consumer), do: Worker.nack(consumer, message_ids)

  @doc """
  Grants a consumer more flow permits.

  Only needed when `:flow_initial` is `0`, which turns off automatic flow control.
  """
  @spec send_flow(pid() | String.t() | atom(), non_neg_integer(), keyword()) :: :ok | {:error, term()}
  def send_flow(consumer, permits, opts \\ [])

  def send_flow(consumer, permits, _opts) when is_pid(consumer), do: Worker.send_flow(consumer, permits)

  def send_flow(name, permits, opts) when is_binary(name) or is_atom(name) do
    case workers(name, opts) do
      {:error, :not_found} -> {:error, :consumer_not_found}
      workers -> Enum.each(workers, &Worker.send_flow(&1, permits))
    end
  end

  @doc """
  Returns the topic a consumer is subscribed to.

  Takes any of the pids a consumer is made of: the one `start/1` returns, a partition's
  group, or a worker. A worker reports the partition it is subscribed to, the others the
  topic they cover.
  """
  @spec topic(pid()) :: String.t() | {:error, :not_found}
  def topic(consumer) do
    case kind(consumer) do
      :worker -> Worker.topic(consumer)
      :group -> worker_topic(consumer)
      :partitioned -> with topic when is_binary(topic) <- worker_topic(consumer), do: PartitionTopic.base(topic)
    end
  end

  defp worker_topic(supervisor) do
    case collect_workers(supervisor) do
      [worker | _rest] -> Worker.topic(worker)
      [] -> {:error, :not_found}
    end
  end

  # A supervisor cannot answer a GenServer call, so asking one for its topic would take
  # the group down with it. :proc_lib.initial_call/1 tells the levels apart without
  # walking the tree, which is what an earlier partition-routing bug turned on.
  defp kind(pid) do
    case :proc_lib.initial_call(pid) do
      {:supervisor, Pulsar.Partitioned, _args} -> :partitioned
      {:supervisor, Pulsar.Group, _args} -> :group
      _worker -> :worker
    end
  end

  # Descends through the partition supervisors, if any, so a partitioned and a plain
  # consumer read the same. Matching on the worker module skips the partition discovery
  # process, which is also a `:worker` child.
  defp collect_workers(supervisor) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.flat_map(fn
      {_id, pid, :worker, [Worker]} when is_pid(pid) -> [pid]
      {_id, pid, :supervisor, _modules} when is_pid(pid) -> collect_workers(pid)
      _child -> []
    end)
  end

  # Resolved by the caller for Pulsar.Consumer.start/4, so that the lookup and its retries
  # do not run inside the client's supervisor and block every other consumer start.
  # Asking the process which supervisor owns it, rather than deriving one from `:client`:
  # a pid carries no clue which client it belongs to, and stopping a permanent child any
  # other way just has its supervisor start it again.
  defp owning_supervisor(pid) do
    case Process.info(pid, :dictionary) do
      {:dictionary, dictionary} -> dictionary |> Keyword.get(:"$ancestors", []) |> List.first()
      nil -> nil
    end
  end

  # A consumer that is already gone reads as stopped, which is also what a caller holding a
  # pid replaced by a restart sees.
  defp stop_directly(pid) do
    Supervisor.stop(pid)
  catch
    :exit, _reason -> :ok
  end

  defp remove_from(nil, _pid), do: {:error, :not_found}

  defp remove_from(supervisor, pid) do
    DynamicSupervisor.terminate_child(supervisor, pid)
  catch
    :exit, _reason -> {:error, :not_found}
  end

  defp partition_count(opts, topic, client) do
    case Keyword.fetch(opts, :partitions) do
      {:ok, partitions} -> {:ok, partitions}
      :error -> ServiceDiscovery.partition_count_with_retry(topic, client: client)
    end
  end

  # Two consumers in one static supervision tree need distinct ids, so the id follows
  # the same default as the consumer's name.
  defp id(opts) do
    Keyword.get_lazy(opts, :name, fn ->
      default_name(Keyword.get(opts, :topic), Keyword.get(opts, :subscription_name))
    end)
  end

  defp default_name(topic, subscription_name), do: "#{topic}-#{subscription_name}"
end
