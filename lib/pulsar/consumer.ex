defmodule Pulsar.Consumer do
  @moduledoc """
  A consumer subscribes to a topic and hands each message to a callback module.

  This module is how you add, inspect and stop consumers. To declare them on a client
  instead, so they start and restart with it, see `Pulsar.Client`. The callback module
  they dispatch into is `Pulsar.Consumer.Callback`.

  `start/1` adds a consumer to a running client and `stop/2` removes it. Operations target
  the logical consumer by its stable root or registered name without exposing its partition
  workers. `await_ready/2` waits for its topology and configured workers when an operation
  must not observe asynchronous startup.

  `ack/2` and `nack/2` acknowledge manually, from a process other than the worker that
  delivered the message. `send_flow/3` grants permits to a worker or every worker behind
  the consumer root.

  ## Flow Control

  A consumer grants `:flow_initial` permits when it subscribes, and `:flow_policy` refills
  them from there. `:auto` grants `:flow_refill` whenever outstanding permits reach
  `:flow_threshold`. Name a `{module, function, args}` to decide for yourself:

      defmodule MyApp.Flow do
        def decide(%{outstanding: outstanding}, refill) when outstanding <= 20, do: {:grant, refill}
        def decide(_flow, _refill), do: :ok
      end

      Pulsar.Consumer.start(topic, subscription, MyApp.Callback,
        flow_policy: {MyApp.Flow, :decide, [100]}
      )

  It is asked after every delivery with `%{consumed: permits, outstanding: permits}`, and
  answers `:ok` or `{:grant, permits}`. `:consumed` is what the delivery cost and
  `:outstanding` what is left after it, before any grant the policy makes.

  `:consumed` counts every message the broker charged for, not the ones a callback saw. The
  broker also charges for batch members excluded by an `ack_set` on a partially acknowledged
  entry, members compacted away, and deliveries diverted to a dead letter topic — none of
  which reach `c:Pulsar.Consumer.Callback.handle_message/2` or
  `c:Pulsar.Consumer.Callback.handle_invalid_message/2`. A policy counting callbacks instead
  eventually believes the broker holds permits it has already spent.

  Two things follow from a policy only being asked after a delivery. It cannot grant the
  first permits, since without them nothing is delivered: those come from `:flow_initial`, or
  from `send_flow/3` in another process. And it runs in the consumer process, so it must not
  call `send_flow/3` on that consumer — that is a call to itself and deadlocks. Handing the
  decision to another process, which then calls `send_flow/3`, is fine as long as that process
  is not waiting on this one.

  A policy that always answers `:ok` grants nothing, leaving every refill to `send_flow/3`.
  Granting does not ask the policy again, and neither does `send_flow/3`.

  ## Options

  #{Pulsar.Consumer.Options.docs()}
  """

  alias Pulsar.Client
  alias Pulsar.Consumer.DeadLetter
  alias Pulsar.Consumer.Options
  alias Pulsar.Consumer.Worker
  alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
  alias Pulsar.Topology

  @doc false
  def child_spec(opts), do: Topology.child_spec(__MODULE__, id(opts), opts)

  @doc """
  Starts a consumer, linked to the calling process.

  Returns the stable consumer root. See the module documentation for the options.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    opts = Options.validate!(opts)
    topic = Keyword.fetch!(opts, :topic)
    client = Keyword.fetch!(opts, :client)

    opts =
      opts
      |> Keyword.put_new_lazy(:name, fn ->
        default_name(topic, Keyword.fetch!(opts, :subscription_name))
      end)
      |> Keyword.put(:companions, &DeadLetter.attach/2)

    Topology.start_link(Worker, Client.registry(:consumers, client), :consumers, opts)
  end

  @doc """
  Adds a consumer to a running client.

  For consumers whose set is only known at runtime. Prefer the client's `:consumers` for
  ones known up front: a consumer added here is not recreated if the client restarts.

  Returns once the stable consumer supervisor has been registered. Topic discovery and
  worker initialization continue asynchronously; worker-dependent operations return
  `{:error, :not_ready}` until discovery completes.
  """
  @spec start(keyword()) :: DynamicSupervisor.on_start_child()
  def start(opts) when is_list(opts) do
    opts = Options.validate!(opts)
    client = Keyword.fetch!(opts, :client)

    Client.start_resource(Client.resource_supervisor(:consumers, client), {__MODULE__, opts})
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
  Waits for a consumer and all its configured workers to be ready.

  Takes the stable root returned by `start/1` or its registered name. A named consumer is
  resolved repeatedly, so the wait also tolerates its client or resource branch restarting.

  Readiness means initial topic discovery and topology construction have completed, and every
  configured worker has subscribed and initialized its callback. A worker that repeatedly
  fails initialization causes the wait to time out. Readiness is a snapshot: it does not
  guarantee continued broker availability or prevent a worker from restarting immediately
  afterward.

  Options:

  - `:timeout` - maximum time to wait in milliseconds, or `:infinity`; defaults to 5 seconds
  - `:client` - client name or pid used to resolve a consumer name; defaults to `:default`
  """
  @spec await_ready(pid() | String.t() | atom(), keyword()) ::
          :ok | {:error, :not_found | :timeout}
  def await_ready(consumer, opts \\ []), do: Topology.await_ready(consumer, :consumers, opts)

  @doc """
  Stops a consumer, given its pid or its name.

  A pid must be the stable root returned by `start/1` or `start_link/1`. Worker pids used for
  acknowledgement are not consumer roots and return `{:error, :not_found}` here.

  A root started as a static child is `:transient`, so stopping it leaves it stopped, but its
  child spec stays in the supervision tree until that supervisor restarts. A consumer declared
  on a client is not a static child: stopping it removes it until the client restarts.
  """
  @spec stop(pid() | String.t() | atom(), keyword()) :: :ok | {:error, :not_found}
  def stop(consumer, opts \\ [])

  def stop(consumer, opts) when is_pid(consumer) do
    if Topology.resource?(consumer, :consumers) do
      client = Keyword.get(opts, :client, :default)
      Topology.remove(consumer, Client.resource_supervisor(:consumers, client))
    else
      {:error, :not_found}
    end
  end

  def stop(name, opts) when is_binary(name) or is_atom(name) do
    case Client.lookup(:consumers, name, Keyword.get(opts, :client, :default)) do
      {:ok, consumer} -> stop(consumer, opts)
      {:error, :not_found} = error -> error
    end
  end

  @doc """
  Acknowledges one or more messages, marking them as processed.

  Takes the pid of the worker that delivered them. Not a group or a name: an acknowledgement
  carries the consumer id of the worker the broker sent the message to, so no other worker
  can answer for it.

  Manual acknowledgement is for handing a message to whatever actually processes it and
  acknowledging once that work is done. Return `{:noreply, state}` from
  `c:Pulsar.Consumer.Callback.handle_message/2` to leave the message unacknowledged, passing
  along the worker and the message id:

      def handle_message(message, state) do
        MyApp.Jobs.enqueue(message.payload, ack: {self(), message.message_id})
        {:noreply, state}
      end

  The job calls `Pulsar.Consumer.ack(consumer, message_id)` when it finishes. It has to be
  that process and not the callback: every callback function runs inside its worker, so
  `ack(self(), ...)` is a `GenServer` call a process makes to itself, which exits with
  `:calling_self` and takes the consumer down.

  ## Batched messages

  The broker acknowledges entries, not the messages inside them, so acking a batched message
  only counts it off: its entry is acknowledged once every message in it has been acked. The
  call is unchanged, but a message left unacked holds the ones batched with it, and a nack
  brings the whole entry back — including messages already acked from it.

  `:batch_index_ack_enabled` narrows that to just the unacked messages, on brokers configured
  for it.

  Every message must be acked eventually, or nacked with a `:redelivery_interval` configured to
  bring it back. One that is neither holds its entry's bookkeeping for the life of the consumer.
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

  Needed when the consumer's `:flow_policy` leaves refills to you, and to grant the first
  permits when `:flow_initial` is `0`, which no policy can do itself.

  Takes the stable consumer root, one of its worker pids, or its name. Every live worker behind
  a root is granted the permits, and the first refusal is returned — retrying by name is safe,
  since a worker that already holds permits is only over-credited, and a worker that refused
  has usually been replaced by one with a different pid. If a configured group has no live
  worker, the other groups are granted permits before `{:error, :no_consumers_available}` is
  returned.

  A consumer with no workers is an error rather than a silent success: nothing was granted,
  so nothing will be delivered. Permits belong to individual worker instances, and each grants
  `:flow_initial` for itself on subscribe, so a replacement needs another grant only for
  whatever it had been given on top of that.
  """
  @spec send_flow(pid() | String.t() | atom(), pos_integer(), keyword()) :: :ok | {:error, term()}
  def send_flow(consumer, permits, opts \\ [])

  def send_flow(consumer, permits, _opts) when is_pid(consumer) and is_integer(permits) and permits > 0 do
    case Topology.kind(consumer) do
      :worker ->
        grant(consumer, permits)

      :group ->
        {:error, :not_found}

      :topology ->
        case topology_workers(consumer) do
          :initializing -> {:error, :not_ready}
          {:ready, workers, unavailable?} -> grant_all(workers, permits, unavailable?)
        end
    end
  catch
    :exit, reason -> {:error, reason}
  end

  def send_flow(name, permits, opts) when (is_binary(name) or is_atom(name)) and is_integer(permits) and permits > 0 do
    case Client.lookup(:consumers, name, Keyword.get(opts, :client, :default)) do
      {:ok, consumer} -> send_flow(consumer, permits, opts)
      {:error, :not_found} = error -> error
    end
  end

  defp grant_all([], _permits), do: {:error, :no_consumers_available}

  # Granted to every worker before reporting, so one unreachable partition does not stop the
  # rest from flowing.
  defp grant_all(workers, permits) do
    workers
    |> Enum.map(&grant(&1, permits))
    |> Enum.find(:ok, &(&1 != :ok))
  end

  defp grant_all(workers, permits, unavailable?) do
    case grant_all(workers, permits) do
      :ok when unavailable? -> {:error, :no_consumers_available}
      result -> result
    end
  end

  # A worker listed a moment ago can be gone by the time it is called, which exits rather than
  # answering; that is a failure to report, not one to propagate.
  defp grant(worker, permits) do
    Worker.send_flow(worker, permits)
  catch
    :exit, reason -> {:error, reason}
  end

  @doc """
  Returns the topic a consumer is subscribed to.

  A stable root returns the topic it was configured with. This remains the exact topic for a
  consumer started on a concrete partition such as `topic-partition-3`; a root that discovered
  a partitioned base topic returns that base topic.

  A worker returns its resolved topic, which is the concrete partition for a partitioned
  consumer.
  """
  @spec topic(pid()) :: String.t() | {:error, :not_found}
  def topic(consumer) do
    case Topology.kind(consumer) do
      :worker ->
        Worker.topic(consumer)

      :group ->
        {:error, :not_found}

      :topology ->
        Topology.topic(consumer)
    end
  catch
    :exit, _reason -> {:error, :not_found}
  end

  # Initial discovery has no groups. Once constructed, their child ids remain present while
  # workers restart, so this distinguishes initialization without calling the discovery process.
  defp topology_workers(consumer) do
    case Topology.groups(consumer) do
      [] ->
        :initializing

      groups ->
        workers_by_group =
          Enum.map(groups, fn
            {_index, group} when is_pid(group) -> Topology.workers(group)
            {_index, _not_running} -> []
          end)

        {:ready, List.flatten(workers_by_group), Enum.any?(workers_by_group, &(&1 == []))}
    end
  end

  # Two consumers in one static supervision tree need distinct ids, so the id follows
  # the same default as the consumer's name.
  @doc false
  def id(opts) do
    Keyword.get_lazy(opts, :name, fn ->
      default_name(Keyword.get(opts, :topic), Keyword.get(opts, :subscription_name))
    end)
  end

  defp default_name(topic, subscription_name), do: "#{topic}-#{subscription_name}"
end
