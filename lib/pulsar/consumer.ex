defmodule Pulsar.Consumer do
  @moduledoc """
  A consumer subscribes to a topic and hands each message to a callback module.

  This module is how you add, inspect and stop consumers. To declare them on a client
  instead, so they start and restart with it, see `Pulsar.Client`. The callback module
  they dispatch into is `Pulsar.Consumer.Callback`.

  `start/1` adds a consumer to a running client and `stop/2` removes it. Operations target
  the logical consumer by its stable root or registered name without exposing its partition
  workers.

  `ack/2` and `nack/2` acknowledge manually, from a process other than the worker that
  delivered the message. `send_flow/3` grants permits to a worker or every worker behind
  the consumer root.

  ## Options

  #{Pulsar.Consumer.Options.docs()}
  """

  alias Pulsar.Consumer.Options
  alias Pulsar.Consumer.Worker
  alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
  alias Pulsar.Topic
  alias Pulsar.Topology

  @default_client :default

  @doc false
  def child_spec(opts) do
    %{
      id: {__MODULE__, id(opts)},
      start: {__MODULE__, :start_link, [opts]},
      restart: :transient,
      type: :supervisor
    }
  end

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
      Keyword.put_new_lazy(opts, :name, fn ->
        default_name(topic, Keyword.fetch!(opts, :subscription_name))
      end)

    Topology.start_link(Worker, Pulsar.Client.consumer_registry(client), :consumer_count, opts)
  end

  @doc """
  Adds a consumer to a running client.

  For consumers whose set is only known at runtime. Prefer the client's `:consumers` for
  ones known up front: a consumer added here is not recreated if the client restarts.

  Returns once the stable consumer supervisor has been registered. Topic discovery and
  worker initialization continue asynchronously; inspection and flow operations return
  `{:error, :not_ready}` until discovery completes.
  """
  @spec start(keyword()) :: DynamicSupervisor.on_start_child()
  def start(opts) when is_list(opts) do
    opts = Options.validate!(opts)
    client = Keyword.fetch!(opts, :client)

    Pulsar.Client.start_resource(Pulsar.Client.consumer_supervisor(client), {__MODULE__, opts})
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

  def stop(consumer, _opts) when is_pid(consumer), do: Topology.remove(consumer)

  def stop(name, opts) when is_binary(name) or is_atom(name) do
    case resolve(name, opts) do
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
        MyApp.Jobs.enqueue(message.payload, ack: {self(), message.message_id_to_ack})
        {:noreply, state}
      end

  The job calls `Pulsar.Consumer.ack(consumer, message_id)` when it finishes. It has to be
  that process and not the callback: every callback function runs inside its worker, so
  `ack(self(), ...)` is a `GenServer` call a process makes to itself, which exits with
  `:calling_self` and takes the consumer down.
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

  Takes the stable consumer root, one of its worker pids, or its name. Every worker behind a
  root is granted the permits, and the first refusal is returned — retrying by name is safe,
  since a worker that already holds permits is only over-credited, and a worker that refused
  has usually been replaced by one with a different pid.

  A consumer with no workers is an error rather than a silent success: nothing was granted,
  so nothing will be delivered.
  """
  @spec send_flow(pid() | String.t() | atom(), non_neg_integer(), keyword()) :: :ok | {:error, term()}
  def send_flow(consumer, permits, opts \\ [])

  def send_flow(consumer, permits, _opts) when is_pid(consumer) do
    case Topology.kind(consumer) do
      :worker ->
        grant(consumer, permits)

      :group ->
        grant_all(Topology.workers(consumer), permits)

      :topology ->
        if Topology.initialized?(consumer),
          do: grant_all(Topology.workers(consumer), permits),
          else: {:error, :not_ready}
    end
  catch
    :exit, reason -> {:error, reason}
  end

  def send_flow(name, permits, opts) when is_binary(name) or is_atom(name) do
    case resolve(name, opts) do
      {:ok, consumer} -> send_flow(consumer, permits, opts)
      {:error, :not_found} -> {:error, :consumer_not_found}
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

  # A worker listed a moment ago can be gone by the time it is called, which exits rather than
  # answering; that is a failure to report, not one to propagate.
  defp grant(worker, permits) do
    Worker.send_flow(worker, permits)
  catch
    :exit, reason -> {:error, reason}
  end

  @doc """
  Returns the topic a consumer is subscribed to.

  Takes the stable root returned by `start/1` and reports the logical topic across all
  partitions.
  """
  @spec topic(pid()) :: String.t() | {:error, :not_found | :not_ready}
  def topic(consumer) do
    case Topology.kind(consumer) do
      :worker ->
        Worker.topic(consumer)

      :group ->
        worker_topic(consumer)

      :topology ->
        topology_topic(consumer)
    end
  catch
    :exit, _reason -> {:error, :not_found}
  end

  defp topology_topic(consumer) do
    if Topology.initialized?(consumer) do
      case worker_topic(consumer) do
        topic when is_binary(topic) -> Topic.base(topic)
        error -> error
      end
    else
      {:error, :not_ready}
    end
  end

  defp worker_topic(supervisor) do
    case Topology.workers(supervisor) do
      [worker | _rest] -> Worker.topic(worker)
      [] -> {:error, :not_found}
    end
  end

  defp resolve(name, opts) do
    client = Keyword.get(opts, :client, @default_client)

    Pulsar.Client.lookup(Pulsar.Client.consumer_registry(client), name)
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
