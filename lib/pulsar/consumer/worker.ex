defmodule Pulsar.Consumer.Worker do
  @moduledoc false

  # The GenServer behind a single consumer. Pulsar.Consumer starts these through
  # Pulsar.Topology.Group, one per partition and per :consumer_count.

  use GenServer

  alias Pulsar.Backoff
  alias Pulsar.Consumer.Ack
  alias Pulsar.Consumer.ChunkedMessageContext
  alias Pulsar.Consumer.DeadLetter
  alias Pulsar.Protocol
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.Schema
  alias Pulsar.Topology.Resolver

  require Logger

  # ezstd fills this buffer up to 1000 times per call, so it is what caps how large a zstd
  # message can be. Sizing it to what the producer declared keeps one round the common case.
  @zstd_min_buffer_size 64 * 1024
  @zstd_max_buffer_size 1024 * 1024

  defstruct [
    :client,
    :topic,
    :base_topic,
    :partition,
    :subscription_name,
    :subscription_type,
    :consumer_id,
    :consumer_name,
    :callback_module,
    :callback_state,
    :broker_pid,
    :broker_monitor,
    {:ready, false},
    :flow_policy,
    :flow_initial,
    :flow_threshold,
    :flow_refill,
    {:flow_outstanding_permits, 0},
    :initial_position,
    :durable,
    :force_create_topic,
    :read_compacted,
    :start_message_id,
    :start_timestamp,
    :acks,
    :redelivery_interval,
    :max_redelivery,
    :dead_letter_root,
    :dead_letter_topic,
    {:chunked_message_contexts, %{}},
    :max_pending_chunked_messages,
    :expire_incomplete_chunked_message_after,
    :chunk_cleanup_interval,
    :schema,
    :schema_version
  ]

  @type flow_mfa :: {module(), atom(), [term()]}

  @type t :: %__MODULE__{
          topic: String.t(),
          base_topic: String.t(),
          partition: non_neg_integer() | nil,
          subscription_name: String.t(),
          subscription_type: atom(),
          consumer_id: integer(),
          consumer_name: String.t() | nil,
          callback_module: module(),
          callback_state: term(),
          broker_pid: pid(),
          broker_monitor: reference(),
          ready: boolean(),
          flow_policy: :auto | flow_mfa(),
          flow_initial: non_neg_integer(),
          flow_threshold: non_neg_integer(),
          flow_refill: non_neg_integer(),
          flow_outstanding_permits: non_neg_integer(),
          initial_position: atom(),
          force_create_topic: boolean(),
          start_message_id: {non_neg_integer(), non_neg_integer()},
          start_timestamp: non_neg_integer(),
          acks: Ack.t(),
          redelivery_interval: non_neg_integer() | nil,
          max_redelivery: non_neg_integer() | nil,
          dead_letter_root: pid() | nil,
          dead_letter_topic: String.t() | nil,
          chunked_message_contexts: %{optional(String.t()) => ChunkedMessageContext.t()},
          max_pending_chunked_messages: non_neg_integer(),
          expire_incomplete_chunked_message_after: non_neg_integer(),
          schema: Schema.t() | nil,
          schema_version: binary() | nil
        }

  ## Public API

  @doc """
  Starts one consumer process.

  Takes `Pulsar.Consumer`'s options, already validated against `Pulsar.Consumer.Options`
  and given the `:name` of this worker within its group.
  """
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc false
  @spec ready?(pid(), timeout()) :: boolean()
  def ready?(consumer, timeout), do: GenServer.call(consumer, :ready?, timeout)

  @doc """
  Returns the fully resolved topic this consumer is subscribed to.

  For partitioned topics, this returns the specific partition topic
  (e.g. `persistent://public/default/my-topic-partition-0`) rather than
  the base topic name it was started with.
  """
  @spec topic(GenServer.server()) :: String.t()
  def topic(consumer) do
    GenServer.call(consumer, :topic)
  end

  @doc """
  Grants this worker more flow permits. Backs `Pulsar.Consumer.send_flow/3`.
  """
  @spec send_flow(pid(), pos_integer()) :: :ok | {:error, term()}
  def send_flow(consumer, permits) when is_integer(permits) and permits > 0 do
    GenServer.call(consumer, {:send_flow, permits})
  end

  @doc """
  Acknowledges one or more messages, batching a list into a single ACK command.
  Backs `Pulsar.Consumer.ack/2`.
  """
  @spec ack(pid(), Binary.MessageIdData.t() | [Binary.MessageIdData.t()]) :: :ok | {:error, term()}
  def ack(consumer, message_ids) when is_list(message_ids) do
    GenServer.call(consumer, {:ack, message_ids})
  end

  def ack(consumer, message_id) do
    ack(consumer, [message_id])
  end

  @doc """
  Negatively acknowledges one or more messages, tracking them for redelivery when
  `:redelivery_interval` is set. Backs `Pulsar.Consumer.nack/2`.
  """
  @spec nack(pid(), Binary.MessageIdData.t() | [Binary.MessageIdData.t()]) :: :ok | {:error, term()}
  def nack(consumer, message_ids) when is_list(message_ids) do
    GenServer.call(consumer, {:nack, message_ids})
  end

  def nack(consumer, message_id) do
    nack(consumer, [message_id])
  end

  ## GenServer Callbacks

  @impl true
  def init(opts) do
    # A worker is taken out of the tree by its parent rather than by exiting, and an untrapped
    # child is killed outright by that - skipping terminate/2, and with it the callback's.
    Process.flag(:trap_exit, true)

    # Option names and struct field names are the same, so struct/2 carries them across
    # and ignores the group-level options that are not part of a consumer's state. That
    # includes :dead_letter_root, resolved once for the whole consumer by Pulsar.Topology.
    state = %{
      struct(__MODULE__, opts)
      | consumer_id: System.unique_integer([:positive, :monotonic]),
        consumer_name: Keyword.get(opts, :name),
        acks: Ack.new(Keyword.take(opts, [:batch_index_ack_enabled])),
        schema: build_schema(Keyword.get(opts, :schema)),
        max_redelivery: max_redelivery(Keyword.get(opts, :dead_letter_policy))
    }

    Logger.debug("Starting consumer for topic #{state.topic}")

    startup_delay_ms = Keyword.fetch!(opts, :startup_delay_ms)
    startup_jitter_ms = Keyword.fetch!(opts, :startup_jitter_ms)

    init_args = Keyword.fetch!(opts, :init_args)

    if startup_delay_ms + startup_jitter_ms > 0 do
      {:ok, state, {:continue, {:startup_delay, startup_delay_ms, startup_jitter_ms, init_args}}}
    else
      {:ok, state, {:continue, {:subscribe, init_args}}}
    end
  end

  # By timer rather than by sleeping: a worker that traps exits is only shut down while it is
  # reading its mailbox, and one asleep in a callback is killed after the shutdown timeout -
  # skipping the terminate/2 that trapping exists to guarantee.
  @impl true
  def handle_continue({:startup_delay, base_delay_ms, jitter_ms, init_args}, state) do
    jitter = if jitter_ms > 0, do: :rand.uniform(jitter_ms), else: 0
    total_delay_ms = base_delay_ms + jitter
    Logger.debug("Consumer waiting #{total_delay_ms}ms (base: #{base_delay_ms}ms, jitter: #{jitter}ms)")
    Process.send_after(self(), {:subscribe_now, init_args}, total_delay_ms)

    {:noreply, state}
  end

  def handle_continue({:subscribe, init_args}, state) do
    attempt_subscribe(state, init_args, 0, Backoff.deadline())
  end

  def handle_continue({:seek_subscription, init_args}, state) do
    case maybe_seek_subscription(
           state.broker_pid,
           state.consumer_id,
           state.start_message_id,
           state.start_timestamp
         ) do
      {:ok, :skipped} ->
        {:noreply, state, {:continue, {:send_initial_flow, init_args}}}

      {:ok, _response} ->
        {:noreply, state, {:continue, {:resubscribe, init_args}}}

      {:error, {:UnknownError, "Reset subscription to publish time error: Failed to fence subscription"}} ->
        {:noreply, state, {:continue, {:resubscribe, init_args}}}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def handle_continue({:resubscribe, init_args}, state) do
    receive do
      # When sending a Seek, we expect the broker to send a CloseConsumer
      {:broker_message, %Binary.CommandCloseConsumer{}} ->
        case subscribe_to_topic(
               state.broker_pid,
               state.topic,
               state.subscription_name,
               state.subscription_type,
               state.consumer_id,
               state.consumer_name,
               initial_position: state.initial_position,
               durable: state.durable,
               force_create_topic: state.force_create_topic,
               read_compacted: state.read_compacted,
               schema: state.schema
             ) do
          {:ok, _response} ->
            {:noreply, state, {:continue, {:send_initial_flow, init_args}}}

          {:error, reason} ->
            {:stop, reason, state}
        end
    after
      # TO-DO: Should be configurable
      1_000 ->
        {:stop, :no_close_consumer, state}
    end
  end

  def handle_continue({:send_initial_flow, init_args}, state) do
    result =
      if state.flow_initial > 0 do
        send_flow_command(state, state.flow_initial)
      else
        :ok
      end

    case result do
      :ok ->
        broker_monitor = Process.monitor(state.broker_pid)
        schedule_redelivery(state.redelivery_interval)
        schedule_chunk_cleanup(state.chunk_cleanup_interval)

        {:noreply,
         %{
           state
           | broker_monitor: broker_monitor,
             flow_outstanding_permits: state.flow_initial
         }, {:continue, {:init_callback, init_args}}}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def handle_continue({:init_callback, init_args}, state) do
    case state.callback_module.init(init_args, context(state)) do
      {:ok, callback_state} ->
        {:noreply, %{state | callback_state: callback_state, ready: true}}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  defp context(state) do
    %{
      topic: state.topic,
      base_topic: state.base_topic,
      partition: state.partition,
      subscription_name: state.subscription_name,
      subscription_type: state.subscription_type,
      consumer_name: state.consumer_name
    }
  end

  defp subscribe(state) do
    with {:ok, broker_pid} <- Resolver.lookup_topic(state.topic, client: state.client),
         :ok <- Pulsar.Broker.register_consumer(broker_pid, state.consumer_id, self()),
         {:ok, _response} <-
           subscribe_to_topic(
             broker_pid,
             state.topic,
             state.subscription_name,
             state.subscription_type,
             state.consumer_id,
             state.consumer_name,
             initial_position: state.initial_position,
             durable: state.durable,
             force_create_topic: state.force_create_topic,
             read_compacted: state.read_compacted,
             schema: state.schema
           ) do
      {:ok, broker_pid}
    end
  end

  @impl true
  def handle_info({:broker_message, %Binary.CommandCloseConsumer{}}, state) do
    {:stop, :broker_close_requested, state}
  end

  def handle_info({:broker_message, %Binary.CommandActiveConsumerChange{is_active: true}}, state) do
    Logger.info("Consumer #{state.consumer_id} became the active consumer for subscription #{state.subscription_name}")

    dispatch_event(state, :became_active)
  end

  def handle_info({:broker_message, %Binary.CommandActiveConsumerChange{is_active: false}}, state) do
    Logger.info("Consumer #{state.consumer_id} became a passive consumer for subscription #{state.subscription_name}")

    dispatch_event(state, :became_passive)
  end

  def handle_info({:broker_message, %Binary.CommandReachedEndOfTopic{}}, state) do
    Logger.info("Consumer #{state.consumer_id} reached the end of terminated topic #{state.topic}")

    dispatch_event(state, :reached_end_of_topic)
  end

  def handle_info({:broker_message, message_data}, state) do
    {messages, skipped_ids, new_state} =
      case message_data do
        {:invalid, command, bytes, validation_error} ->
          report_invalid_message(state, validation_error)
          {[build_invalid_message(command, bytes, validation_error)], [], state}

        {command, metadata, payload, broker_metadata} ->
          if chunked_message?(metadata) do
            # A chunk is a slice of the compressed message, so it cannot be decompressed
            # before the rest of the slices are back around it.
            {state_after, msgs} = maybe_assemble_chunked_message(state, command, metadata, payload, broker_metadata)
            {msgs, [], state_after}
          else
            {msgs, skipped_ids} = build_messages_from_entry(command, metadata, payload, broker_metadata, state)
            {msgs, skipped_ids, state}
          end

        {commands, metadatas, payload, broker_metadatas, chunk_metadata} ->
          chunk_metadata_full =
            Map.merge(chunk_metadata, %{
              commands: commands,
              metadatas: metadatas,
              broker_metadatas: broker_metadatas
            })

          message = build_message_from_chunk(chunk_metadata_full, payload)
          {[message], [], state}
      end

    # A skipped message was still sent, and still charged a permit.
    permits_consumed = length(skipped_ids) + Enum.sum(Enum.map(messages, &Pulsar.Message.num_broker_messages/1))

    # No callback runs for a skipped message, so nothing else can count it off its entry.
    new_state =
      new_state
      |> decrement_permits(permits_consumed)
      |> ack_message_ids(skipped_ids)

    # A delivery the policy is done with is diverted instead of delivered, not as well as.
    new_state =
      case divert(new_state, messages) do
        {:divert, redelivery_count} -> send_batch_to_dead_letter(new_state, messages, redelivery_count)
        :deliver -> process_messages_normally(new_state, messages)
      end

    {:noreply, apply_flow_policy(new_state, permits_consumed)}
  end

  @impl true
  def handle_info(:trigger_redelivery, state) do
    new_state =
      case Ack.take_nacked(state.acks) do
        {[], _acks} ->
          state

        {entry_ids, acks} ->
          request_redelivery(state, entry_ids)
          %{state | acks: acks}
      end

    schedule_redelivery(state.redelivery_interval)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:cleanup_expired_chunks, state) do
    new_state = cleanup_expired_chunked_messages(state)
    schedule_chunk_cleanup(state.chunk_cleanup_interval)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(
        {:DOWN, monitor_ref, :process, broker_pid, reason},
        %__MODULE__{broker_monitor: monitor_ref, broker_pid: broker_pid} = state
      ) do
    Logger.info("Broker #{inspect(broker_pid)} crashed: #{inspect(reason)}, consumer will restart")

    {:stop, :broker_crashed, state}
  end

  @impl true
  def handle_info({:subscribe_now, init_args}, state) do
    {:noreply, state, {:continue, {:subscribe, init_args}}}
  end

  def handle_info({:retry_subscribe, init_args, backoff, deadline}, state) do
    attempt_subscribe(state, init_args, backoff, deadline)
  end

  # Untrapped, any linked process dying took this one with it, and a reconnecting broker relies
  # on that to make its consumers subscribe again. Trapping is for terminate/2 and nothing else,
  # so every exit is still fatal - gen_server answers the parent's itself and never sends it here.
  def handle_info({:EXIT, pid, reason}, state) do
    Logger.info("#{inspect(pid)} exited: #{inspect(reason)}, consumer will restart")

    {:stop, :linked_process_exited, state}
  end

  # Nothing is handed to a callback before its init/2 has run, and the startup delay and the
  # gaps between subscribe retries both leave the worker reading its mailbox.
  def handle_info(_message, %__MODULE__{callback_state: nil} = state), do: {:noreply, state}

  @impl true
  def handle_info(message, state) do
    case state.callback_module.handle_info(message, state.callback_state) do
      {:noreply, new_callback_state} ->
        {:noreply, %{state | callback_state: new_callback_state}}

      {:noreply, new_callback_state, timeout_or_hibernate} ->
        {:noreply, %{state | callback_state: new_callback_state}, timeout_or_hibernate}
    end
  end

  # The return is ignored, as terminate/2's is: these tell a callback something happened, and a
  # consumer is stopped through Pulsar.Consumer.stop/2 rather than from inside one.
  defp dispatch_event(%__MODULE__{callback_state: nil} = state, _callback_fun), do: {:noreply, state}

  # These tell a callback something happened; stopping is Pulsar.Consumer.stop/2's job, so the
  # only thing to answer with is the state to carry on with.
  defp dispatch_event(state, callback_fun) do
    {:ok, callback_state} = apply(state.callback_module, callback_fun, [state.callback_state])

    {:noreply, %{state | callback_state: callback_state}}
  end

  # Answered before counting, so a consumer with no dead letter policy never walks a delivery.
  defp divert(%__MODULE__{max_redelivery: nil}, _messages), do: :deliver
  defp divert(%__MODULE__{dead_letter_root: nil}, _messages), do: :deliver

  defp divert(%__MODULE__{} = state, messages) do
    redelivery_count = batch_redelivery_count(messages)

    if redelivery_count >= state.max_redelivery, do: {:divert, redelivery_count}, else: :deliver
  end

  # One delivery diverts as a whole: a batch arrives as a single broker command and a chunked
  # message answers the maximum across its chunks, so every message built from it carries the
  # same redelivery count.
  defp batch_redelivery_count(messages) do
    messages
    |> Enum.map(&Pulsar.Message.redelivery_count/1)
    |> Enum.max(fn -> 0 end)
  end

  defp send_batch_to_dead_letter(state, messages, redelivery_count) do
    Logger.warning(
      "Redelivery count of #{redelivery_count} reached max redelivery of " <>
        "#{state.max_redelivery}, diverting to the dead letter topic"
    )

    {:ok, producer} = DeadLetter.producer(state.dead_letter_root)

    state =
      Enum.reduce(messages, state, fn %Pulsar.Message{} = message, acc_state ->
        :ok = DeadLetter.divert(producer, message, acc_state.topic)
        ack_message_ids(acc_state, List.wrap(message.message_id), validation_error(message))
      end)

    :telemetry.execute(
      [:pulsar, :consumer, :dead_letter, :diverted],
      %{count: length(messages)},
      dead_letter_metadata(state, redelivery_count)
    )

    state
  end

  defp dead_letter_metadata(state, redelivery_count) do
    state
    |> consumer_metadata()
    |> Map.merge(%{dead_letter_topic: state.dead_letter_topic, redelivery_count: redelivery_count})
  end

  # Shared by every event this consumer emits, so they all group the same way.
  defp consumer_metadata(state) do
    %{
      topic: state.topic,
      base_topic: state.base_topic,
      partition: state.partition,
      subscription_name: state.subscription_name,
      consumer_id: state.consumer_id
    }
  end

  defp process_messages_normally(state, messages) when is_list(messages) do
    {new_state, nacked_ids} =
      Enum.reduce(messages, {state, []}, fn message, {acc_state, nacked_acc} ->
        process_single_message(acc_state, message, nacked_acc)
      end)

    if nacked_ids != [] do
      :telemetry.execute(
        [:pulsar, :consumer, :message, :nacked],
        %{count: length(nacked_ids)},
        consumer_metadata(state)
      )
    end

    track_nacked(new_state, nacked_ids)
  end

  defp request_redelivery(state, entry_ids) do
    redeliver_command = %Binary.CommandRedeliverUnacknowledgedMessages{
      consumer_id: state.consumer_id,
      message_ids: entry_ids
    }

    :ok = Pulsar.Broker.send_command(state.broker_pid, redeliver_command)
    Logger.warning("Requested redelivery of #{length(entry_ids)} NACKed messages")

    :telemetry.execute(
      [:pulsar, :consumer, :redelivery, :requested],
      %{count: length(entry_ids)},
      consumer_metadata(state)
    )
  end

  defp track_nacked(state, []), do: state

  # Nothing fires :trigger_redelivery without an interval, so the entry is not coming back and
  # its tally is deliberately kept: acking the message later still completes it.
  defp track_nacked(%__MODULE__{redelivery_interval: nil} = state, nacked_ids) do
    Logger.debug("NACKed #{length(nacked_ids)} message(s), but no redelivery_interval configured")
    state
  end

  defp track_nacked(%__MODULE__{} = state, nacked_ids) do
    %{state | acks: Ack.record_nack(state.acks, nacked_ids)}
  end

  defp ack_message_ids(state, message_ids, validation_error \\ nil) do
    {to_send, acks} = Ack.record_ack(state.acks, message_ids)

    case to_send do
      [] -> %{state | acks: acks}
      ids -> send_ack(%{state | acks: acks}, ids, validation_error)
    end
  end

  defp send_ack(state, message_ids, validation_error) do
    ack_command = %Binary.CommandAck{
      consumer_id: state.consumer_id,
      ack_type: :Individual,
      message_id: message_ids,
      validation_error: validation_error
    }

    :ok = Pulsar.Broker.send_command(state.broker_pid, ack_command)
    state
  end

  defp process_single_message(state, %Pulsar.Message{} = message, nacked_acc) do
    # Call callback for ALL messages (including incomplete chunks). An invalid one
    # goes to its own callback, so handle_message/2 can trust what it is given.
    result =
      if Pulsar.Message.valid?(message) do
        state.callback_module.handle_message(message, state.callback_state)
      else
        state.callback_module.handle_invalid_message(message, state.callback_state)
      end

    message_ids_list = List.wrap(message.message_id)

    case result do
      {:ok, new_callback_state} ->
        state = ack_message_ids(state, message_ids_list, validation_error(message))
        {%{state | callback_state: new_callback_state}, nacked_acc}

      {:noreply, new_callback_state} ->
        {%{state | callback_state: new_callback_state}, nacked_acc}

      {:error, reason, new_callback_state} ->
        redelivery_count = Pulsar.Message.redelivery_count(message)

        Logger.warning(
          "Message processing failed: #{inspect(reason)}, tracking for redelivery (count: #{redelivery_count})"
        )

        {%{state | callback_state: new_callback_state}, message_ids_list ++ nacked_acc}
    end
  end

  @impl true
  def terminate(reason, state) do
    try do
      state.callback_module.terminate(reason, state.callback_state)
    rescue
      error ->
        Logger.warning("Error in callback terminate function: #{inspect(error)}")
    end

    :ok
  end

  @impl true
  def handle_call(:ready?, _from, state), do: {:reply, state.ready, state}

  def handle_call(:topic, _from, state) do
    {:reply, state.topic, state}
  end

  def handle_call(request, _from, %__MODULE__{broker_pid: nil} = state)
      when elem(request, 0) in [:send_flow, :ack, :nack] do
    {:reply, {:error, :not_ready}, state}
  end

  def handle_call({:send_flow, permits}, _from, state) do
    case send_flow_command(state, permits) do
      :ok ->
        new_permits = state.flow_outstanding_permits + permits
        {:reply, :ok, %{state | flow_outstanding_permits: new_permits}}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:ack, message_ids}, _from, state) when is_list(message_ids) do
    {:reply, :ok, ack_message_ids(state, message_ids)}
  end

  def handle_call({:nack, message_ids}, _from, state) when is_list(message_ids) do
    # Manual NACK follows the same pattern as auto-NACK:
    # - Add to nacked_messages if redelivery_interval is configured (for periodic redelivery)
    # - Note: DLQ logic cannot be applied here since we don't have redelivery_count or payload
    #   from the broker. The DLQ will only trigger on subsequent redeliveries when the message
    #   comes back through handle_message with an updated redelivery_count.

    if message_ids != [] do
      :telemetry.execute(
        [:pulsar, :consumer, :message, :nacked],
        %{count: length(message_ids)},
        consumer_metadata(state)
      )
    end

    {:reply, :ok, track_nacked(state, message_ids)}
  end

  def handle_call(_request, _from, %__MODULE__{callback_state: nil} = state) do
    {:reply, {:error, :not_ready}, state}
  end

  def handle_call(request, from, state) do
    case state.callback_module.handle_call(request, from, state.callback_state) do
      {:reply, reply, new_callback_state} ->
        {:reply, reply, %{state | callback_state: new_callback_state}}

      {:reply, reply, new_callback_state, timeout_or_hibernate} ->
        {:reply, reply, %{state | callback_state: new_callback_state}, timeout_or_hibernate}

      {:noreply, new_callback_state} ->
        {:noreply, %{state | callback_state: new_callback_state}}

      {:noreply, new_callback_state, timeout_or_hibernate} ->
        {:noreply, %{state | callback_state: new_callback_state}, timeout_or_hibernate}
    end
  end

  @impl true
  def handle_cast(_request, %__MODULE__{callback_state: nil} = state), do: {:noreply, state}

  def handle_cast(request, state) do
    case state.callback_module.handle_cast(request, state.callback_state) do
      {:noreply, new_callback_state} ->
        {:noreply, %{state | callback_state: new_callback_state}}

      {:noreply, new_callback_state, timeout_or_hibernate} ->
        {:noreply, %{state | callback_state: new_callback_state}, timeout_or_hibernate}
    end
  end

  ## Private Functions

  defp attempt_subscribe(state, init_args, backoff, deadline) do
    case subscribe(state) do
      {:ok, broker_pid} ->
        {:noreply, %{state | broker_pid: broker_pid}, {:continue, {:seek_subscription, init_args}}}

      {:error, reason} ->
        case Backoff.retry_in(reason, backoff, deadline) do
          {:retry, wait, next} ->
            Process.send_after(self(), {:retry_subscribe, init_args, next, deadline}, wait)
            {:noreply, state}

          :give_up ->
            Logger.error("Consumer for #{state.topic} cannot subscribe: #{inspect(reason)}")
            {:stop, reason, state}
        end
    end
  end

  defp initial_position(:latest), do: :Latest
  defp initial_position(:earliest), do: :Earliest

  defp subscribe_to_topic(broker_pid, topic, subscription_name, subscription_type, consumer_id, consumer_name, opts) do
    request_id = System.unique_integer([:positive, :monotonic])
    initial_position = opts |> Keyword.get(:initial_position) |> initial_position()
    durable = Keyword.get(opts, :durable, true)
    force_create_topic = Keyword.get(opts, :force_create_topic, true)
    read_compacted = Keyword.get(opts, :read_compacted, false)
    schema = Keyword.get(opts, :schema)

    subscribe_command =
      %Binary.CommandSubscribe{
        topic: topic,
        subscription: subscription_name,
        subType: Protocol.to_subscription_type(subscription_type),
        consumer_id: consumer_id,
        consumer_name: consumer_name,
        request_id: request_id,
        initialPosition: initial_position,
        durable: durable,
        force_topic_creation: force_create_topic,
        read_compacted: read_compacted,
        schema: Schema.to_binary(schema)
      }

    Pulsar.Broker.send_request(broker_pid, subscribe_command)
  end

  defp maybe_seek_subscription(_broker_pid, _consumer_id, nil, nil) do
    {:ok, :skipped}
  end

  defp maybe_seek_subscription(broker_pid, consumer_id, message_id, timestamp) do
    request_id = System.unique_integer([:positive, :monotonic])

    seek_command =
      %Binary.CommandSeek{
        consumer_id: consumer_id,
        request_id: request_id
      }
      |> maybe_add_message_id(message_id)
      |> maybe_add_timestamp(timestamp)

    Pulsar.Broker.send_request(broker_pid, seek_command)
  end

  defp decrement_permits(state, count) do
    new_permits = max(state.flow_outstanding_permits - count, 0)
    %{state | flow_outstanding_permits: new_permits}
  end

  defp apply_flow_policy(state, consumed) do
    case decide_flow(state, consumed) do
      :ok ->
        state

      {:grant, permits} when is_integer(permits) and permits > 0 ->
        grant_permits(state, permits)

      unexpected_result ->
        Logger.warning("Unexpected flow policy result: #{inspect(unexpected_result)}, granting no permits")
        state
    end
  end

  defp decide_flow(%__MODULE__{flow_policy: :auto} = state, _consumed) do
    if state.flow_outstanding_permits <= state.flow_threshold and state.flow_refill > 0 do
      {:grant, state.flow_refill}
    else
      :ok
    end
  end

  defp decide_flow(%__MODULE__{flow_policy: {module, function, args}} = state, consumed) do
    flow = %{consumed: consumed, outstanding: state.flow_outstanding_permits}

    apply(module, function, [flow | args])
  end

  defp grant_permits(state, permits) do
    case send_flow_command(state, permits) do
      :ok ->
        %{state | flow_outstanding_permits: state.flow_outstanding_permits + permits}

      {:error, reason} ->
        exit({:send_flow_failed, reason})
    end
  end

  defp send_flow_command(state, permits) do
    flow_command = %Binary.CommandFlow{
      consumer_id: state.consumer_id,
      messagePermits: permits
    }

    outstanding_permits = state.flow_outstanding_permits

    start_metadata = %{
      consumer_id: state.consumer_id,
      permits_requested: permits,
      permits_before: outstanding_permits
    }

    :telemetry.span(
      [:pulsar, :consumer, :flow_control],
      start_metadata,
      fn ->
        result = Pulsar.Broker.send_command(state.broker_pid, flow_command)

        stop_metadata = %{success: true, permits_after: outstanding_permits + permits}

        {result, Map.merge(start_metadata, stop_metadata)}
      end
    )
  end

  defp maybe_add_message_id(command, nil), do: command

  defp maybe_add_message_id(command, {ledger_id, entry_id}) do
    %{command | message_id: %Binary.MessageIdData{ledgerId: ledger_id, entryId: entry_id}}
  end

  defp maybe_add_timestamp(command, nil), do: command

  defp maybe_add_timestamp(command, timestamp) do
    %{command | message_publish_time: timestamp}
  end

  defp schedule_redelivery(nil), do: :ok

  defp schedule_redelivery(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :trigger_redelivery, interval)
    :ok
  end

  # Pulsar.Consumer.Options requires a positive integer here, so a policy always carries one.
  defp max_redelivery(nil), do: nil
  defp max_redelivery(policy) when is_list(policy), do: Keyword.fetch!(policy, :max_redelivery)

  defp maybe_uncompress(%Binary.MessageMetadata{compression: :NONE}, payload), do: {:ok, payload}

  defp maybe_uncompress(%Binary.MessageMetadata{} = metadata, compressed_payload) do
    with {:ok, payload} <- uncompress(metadata, compressed_payload) do
      verify_uncompressed_size(payload, metadata.uncompressed_size)
    end
  end

  defp uncompress(%Binary.MessageMetadata{compression: :ZLIB}, compressed_payload) do
    {:ok, :zlib.uncompress(compressed_payload)}
  rescue
    error in ErlangError -> {:error, error.original}
  end

  defp uncompress(%Binary.MessageMetadata{compression: :LZ4} = metadata, compressed_payload) do
    NimbleLZ4.decompress(compressed_payload, metadata.uncompressed_size)
  end

  defp uncompress(%Binary.MessageMetadata{compression: :ZSTD} = metadata, compressed_payload) do
    buffer_size = metadata.uncompressed_size |> max(@zstd_min_buffer_size) |> min(@zstd_max_buffer_size)

    with context when is_reference(context) <- :ezstd.create_decompression_context(buffer_size),
         payload when is_list(payload) <- :ezstd.decompress_streaming(context, compressed_payload) do
      {:ok, IO.iodata_to_binary(payload)}
    end
  end

  defp uncompress(%Binary.MessageMetadata{compression: :SNAPPY}, compressed_payload) do
    :snappyer.decompress(compressed_payload)
  end

  # Protobuf preserves unknown enum values as integers.
  defp uncompress(%Binary.MessageMetadata{compression: compression}, _compressed_payload) do
    {:error, {:unsupported_compression, compression}}
  end

  # Chunk ids outside the range the producer announced can fill the context without chunk 0
  # ever arriving, leaving nothing that describes the reassembled payload.
  defp uncompress_assembled(nil, _payload, _total_chunk_msg_size, _ctx) do
    {:error, :missing_first_chunk}
  end

  defp uncompress_assembled(metadata, payload, total_chunk_msg_size, ctx)
       when total_chunk_msg_size in [0, nil] or byte_size(payload) == total_chunk_msg_size do
    if independently_compressed_chunks?(ctx) do
      {:error, :unsupported_chunk_framing}
    else
      maybe_uncompress(metadata, payload)
    end
  end

  defp uncompress_assembled(_metadata, payload, total_chunk_msg_size, _ctx) do
    {:error, {:total_chunk_msg_size_mismatch, expected: total_chunk_msg_size, actual: byte_size(payload)}}
  end

  defp independently_compressed_chunks?(%ChunkedMessageContext{num_chunks_from_msg: num_chunks} = ctx)
       when num_chunks > 1 do
    case ChunkedMessageContext.metadata(ctx, 0) do
      %Binary.MessageMetadata{compression: compression} when compression != :NONE ->
        Enum.all?(0..(num_chunks - 1), &independently_compressed_chunk?(ctx, &1))

      _ ->
        false
    end
  end

  defp independently_compressed_chunks?(_ctx), do: false

  defp independently_compressed_chunk?(ctx, chunk_id) do
    with %Binary.MessageMetadata{} = metadata <- ChunkedMessageContext.metadata(ctx, chunk_id),
         payload when is_binary(payload) <- Map.get(ctx.chunks, chunk_id),
         {:ok, _uncompressed} <- maybe_uncompress(metadata, payload) do
      true
    else
      _ -> false
    end
  end

  # A payload the codec accepted is only whole if it is the size the producer recorded
  # before compressing.
  defp verify_uncompressed_size(payload, 0), do: {:ok, payload}
  defp verify_uncompressed_size(payload, size) when byte_size(payload) == size, do: {:ok, payload}

  defp verify_uncompressed_size(payload, size) do
    {:error, {:uncompressed_size_mismatch, expected: size, actual: byte_size(payload)}}
  end

  # A partly acknowledged entry is redelivered whole, carrying a set of what is still owed, and a
  # compacted topic leaves entries holding messages compaction has replaced. Neither is delivered,
  # and both still count towards their entry, so they come back to be counted off.
  defp build_messages_from_unwrapped(command, metadata, broker_metadata, unwrapped_messages) do
    base_message_id = command.message_id
    batch_size = length(unwrapped_messages)
    outstanding = Ack.outstanding(command.ack_set)

    {delivered, skipped} =
      unwrapped_messages
      |> Enum.with_index()
      |> Enum.split_with(fn {{single_metadata, _payload}, index} ->
        deliverable?(single_metadata, outstanding, index)
      end)

    messages =
      Enum.map(delivered, fn {{single_metadata, payload}, index} ->
        %Pulsar.Message{
          payload: payload,
          message_id: unwrapped_message_id(base_message_id, single_metadata, index, batch_size),
          chunk_metadata: nil,
          raw: %{
            command: command,
            metadata: metadata,
            single_metadata: single_metadata,
            broker_metadata: broker_metadata
          }
        }
      end)

    skipped_ids =
      Enum.map(skipped, fn {{single_metadata, _payload}, index} ->
        unwrapped_message_id(base_message_id, single_metadata, index, batch_size)
      end)

    {messages, skipped_ids}
  end

  # The width travels on the id so a deferred ack, arriving with nothing else, can still tell
  # when its entry is complete.
  defp unwrapped_message_id(base_message_id, nil, _index, _batch_size), do: base_message_id

  defp unwrapped_message_id(base_message_id, _single_metadata, index, batch_size) do
    %{base_message_id | batch_index: index, batch_size: batch_size}
  end

  # A delivery with no batch framing is one message, whatever set the entry arrived with.
  defp deliverable?(nil, _outstanding, _index), do: true

  defp deliverable?(single_metadata, outstanding, index) do
    Ack.deliverable?(outstanding, index) and not compacted_out?(single_metadata)
  end

  defp compacted_out?(%Binary.SingleMessageMetadata{compacted_out: true}), do: true
  defp compacted_out?(_single_metadata), do: false

  defp validation_error(%Pulsar.Message{validation_error: nil}), do: nil
  defp validation_error(%Pulsar.Message{validation_error: :decompression_failed}), do: :DecompressionError
  defp validation_error(%Pulsar.Message{validation_error: :uncompressed_size_corruption}), do: :UncompressedSizeCorruption
  defp validation_error(%Pulsar.Message{}), do: :ChecksumMismatch

  # A chunk's uuid lives in the metadata that failed validation, so a corrupt chunk
  # cannot be tied back to its siblings; those expire as an incomplete message.
  defp build_invalid_message(command, bytes, validation_error, metadata \\ nil, broker_metadata \\ nil) do
    %Pulsar.Message{
      payload: bytes,
      message_id: command.message_id,
      chunk_metadata: nil,
      validation_error: validation_error,
      raw: %{command: command, metadata: metadata, single_metadata: nil, broker_metadata: broker_metadata}
    }
  end

  defp build_messages_from_entry(command, metadata, payload, broker_metadata, state) do
    case maybe_uncompress(metadata, payload) do
      {:ok, uncompressed} ->
        unwrapped = unwrap_messages(metadata, uncompressed)
        build_messages_from_unwrapped(command, metadata, broker_metadata, unwrapped)

      {:error, reason} ->
        validation_error = report_unreadable_payload(state, reason)

        {[
           build_invalid_message(
             command,
             payload,
             validation_error,
             metadata,
             broker_metadata
           )
         ], []}
    end
  end

  defp report_unreadable_payload(state, reason) do
    {validation_error, decompression_reason} =
      case reason do
        {:uncompressed_size_mismatch, _} ->
          {:uncompressed_size_corruption, :uncompressed_size_mismatch}

        {:total_chunk_msg_size_mismatch, _} ->
          {:uncompressed_size_corruption, :total_chunk_msg_size_mismatch}

        :missing_first_chunk ->
          {:uncompressed_size_corruption, :missing_first_chunk}

        {:unsupported_compression, _} ->
          {:decompression_failed, :unsupported_compression}

        :unsupported_chunk_framing ->
          {:decompression_failed, :unsupported_chunk_framing}

        _ ->
          {:decompression_failed, :decompression_error}
      end

    :telemetry.execute(
      [:pulsar, :consumer, :message, :invalid],
      %{count: 1},
      state
      |> consumer_metadata()
      |> Map.merge(%{reason: validation_error, decompression_reason: decompression_reason, detail: reason})
    )

    validation_error
  end

  defp report_invalid_message(state, reason) do
    :telemetry.execute(
      [:pulsar, :consumer, :message, :invalid],
      %{count: 1},
      state
      |> consumer_metadata()
      |> Map.merge(%{reason: reason, decompression_reason: nil, detail: nil})
    )
  end

  defp build_message_from_chunk(chunk_metadata, payload, validation_error \\ nil) do
    %Pulsar.Message{
      payload: payload,
      message_id: Map.get(chunk_metadata, :message_ids, []),
      chunk_metadata: chunk_metadata,
      validation_error: validation_error,
      raw: %{
        command: Map.get(chunk_metadata, :commands, []),
        metadata: Map.get(chunk_metadata, :metadatas, []),
        single_metadata: [],
        broker_metadata: Map.get(chunk_metadata, :broker_metadatas, [])
      }
    }
  end

  defp unwrap_messages(metadata, payload) do
    if metadata.num_messages_in_batch > 0 do
      parse_batch_messages(payload, metadata.num_messages_in_batch, [])
    else
      [{nil, payload}]
    end
  end

  defp parse_batch_messages(<<>>, 0, acc), do: Enum.reverse(acc)
  defp parse_batch_messages(_, 0, acc), do: Enum.reverse(acc)

  defp parse_batch_messages(
         <<metadata_size::32, metadata::bytes-size(metadata_size), data::binary>> = payload,
         count,
         acc
       ) do
    single_metadata = Binary.SingleMessageMetadata.decode(metadata)

    payload_size = single_metadata.payload_size

    <<payload::bytes-size(^payload_size), rest::binary>> = data

    message = {single_metadata, payload}

    parse_batch_messages(rest, count - 1, [message | acc])
  rescue
    _ -> [{nil, payload}]
  end

  defp parse_batch_messages(payload, _, _) do
    [{nil, payload}]
  end

  defp chunked_message?(%Binary.MessageMetadata{uuid: uuid, chunk_id: chunk_id})
       when is_binary(uuid) and is_integer(chunk_id) do
    true
  end

  defp chunked_message?(_metadata), do: false

  defp schedule_chunk_cleanup(interval) when interval in [false, nil], do: :ok

  defp schedule_chunk_cleanup(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :cleanup_expired_chunks, interval)
    :ok
  end

  defp maybe_assemble_chunked_message(state, command, metadata, payload, broker_metadata) do
    base_message_id = command.message_id
    uuid = metadata.uuid
    chunk_id = metadata.chunk_id
    num_chunks = metadata.num_chunks_from_msg
    total_size = Map.get(metadata, :total_chunk_msg_size, 0)

    :telemetry.execute(
      [:pulsar, :consumer, :chunk, :received],
      %{chunk_id: chunk_id, num_chunks: num_chunks},
      Map.put(consumer_metadata(state), :uuid, uuid)
    )

    chunk_data = %{
      uuid: uuid,
      chunk_id: chunk_id,
      num_chunks: num_chunks,
      total_size: total_size,
      command: command,
      metadata: metadata,
      payload: payload,
      broker_metadata: broker_metadata,
      message_id: base_message_id
    }

    case Map.get(state.chunked_message_contexts, uuid) do
      nil -> create_chunk_context(state, chunk_data)
      ctx -> add_chunk_to_context(state, ctx, chunk_data)
    end
  end

  defp create_chunk_context(state, chunk_data) do
    if map_size(state.chunked_message_contexts) >= state.max_pending_chunked_messages do
      handle_chunk_queue_full(state, chunk_data)
    else
      do_create_chunk_context(state, chunk_data)
    end
  end

  defp do_create_chunk_context(state, chunk_data) do
    %{
      uuid: uuid,
      num_chunks: num_chunks,
      command: command,
      metadata: metadata,
      payload: payload,
      broker_metadata: broker_metadata
    } = chunk_data

    {:ok, ctx} = ChunkedMessageContext.new(command, metadata, payload, broker_metadata)
    put_chunk_context(state, uuid, ctx, num_chunks)
  end

  defp add_chunk_to_context(state, ctx, chunk_data) do
    %{
      uuid: uuid,
      num_chunks: num_chunks,
      command: command,
      metadata: metadata,
      payload: payload,
      broker_metadata: broker_metadata
    } = chunk_data

    {:ok, updated_ctx} = ChunkedMessageContext.add_chunk(ctx, command, metadata, payload, broker_metadata)
    put_chunk_context(state, uuid, updated_ctx, num_chunks)
  end

  defp put_chunk_context(state, uuid, ctx, num_chunks) do
    new_contexts = Map.put(state.chunked_message_contexts, uuid, ctx)
    new_state = %{state | chunked_message_contexts: new_contexts}

    if ChunkedMessageContext.complete?(ctx) do
      complete_chunked_message(new_state, uuid, ctx, num_chunks)
    else
      {new_state, []}
    end
  end

  defp complete_chunked_message(state, uuid, ctx, num_chunks) do
    metadata = ChunkedMessageContext.metadata(ctx, 0)
    assembled_payload = ChunkedMessageContext.assemble_payload(ctx)

    {complete_payload, validation_error} =
      case uncompress_assembled(metadata, assembled_payload, ctx.total_chunk_msg_size, ctx) do
        {:ok, payload} ->
          {payload, nil}

        {:error, reason} ->
          validation_error = report_unreadable_payload(state, reason)
          {assembled_payload, validation_error}
      end

    :telemetry.execute(
      [:pulsar, :consumer, :chunk, :complete],
      %{
        num_chunks: ctx.num_chunks_from_msg,
        total_size: byte_size(complete_payload),
        age_ms: ChunkedMessageContext.age_ms(ctx)
      },
      Map.merge(consumer_metadata(state), %{uuid: uuid, validation_error: validation_error})
    )

    final_state = %{state | chunked_message_contexts: Map.delete(state.chunked_message_contexts, uuid)}

    chunk_metadata = %{
      chunked: true,
      complete: true,
      uuid: uuid,
      num_chunks: num_chunks,
      message_ids: ChunkedMessageContext.all_message_ids(ctx),
      commands: ctx.commands,
      metadatas: ctx.metadatas,
      broker_metadatas: ctx.broker_metadatas
    }

    message = build_message_from_chunk(chunk_metadata, complete_payload, validation_error)
    {final_state, [message]}
  end

  defp handle_chunk_queue_full(state, chunk_data) do
    case ChunkedMessageContext.pop_oldest(state.chunked_message_contexts) do
      {{oldest_uuid, oldest_ctx}, remaining} ->
        Logger.warning(
          "Chunk queue full (#{state.max_pending_chunked_messages}), evicting oldest chunked message #{oldest_uuid}"
        )

        :telemetry.execute(
          [:pulsar, :consumer, :chunk, :discarded],
          %{received_chunks: oldest_ctx.received_chunks, num_chunks: oldest_ctx.num_chunks_from_msg},
          Map.merge(consumer_metadata(state), %{uuid: oldest_uuid, reason: :queue_full})
        )

        partial_payload = ChunkedMessageContext.assemble_payload(oldest_ctx)
        all_message_ids = ChunkedMessageContext.all_message_ids(oldest_ctx)

        chunk_metadata = %{
          chunked: true,
          complete: false,
          error: :queue_full,
          uuid: oldest_uuid,
          message_ids: all_message_ids,
          received_chunks: oldest_ctx.received_chunks
        }

        send(
          self(),
          {:broker_message,
           {oldest_ctx.commands, oldest_ctx.metadatas, partial_payload, oldest_ctx.broker_metadatas, chunk_metadata}}
        )

        state = %{state | chunked_message_contexts: remaining}

        do_create_chunk_context(state, chunk_data)

      {nil, _remaining} ->
        do_create_chunk_context(state, chunk_data)
    end
  end

  defp cleanup_expired_chunked_messages(state) do
    {expired, remaining} =
      ChunkedMessageContext.pop_expired(
        state.chunked_message_contexts,
        state.expire_incomplete_chunked_message_after
      )

    if !Enum.empty?(expired) do
      Logger.warning("Cleaning up #{length(expired)} expired chunked message(s)")

      Enum.each(expired, fn {uuid, ctx} ->
        :telemetry.execute(
          [:pulsar, :consumer, :chunk, :expired],
          %{
            age_ms: ChunkedMessageContext.age_ms(ctx),
            received_chunks: ctx.received_chunks,
            num_chunks: ctx.num_chunks_from_msg
          },
          Map.merge(consumer_metadata(state), %{uuid: uuid, reason: :expired})
        )

        partial_payload = ChunkedMessageContext.assemble_payload(ctx)
        all_message_ids = ChunkedMessageContext.all_message_ids(ctx)

        chunk_metadata = %{
          chunked: true,
          complete: false,
          error: :expired,
          uuid: uuid,
          message_ids: all_message_ids,
          received_chunks: ctx.received_chunks
        }

        send(
          self(),
          {:broker_message, {ctx.commands, ctx.metadatas, partial_payload, ctx.broker_metadatas, chunk_metadata}}
        )
      end)
    end

    %{state | chunked_message_contexts: remaining}
  end

  # Schema helpers

  defp build_schema(nil), do: nil

  defp build_schema(opts) when is_list(opts) do
    case Schema.new(opts) do
      {:ok, schema} -> schema
      {:error, reason} -> raise ArgumentError, "invalid schema: #{inspect(reason)}"
    end
  end
end
