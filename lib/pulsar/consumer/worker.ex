defmodule Pulsar.Consumer.Worker do
  @moduledoc false

  # The GenServer behind a single consumer. Pulsar.Consumer starts these through
  # Pulsar.Topology.Group, one per partition and per :consumer_count.

  use GenServer

  alias Pulsar.Backoff
  alias Pulsar.Consumer.ChunkedMessageContext
  alias Pulsar.Consumer.DeadLetter
  alias Pulsar.Protocol
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.Schema
  alias Pulsar.Topology.Resolver

  require Logger

  @terminal_errors [
    :AuthenticationError,
    :AuthorizationError,
    :ConsumerBusy,
    :IncompatibleSchema,
    :InvalidTopicName,
    :NotAllowedError,
    :TopicNotFound,
    :UnsupportedVersionError
  ]

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
    {:nacked_messages, MapSet.new()},
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
          flow_initial: non_neg_integer(),
          flow_threshold: non_neg_integer(),
          flow_refill: non_neg_integer(),
          flow_outstanding_permits: non_neg_integer(),
          initial_position: atom(),
          force_create_topic: boolean(),
          start_message_id: {non_neg_integer(), non_neg_integer()},
          start_timestamp: non_neg_integer(),
          nacked_messages: MapSet.t(),
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

  @doc """
  Gracefully stops a consumer process.
  """
  @spec stop(GenServer.server(), term(), timeout()) :: :ok
  def stop(consumer, reason \\ :normal, timeout \\ :infinity) do
    GenServer.stop(consumer, reason, timeout)
  end

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
    # Option names and struct field names are the same, so struct/2 carries them across
    # and ignores the group-level options that are not part of a consumer's state. That
    # includes :dead_letter_root, resolved once for the whole consumer by Pulsar.Topology.
    state = %{
      struct(__MODULE__, opts)
      | consumer_id: System.unique_integer([:positive, :monotonic]),
        consumer_name: Keyword.get(opts, :name),
        schema: build_schema(Keyword.get(opts, :schema)),
        max_redelivery: max_redelivery(Keyword.get(opts, :dead_letter_policy))
    }

    Logger.debug("Starting consumer for topic #{state.topic}")

    init_args = Keyword.fetch!(opts, :init_args)

    startup_delay_ms = Keyword.fetch!(opts, :startup_delay_ms)
    startup_jitter_ms = Keyword.fetch!(opts, :startup_jitter_ms)

    if startup_delay_ms + startup_jitter_ms > 0 do
      {:ok, state, {:continue, {:startup_delay, startup_delay_ms, startup_jitter_ms, init_args}}}
    else
      {:ok, state, {:continue, {:subscribe, init_args}}}
    end
  end

  @impl true
  def handle_continue({:startup_delay, base_delay_ms, jitter_ms, init_args}, state) do
    jitter = if jitter_ms > 0, do: :rand.uniform(jitter_ms), else: 0
    total_sleep_ms = base_delay_ms + jitter
    Logger.debug("Consumer sleeping for #{total_sleep_ms}ms (base: #{base_delay_ms}ms, jitter: #{jitter}ms)")
    Process.sleep(total_sleep_ms)
    {:noreply, state, {:continue, {:subscribe, init_args}}}
  end

  def handle_continue({:subscribe, init_args}, state) do
    case Backoff.run(fn -> subscribe(state) end) do
      {:ok, broker_pid} ->
        {:noreply, %{state | broker_pid: broker_pid}, {:continue, {:seek_subscription, init_args}}}

      # Errors a second attempt cannot change: bad credentials stay bad, a malformed topic
      # stays malformed, and an :exclusive subscription already taken stays taken. Stopping
      # with :shutdown leaves the worker down instead of restarting it into the same answer,
      # and the group follows once its last worker has gone.
      {:error, {code, _message} = reason} when code in @terminal_errors ->
        {:stop, {:shutdown, reason}, state}

      {:error, reason} ->
        {:stop, reason, state}
    end
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
        send_initial_flow(state.broker_pid, state.consumer_id, state.flow_initial)
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
        {:stop, reason, nil}
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

    dispatch_active_state_change(state, :became_active)
  end

  def handle_info({:broker_message, %Binary.CommandActiveConsumerChange{is_active: false}}, state) do
    Logger.info("Consumer #{state.consumer_id} became a passive consumer for subscription #{state.subscription_name}")

    dispatch_active_state_change(state, :became_passive)
  end

  def handle_info({:broker_message, message_data}, state) do
    {messages, new_state} =
      case message_data do
        {:invalid, command, bytes, validation_error} ->
          {[build_invalid_message(command, bytes, validation_error)], state}

        {command, metadata, payload, broker_metadata} ->
          {state_after, msgs} =
            if chunked_message?(metadata) do
              # A chunk is a slice of the compressed message, so it cannot be decompressed
              # before the rest of the slices are back around it.
              maybe_assemble_chunked_message(state, command, metadata, payload, broker_metadata)
            else
              payload = maybe_uncompress(metadata, payload)
              unwrapped = unwrap_messages(metadata, payload)
              pulsar_messages = build_messages_from_unwrapped(command, metadata, broker_metadata, unwrapped)
              {state, pulsar_messages}
            end

          {msgs, state_after}

        {commands, metadatas, payload, broker_metadatas, chunk_metadata} ->
          chunk_metadata_full =
            Map.merge(chunk_metadata, %{
              commands: commands,
              metadatas: metadatas,
              broker_metadatas: broker_metadatas
            })

          message = build_message_from_chunk(chunk_metadata_full, payload)
          {[message], state}
      end

    permits_consumed = Enum.sum(Enum.map(messages, &Pulsar.Message.num_broker_messages/1))
    new_state = decrement_permits(new_state, permits_consumed)

    # A delivery the policy is done with is diverted instead of delivered, not as well as.
    new_state =
      case divert(new_state, messages) do
        {:divert, redelivery_count} -> send_batch_to_dead_letter(new_state, messages, redelivery_count)
        :deliver -> process_messages_normally(new_state, messages)
      end

    {:noreply, check_and_refill_permits(new_state)}
  end

  @impl true
  def handle_info(:trigger_redelivery, state) do
    new_state =
      if MapSet.size(state.nacked_messages) > 0 do
        nacked_list = MapSet.to_list(state.nacked_messages)

        redeliver_command = %Binary.CommandRedeliverUnacknowledgedMessages{
          consumer_id: state.consumer_id,
          message_ids: nacked_list
        }

        :ok = Pulsar.Broker.send_command(state.broker_pid, redeliver_command)
        Logger.warning("Requested redelivery of #{length(nacked_list)} NACKed messages")

        :telemetry.execute(
          [:pulsar, :consumer, :redelivery, :requested],
          %{count: length(nacked_list)},
          consumer_metadata(state)
        )

        %{state | nacked_messages: MapSet.new()}
      else
        state
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
  def handle_info(message, state) do
    case state.callback_module.handle_info(message, state.callback_state) do
      {:noreply, new_callback_state} ->
        {:noreply, %{state | callback_state: new_callback_state}}

      {:noreply, new_callback_state, timeout_or_hibernate} ->
        {:noreply, %{state | callback_state: new_callback_state}, timeout_or_hibernate}

      {:stop, reason, new_callback_state} ->
        {:stop, reason, %{state | callback_state: new_callback_state}}
    end
  end

  defp dispatch_active_state_change(state, callback_fun) do
    case apply(state.callback_module, callback_fun, [state.callback_state]) do
      {:noreply, new_callback_state} ->
        {:noreply, %{state | callback_state: new_callback_state}}

      {:noreply, new_callback_state, timeout_or_hibernate} ->
        {:noreply, %{state | callback_state: new_callback_state}, timeout_or_hibernate}

      {:stop, reason, new_callback_state} ->
        {:stop, reason, %{state | callback_state: new_callback_state}}
    end
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

    # Resolved once for the delivery: this is a call into the topology root, which is also the
    # supervisor discovery adds partitions to.
    producer = DeadLetter.producer(state.dead_letter_root)

    {diverted, nacked_ids, reason} =
      Enum.reduce(messages, {0, [], nil}, fn %Pulsar.Message{} = message, {diverted, nacked_acc, reason} ->
        message_ids_list = List.wrap(message.message_id)

        case publish_to_dead_letter(producer, message, state.topic) do
          :ok ->
            ack_command = %Binary.CommandAck{
              consumer_id: state.consumer_id,
              ack_type: :Individual,
              message_id: message_ids_list,
              validation_error: validation_error(message)
            }

            :ok = Pulsar.Broker.send_command(state.broker_pid, ack_command)
            {diverted + 1, nacked_acc, reason}

          {:error, dlq_reason} ->
            Logger.error("Failed to send message to dead letter topic: #{inspect(dlq_reason)}, leaving as nacked")
            {diverted, message_ids_list ++ nacked_acc, reason || dlq_reason}
        end
      end)

    metadata = dead_letter_metadata(state, redelivery_count)

    if diverted > 0 do
      :telemetry.execute([:pulsar, :consumer, :dead_letter, :diverted], %{count: diverted}, metadata)
    end

    if nacked_ids != [] do
      :telemetry.execute(
        [:pulsar, :consumer, :dead_letter, :failed],
        %{count: length(nacked_ids)},
        Map.put(metadata, :reason, reason)
      )
    end

    track_nacked(state, nacked_ids)
  end

  defp publish_to_dead_letter({:ok, producer}, message, origin_topic) do
    DeadLetter.divert(producer, message, origin_topic)
  end

  defp publish_to_dead_letter({:error, _reason} = error, _message, _origin_topic), do: error

  defp dead_letter_metadata(state, redelivery_count) do
    state
    |> consumer_metadata()
    |> Map.merge(%{dead_letter_topic: state.dead_letter_topic, redelivery_count: redelivery_count})
  end

  defp consumer_metadata(state) do
    %{
      topic: state.topic,
      subscription_name: state.subscription_name,
      consumer_id: state.consumer_id
    }
  end

  defp process_messages_normally(state, messages) when is_list(messages) do
    {final_callback_state, nacked_ids} =
      Enum.reduce(messages, {state.callback_state, []}, fn message, {callback_state, nacked_acc} ->
        process_single_message(state, message, callback_state, nacked_acc)
      end)

    if nacked_ids != [] do
      :telemetry.execute(
        [:pulsar, :consumer, :message, :nacked],
        %{count: length(nacked_ids)},
        consumer_metadata(state)
      )
    end

    %{track_nacked(state, nacked_ids) | callback_state: final_callback_state}
  end

  # :trigger_redelivery drains this set and only fires when a redelivery interval is configured,
  # so without one an id is deliberately not tracked rather than left in a set nothing empties.
  defp track_nacked(state, []), do: state

  defp track_nacked(%__MODULE__{redelivery_interval: nil} = state, nacked_ids) do
    Logger.debug("NACKed #{length(nacked_ids)} message(s), but no redelivery_interval configured")
    state
  end

  defp track_nacked(%__MODULE__{} = state, nacked_ids) do
    %{state | nacked_messages: MapSet.union(state.nacked_messages, MapSet.new(nacked_ids))}
  end

  defp process_single_message(state, %Pulsar.Message{} = message, callback_state, nacked_acc) do
    # Call callback for ALL messages (including incomplete chunks). An invalid one
    # goes to its own callback, so handle_message/2 can trust what it is given.
    result =
      if Pulsar.Message.valid?(message) do
        state.callback_module.handle_message(message, callback_state)
      else
        state.callback_module.handle_invalid_message(message, callback_state)
      end

    message_ids_list = List.wrap(message.message_id)

    case result do
      {:ok, new_callback_state} ->
        ack_command = %Binary.CommandAck{
          consumer_id: state.consumer_id,
          ack_type: :Individual,
          message_id: message_ids_list,
          validation_error: validation_error(message)
        }

        :ok = Pulsar.Broker.send_command(state.broker_pid, ack_command)
        {new_callback_state, nacked_acc}

      {:noreply, new_callback_state} ->
        {new_callback_state, nacked_acc}

      {:error, reason, new_callback_state} ->
        redelivery_count = Pulsar.Message.redelivery_count(message)

        Logger.warning(
          "Message processing failed: #{inspect(reason)}, tracking for redelivery (count: #{redelivery_count})"
        )

        {new_callback_state, message_ids_list ++ nacked_acc}

      unexpected_result ->
        Logger.warning("Unexpected callback result: #{inspect(unexpected_result)}, not acknowledging")
        {callback_state, nacked_acc}
    end
  end

  @impl true
  def terminate(_reason, nil) do
    :ok
  end

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

  def handle_call({:send_flow, permits}, _from, state) do
    case send_flow_command(state.broker_pid, state.consumer_id, permits, state.flow_outstanding_permits) do
      :ok ->
        new_permits = state.flow_outstanding_permits + permits
        {:reply, :ok, %{state | flow_outstanding_permits: new_permits}}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:ack, message_ids}, _from, state) when is_list(message_ids) do
    ack_command = %Binary.CommandAck{
      consumer_id: state.consumer_id,
      ack_type: :Individual,
      message_id: message_ids
    }

    :ok = Pulsar.Broker.send_command(state.broker_pid, ack_command)
    {:reply, :ok, state}
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

      {:stop, reason, reply, new_callback_state} ->
        {:stop, reason, reply, %{state | callback_state: new_callback_state}}

      {:stop, reason, new_callback_state} ->
        {:stop, reason, %{state | callback_state: new_callback_state}}
    end
  end

  @impl true
  def handle_cast(request, state) do
    case state.callback_module.handle_cast(request, state.callback_state) do
      {:noreply, new_callback_state} ->
        {:noreply, %{state | callback_state: new_callback_state}}

      {:noreply, new_callback_state, timeout_or_hibernate} ->
        {:noreply, %{state | callback_state: new_callback_state}, timeout_or_hibernate}

      {:stop, reason, new_callback_state} ->
        {:stop, reason, %{state | callback_state: new_callback_state}}
    end
  end

  ## Private Functions

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

  defp send_initial_flow(broker_pid, consumer_id, permits) do
    send_flow_command(broker_pid, consumer_id, permits, 0)
  end

  defp check_and_refill_permits(%{flow_initial: 0} = state) do
    state
  end

  defp check_and_refill_permits(state) do
    refill_threshold = state.flow_threshold
    refill_amount = state.flow_refill
    current_permits = state.flow_outstanding_permits

    if current_permits <= refill_threshold do
      do_refill_permits(state, refill_amount, current_permits)
    else
      state
    end
  end

  defp do_refill_permits(state, refill_amount, current_permits) do
    case send_flow_command(state.broker_pid, state.consumer_id, refill_amount, current_permits) do
      :ok ->
        %{state | flow_outstanding_permits: current_permits + refill_amount}

      error ->
        Logger.error("Failed to send flow command: #{inspect(error)}")
        state
    end
  end

  defp send_flow_command(broker_pid, consumer_id, permits, outstanding_permits) do
    flow_command = %Binary.CommandFlow{
      consumer_id: consumer_id,
      messagePermits: permits
    }

    start_metadata = %{
      consumer_id: consumer_id,
      permits_requested: permits,
      permits_before: outstanding_permits
    }

    :telemetry.span(
      [:pulsar, :consumer, :flow_control],
      start_metadata,
      fn ->
        result = Pulsar.Broker.send_command(broker_pid, flow_command)

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

  defp maybe_uncompress(%Binary.MessageMetadata{compression: :NONE}, payload) do
    payload
  end

  defp maybe_uncompress(%Binary.MessageMetadata{compression: :ZLIB}, compressed_payload) do
    :zlib.uncompress(compressed_payload)
  end

  defp maybe_uncompress(%Binary.MessageMetadata{compression: :LZ4} = metadata, compressed_payload) do
    {:ok, payload} = NimbleLZ4.decompress(compressed_payload, metadata.uncompressed_size)
    payload
  end

  defp maybe_uncompress(%Binary.MessageMetadata{compression: :ZSTD}, compressed_payload) do
    :ezstd.decompress(compressed_payload)
  end

  defp maybe_uncompress(%Binary.MessageMetadata{compression: :SNAPPY}, compressed_payload) do
    {:ok, payload} = :snappyer.decompress(compressed_payload)
    payload
  end

  defp build_messages_from_unwrapped(command, metadata, broker_metadata, unwrapped_messages) do
    base_message_id = command.message_id

    unwrapped_messages
    |> Enum.with_index()
    |> Enum.map(fn {{single_metadata, payload}, index} ->
      message_id =
        if single_metadata == nil do
          base_message_id
        else
          %{base_message_id | batch_index: index}
        end

      %Pulsar.Message{
        payload: payload,
        message_id: message_id,
        chunk_metadata: nil,
        raw: %{
          command: command,
          metadata: metadata,
          single_metadata: single_metadata,
          broker_metadata: broker_metadata
        }
      }
    end)
  end

  # Pulsar's validation errors all describe damaged message contents, with nothing
  # for a frame that was malformed around them, so every reason is reported as the
  # nearest available. Pulsar.Message.validation_error carries the exact one.
  defp validation_error(message) do
    if Pulsar.Message.valid?(message), do: nil, else: :ChecksumMismatch
  end

  # A chunk's uuid lives in the metadata that failed validation, so a corrupt chunk
  # cannot be tied back to its siblings; those expire as an incomplete message.
  defp build_invalid_message(command, bytes, validation_error) do
    %Pulsar.Message{
      payload: bytes,
      message_id: command.message_id,
      chunk_metadata: nil,
      validation_error: validation_error,
      raw: %{command: command, metadata: nil, single_metadata: nil, broker_metadata: nil}
    }
  end

  defp build_message_from_chunk(chunk_metadata, payload) do
    %Pulsar.Message{
      payload: payload,
      message_id: Map.get(chunk_metadata, :message_ids, []),
      chunk_metadata: chunk_metadata,
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

  # The keys `context/1` gives a callback, so chunk events group like every other signal.
  defp chunk_context(state, uuid) do
    %{
      uuid: uuid,
      consumer_id: state.consumer_id,
      topic: state.topic,
      base_topic: state.base_topic,
      partition: state.partition,
      subscription_name: state.subscription_name
    }
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
      chunk_context(state, uuid)
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
    # Every chunk repeats the metadata of the message it came from, so any of them says how
    # the reassembled payload was compressed.
    assembled_payload = ChunkedMessageContext.assemble_payload(ctx)
    complete_payload = maybe_uncompress(hd(ctx.metadatas), assembled_payload)

    :telemetry.execute(
      [:pulsar, :consumer, :chunk, :complete],
      %{
        num_chunks: ctx.num_chunks_from_msg,
        total_size: byte_size(complete_payload),
        age_ms: ChunkedMessageContext.age_ms(ctx)
      },
      chunk_context(state, uuid)
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

    message = build_message_from_chunk(chunk_metadata, complete_payload)
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
          Map.put(chunk_context(state, oldest_uuid), :reason, :queue_full)
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
          Map.put(chunk_context(state, uuid), :reason, :expired)
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
