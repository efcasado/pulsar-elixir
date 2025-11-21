defmodule Pulsar.Consumer do
  @moduledoc """
  Pulsar consumer process that communicates with broker processes.

  This consumer uses service discovery to find the appropriate broker
  for the topic and then communicates with that broker process.

  ## Callback Module

  The consumer requires a callback module that implements the `Pulsar.Consumer.Callback`
  behaviour, providing stateful message processing capabilities.

  See `Pulsar.Consumer.Callback` for detailed documentation and examples.
  """

  use GenServer

  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.ServiceDiscovery

  require Logger

  defstruct [
    :topic,
    :subscription_name,
    :subscription_type,
    :consumer_id,
    :consumer_name,
    :callback_module,
    :callback_state,
    :broker_pid,
    :broker_monitor,
    :flow_initial,
    :flow_threshold,
    :flow_refill,
    :flow_outstanding_permits,
    :initial_position,
    :durable,
    :force_create_topic,
    :start_message_id,
    :start_timestamp,
    :nacked_messages,
    :redelivery_interval,
    :max_redelivery,
    :dead_letter_topic,
    :dead_letter_producer_pid
  ]

  @type t :: %__MODULE__{
          topic: String.t(),
          subscription_name: String.t(),
          subscription_type: String.t(),
          consumer_id: integer(),
          consumer_name: String.t() | nil,
          callback_module: module(),
          callback_state: term(),
          broker_pid: pid(),
          broker_monitor: reference(),
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
          dead_letter_topic: String.t() | nil,
          dead_letter_producer_pid: pid() | nil
        }

  ## Public API

  @doc """
  Starts a consumer process with explicit parameters.

  ## Parameters

  - `topic` - The topic to subscribe to
  - `subscription_name` - Name of the subscription
  - `subscription_type` - Type of subscription (e.g., :Exclusive, :Shared)
  - `callback_module` - Module that implements `Pulsar.Consumer.Callback` behaviour
  - `opts` - Additional options:
    - `:init_args` - Arguments passed to callback module's init/1 function
    - `:flow_initial` - Initial flow permits (default: 100). Set to 0 to disable automatic flow control and use `send_flow/2` manually.
    - `:flow_threshold` - Flow permits threshold for automatic refill (default: 50). Ignored when `:flow_initial` is 0.
    - `:flow_refill` - Flow permits refill amount (default: 50). Ignored when `:flow_initial` is 0.
    - `:initial_position` - Initial position for subscription (`:latest` or `:earliest`, defaults to `:latest`)
    - `:redelivery_interval` - Interval in milliseconds for redelivering NACKed messages (default: nil, disabled)
    - `:dead_letter_policy` - Dead letter policy configuration (default: nil, disabled):
      - `:max_redelivery` - Maximum number of redeliveries before sending to dead letter topic (must be >= 1)
      - `:topic` - Dead letter topic (optional, defaults to `<topic>-<subscription>-DLQ`)
    - `:startup_jitter_ms` - Maximum random startup delay in milliseconds to avoid thundering herd (default: 0, disabled)

  The consumer will automatically use any available broker for service discovery.
  """
  def start_link(topic, subscription_name, subscription_type, callback_module, opts \\ []) do
    {init_args, genserver_opts} = Keyword.pop(opts, :init_args, [])
    {initial_permits, genserver_opts} = Keyword.pop(genserver_opts, :flow_initial, 100)
    {refill_threshold, genserver_opts} = Keyword.pop(genserver_opts, :flow_threshold, 50)
    {refill_amount, genserver_opts} = Keyword.pop(genserver_opts, :flow_refill, 50)
    {initial_position, genserver_opts} = Keyword.pop(genserver_opts, :initial_position, :latest)
    {durable, genserver_opts} = Keyword.pop(genserver_opts, :durable, true)
    {force_create_topic, genserver_opts} = Keyword.pop(genserver_opts, :force_create_topic, true)
    {start_message_id, genserver_opts} = Keyword.pop(genserver_opts, :start_message_id, nil)
    {start_timestamp, genserver_opts} = Keyword.pop(genserver_opts, :start_timestamp, nil)

    {redelivery_interval, genserver_opts} =
      Keyword.pop(genserver_opts, :redelivery_interval, nil)

    {dead_letter_policy, genserver_opts} =
      Keyword.pop(genserver_opts, :dead_letter_policy, nil)

    {startup_jitter_ms, _genserver_opts} = Keyword.pop(genserver_opts, :startup_jitter_ms, 0)

    # TODO: add some validation to check opts are valid? (e.g. initial_permits > 0, etc)
    consumer_config = %{
      topic: topic,
      subscription_name: subscription_name,
      subscription_type: subscription_type,
      callback_module: callback_module,
      init_args: init_args,
      flow_initial: initial_permits,
      flow_threshold: refill_threshold,
      flow_refill: refill_amount,
      initial_position: initial_position,
      durable: durable,
      force_create_topic: force_create_topic,
      start_message_id: start_message_id,
      start_timestamp: start_timestamp,
      redelivery_interval: redelivery_interval,
      dead_letter_policy: dead_letter_policy,
      startup_jitter_ms: startup_jitter_ms
    }

    GenServer.start_link(__MODULE__, consumer_config, [])
  end

  @doc """
  Gracefully stops a consumer process.
  """
  @spec stop(GenServer.server(), term(), timeout()) :: :ok
  def stop(consumer, reason \\ :normal, timeout \\ :infinity) do
    GenServer.stop(consumer, reason, timeout)
  end

  @doc """
  Sends a flow command to request more messages from the broker.

  Use this function when you've disabled automatic flow control by setting
  `:flow_initial` to 0. This allows you to implement custom flow control,
  such as integrating with Broadway's demand mechanism.

  ## Parameters

  - `consumer` - The consumer process PID
  - `permits` - Number of message permits to request

  ## Examples

      # Start consumer with no automatic flow control
      {:ok, consumer_pid} = Pulsar.start_consumer(
        topic,
        subscription,
        MyCallback,
        flow_initial: 0  # Disable automatic flow
      )

      # Manually request messages based on your own demand
      Pulsar.Consumer.send_flow(consumer_pid, 10)

      # Example with Broadway demand
      def handle_demand(demand, state) do
        Pulsar.Consumer.send_flow(state.consumer, demand)
        # ... rest of logic
      end
  """
  @spec send_flow(pid(), non_neg_integer()) :: :ok | {:error, term()}
  def send_flow(consumer, permits) when is_integer(permits) and permits > 0 do
    GenServer.call(consumer, {:send_flow, permits})
  end

  @doc """
  Manually acknowledges one or more messages.

  Use this when your callback returns `{:noreply, state}` to manually control acknowledgment.
  Supports batching multiple message IDs in a single ACK command for better performance.

  ## Parameters

  - `consumer` - The consumer process PID
  - `message_ids` - A single message ID or a list of message IDs to acknowledge

  ## Examples

      # Acknowledge a single message
      def handle_message({command, _metadata, _payload, _broker_metadata}, state) do
        message_id = command.message_id
        # Process message...
        spawn(fn ->
          # Do async processing
          Pulsar.Consumer.ack(consumer_pid, message_id)
        end)
        {:noreply, state}
      end

      # Acknowledge multiple messages in batch (more efficient)
      Pulsar.Consumer.ack(consumer_pid, [message_id1, message_id2, message_id3])
  """
  @spec ack(pid(), Binary.MessageIdData.t() | [Binary.MessageIdData.t()]) :: :ok | {:error, term()}
  def ack(consumer, message_ids) when is_list(message_ids) do
    GenServer.call(consumer, {:ack, message_ids})
  end

  def ack(consumer, message_id) do
    ack(consumer, [message_id])
  end

  @doc """
  Manually negatively acknowledges one or more messages.

  Use this when your callback returns `{:noreply, state}` to manually control acknowledgment.
  Supports batching multiple message IDs in a single NACK for better performance.

  The messages will be tracked for redelivery if `:redelivery_interval` is configured.
  When the messages are redelivered and the redelivery count exceeds `:max_redelivery`,
  they will automatically be sent to the dead letter queue (if `:dead_letter_policy` is configured),
  regardless of whether you use manual or automatic acknowledgment.

  ## Parameters

  - `consumer` - The consumer process PID
  - `message_ids` - A single message ID or a list of message IDs to negatively acknowledge

  ## Examples

      # NACK a single message
      def handle_message({command, _metadata, _payload, _broker_metadata}, state) do
        message_id = command.message_id
        case process_message() do
          :ok -> Pulsar.Consumer.ack(self(), message_id)
          {:error, _reason} -> Pulsar.Consumer.nack(self(), message_id)
        end
        {:noreply, state}
      end

      # NACK multiple messages in batch (more efficient)
      Pulsar.Consumer.nack(consumer_pid, [message_id1, message_id2, message_id3])
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
  def init(consumer_config) do
    %{
      topic: topic,
      subscription_name: subscription_name,
      subscription_type: subscription_type,
      callback_module: callback_module,
      init_args: init_args,
      flow_initial: initial_permits,
      flow_threshold: refill_threshold,
      flow_refill: refill_amount,
      initial_position: initial_position,
      durable: durable,
      force_create_topic: force_create_topic,
      start_message_id: start_message_id,
      start_timestamp: start_timestamp,
      redelivery_interval: redelivery_interval,
      dead_letter_policy: dead_letter_policy,
      startup_jitter_ms: startup_jitter_ms
    } = consumer_config

    {max_redelivery, dead_letter_topic} = parse_dead_letter_policy(dead_letter_policy)

    state = %__MODULE__{
      consumer_id: System.unique_integer([:positive, :monotonic]),
      consumer_name: nil,
      topic: topic,
      subscription_name: subscription_name,
      subscription_type: subscription_type,
      callback_module: callback_module,
      flow_initial: initial_permits,
      flow_threshold: refill_threshold,
      flow_refill: refill_amount,
      flow_outstanding_permits: 0,
      initial_position: initial_position,
      durable: durable,
      force_create_topic: force_create_topic,
      start_message_id: start_message_id,
      start_timestamp: start_timestamp,
      nacked_messages: MapSet.new(),
      redelivery_interval: redelivery_interval,
      max_redelivery: max_redelivery,
      dead_letter_topic: dead_letter_topic,
      dead_letter_producer_pid: nil
    }

    Logger.info("Starting consumer for topic #{state.topic}")

    if startup_jitter_ms > 0 do
      {:ok, state, {:continue, {:startup_jitter, startup_jitter_ms, init_args}}}
    else
      {:ok, state, {:continue, {:init_callback, init_args}}}
    end
  end

  @impl true
  def handle_continue({:startup_jitter, jitter_ms, init_args}, state) do
    sleep_ms = :rand.uniform(jitter_ms)
    Logger.debug("Consumer sleeping for #{sleep_ms}ms jitter")
    Process.sleep(sleep_ms)
    {:noreply, state, {:continue, {:init_callback, init_args}}}
  end

  def handle_continue({:init_callback, init_args}, state) do
    case apply(state.callback_module, :init, [init_args]) do
      {:ok, callback_state} ->
        {:noreply, %{state | callback_state: callback_state}, {:continue, :subscribe}}

      {:error, reason} ->
        {:stop, reason, nil}
    end
  end

  def handle_continue(:subscribe, state) do
    with {:ok, broker_pid} <- ServiceDiscovery.lookup_topic(state.topic),
         :ok <- Pulsar.Broker.register_consumer(broker_pid, state.consumer_id, self()),
         {:ok, response} <-
           subscribe_to_topic(
             broker_pid,
             state.topic,
             state.subscription_name,
             state.subscription_type,
             state.consumer_id,
             initial_position: state.initial_position,
             durable: state.durable,
             force_create_topic: state.force_create_topic
           ) do
      consumer_name = Map.get(response, :consumer_name, "unknown")

      {:noreply,
       %{
         state
         | consumer_id: state.consumer_id,
           consumer_name: consumer_name,
           broker_pid: broker_pid
       }, {:continue, :seek_subscription}}
    else
      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def handle_continue(:seek_subscription, state) do
    case maybe_seek_subscription(
           state.broker_pid,
           state.consumer_id,
           state.start_message_id,
           state.start_timestamp
         ) do
      {:ok, :skipped} ->
        {:noreply, state, {:continue, :send_initial_flow}}

      {:ok, _response} ->
        {:noreply, state, {:continue, :resubscribe}}

      {:error, {:UnknownError, "Reset subscription to publish time error: Failed to fence subscription"}} ->
        {:noreply, state, {:continue, :resubscribe}}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def handle_continue(:resubscribe, state) do
    receive do
      # When sending a Seek, we expect the broker to send a CloseConsumer
      {:broker_message, %Binary.CommandCloseConsumer{}} ->
        case subscribe_to_topic(
               state.broker_pid,
               state.topic,
               state.subscription_name,
               state.subscription_type,
               state.consumer_id,
               initial_position: state.initial_position,
               durable: state.durable,
               force_create_topic: state.force_create_topic
             ) do
          {:ok, _response} ->
            {:noreply, state, {:continue, :send_initial_flow}}

          {:error, reason} ->
            {:stop, reason, state}
        end
    after
      # TO-DO: Should be configurable
      1_000 ->
        {:stop, :no_close_consumer, state}
    end
  end

  def handle_continue(:send_initial_flow, state) do
    # Only send initial flow if flow_initial > 0 (automatic flow control enabled)
    result =
      if state.flow_initial > 0 do
        send_initial_flow(state.broker_pid, state.consumer_id, state.flow_initial)
      else
        # Manual flow control - don't send initial flow
        :ok
      end

    case result do
      :ok ->
        broker_monitor = Process.monitor(state.broker_pid)
        schedule_redelivery(state.redelivery_interval)

        {:noreply,
         %{
           state
           | broker_monitor: broker_monitor,
             flow_outstanding_permits: state.flow_initial
         }, {:continue, :init_dead_letter_producer}}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def handle_continue(:init_dead_letter_producer, state) do
    if should_init_dead_letter_producer?(state) do
      case start_dead_letter_producer(state) do
        {:ok, producer_pid} ->
          Logger.info("Started dead letter producer for consumer on topic #{state.topic}")
          {:noreply, %{state | dead_letter_producer_pid: producer_pid}}

        {:error, reason} ->
          Logger.error("Failed to start dead letter producer: #{inspect(reason)}")
          {:stop, reason, state}
      end
    else
      {:noreply, state}
    end
  end

  def message_id(%Binary.CommandMessage{} = message) do
    message.message_id
  end

  @impl true
  def handle_info({:broker_message, %Binary.CommandCloseConsumer{}}, state) do
    {:stop, :broker_close_requested, state}
  end

  def handle_info({:broker_message, {command, metadata, payload, broker_metadata}}, state) do
    base_message_id = message_id(command)
    redelivery_count = Map.get(command, :redelivery_count, 0)

    payload = maybe_uncompress(metadata, payload)
    payload = unwrap_messages(metadata, payload)

    num_messages = length(payload)
    state = decrement_permits(state, num_messages)

    # Check if entire batch should go to DLQ (redelivery count applies to whole batch)
    if should_send_to_dead_letter?(state, redelivery_count) do
      Logger.warning(
        "Redelivery count of #{redelivery_count} exceeds max redelivery of #{state.max_redelivery}, sending batch to DLQ"
      )

      {final_callback_state, nacked_ids} =
        payload
        |> Enum.with_index()
        |> Enum.reduce({state.callback_state, []}, fn {{_msg_metadata, msg_payload}, index},
                                                      {callback_state, nacked_acc} ->
          message_id_to_ack =
            if index == 0 and length(payload) == 1 do
              base_message_id
            else
              %{base_message_id | batch_index: index}
            end

          case send_to_dead_letter(state, msg_payload, message_id_to_ack) do
            :ok ->
              # Successfully sent to DLQ, ACK the message asynchronously
              ack_command = %Binary.CommandAck{
                consumer_id: state.consumer_id,
                ack_type: :Individual,
                message_id: [message_id_to_ack]
              }

              :ok = Pulsar.Broker.send_command(state.broker_pid, ack_command)
              {callback_state, nacked_acc}

            {:error, dlq_reason} ->
              Logger.error("Failed to send message to dead letter topic: #{inspect(dlq_reason)}, leaving as nacked")

              {callback_state, [message_id_to_ack | nacked_acc]}
          end
        end)

      state = check_and_refill_permits(state)

      new_nacked_messages =
        if state.redelivery_interval do
          MapSet.union(state.nacked_messages, MapSet.new(nacked_ids))
        else
          state.nacked_messages
        end

      new_state = %{
        state
        | callback_state: final_callback_state,
          nacked_messages: new_nacked_messages
      }

      {:noreply, new_state}
    else
      # Process messages normally
      {final_callback_state, nacked_ids} =
        payload
        |> Enum.with_index()
        |> Enum.reduce({state.callback_state, []}, fn {{msg_metadata, msg_payload}, index},
                                                      {callback_state, nacked_acc} ->
          message_id_to_ack =
            if msg_metadata == nil do
              base_message_id
            else
              %{base_message_id | batch_index: index}
            end

          # Include message_id_to_ack for manual ACK/NACK use cases (e.g., Broadway)
          msg_args = {command, metadata, {msg_metadata, msg_payload}, broker_metadata, message_id_to_ack}

          result = apply(state.callback_module, :handle_message, [msg_args, callback_state])

          case result do
            {:ok, new_callback_state} ->
              # ACK asynchronously - no need to wait for response
              ack_command = %Binary.CommandAck{
                consumer_id: state.consumer_id,
                ack_type: :Individual,
                message_id: [message_id_to_ack]
              }

              :ok = Pulsar.Broker.send_command(state.broker_pid, ack_command)

              {new_callback_state, nacked_acc}

            {:noreply, new_callback_state} ->
              # Manual ACK mode - don't automatically ACK or NACK
              {new_callback_state, nacked_acc}

            {:error, reason, new_callback_state} ->
              Logger.warning(
                "Message processing failed: #{inspect(reason)}, tracking for redelivery (count: #{redelivery_count})"
              )

              {new_callback_state, [message_id_to_ack | nacked_acc]}

            unexpected_result ->
              Logger.warning("Unexpected callback result: #{inspect(unexpected_result)}, not acknowledging")

              {callback_state, nacked_acc}
          end
        end)

      state = check_and_refill_permits(state)

      new_nacked_messages =
        if state.redelivery_interval do
          MapSet.union(state.nacked_messages, MapSet.new(nacked_ids))
        else
          state.nacked_messages
        end

      new_state = %{
        state
        | callback_state: final_callback_state,
          nacked_messages: new_nacked_messages
      }

      {:noreply, new_state}
    end
  end

  # Handle periodic redelivery
  @impl true
  def handle_info(:trigger_redelivery, state) do
    new_state =
      if MapSet.size(state.nacked_messages) > 0 do
        nacked_list = MapSet.to_list(state.nacked_messages)

        redeliver_command = %Binary.CommandRedeliverUnacknowledgedMessages{
          consumer_id: state.consumer_id,
          message_ids: nacked_list
        }

        case Pulsar.Broker.send_command(state.broker_pid, redeliver_command) do
          :ok ->
            Logger.warning("Requested redelivery of #{length(nacked_list)} NACKed messages")
            %{state | nacked_messages: MapSet.new()}

          {:error, reason} ->
            Logger.error("Failed to send redelivery command: #{inspect(reason)}")
            state
        end
      else
        state
      end

    schedule_redelivery(state.redelivery_interval)
    {:noreply, new_state}
  end

  # Handle broker crashes - stop so supervisor can restart us with fresh lookup
  @impl true
  def handle_info(
        {:DOWN, monitor_ref, :process, broker_pid, reason},
        %__MODULE__{broker_monitor: monitor_ref, broker_pid: broker_pid} = state
      ) do
    Logger.info("Broker #{inspect(broker_pid)} crashed: #{inspect(reason)}, consumer will restart")

    {:stop, :broker_crashed, state}
  end

  # Handle other info messages by delegating to callback module
  @impl true
  def handle_info(message, state) do
    # Delegate to callback module if it implements handle_info
    if function_exported?(state.callback_module, :handle_info, 2) do
      case apply(state.callback_module, :handle_info, [message, state.callback_state]) do
        {:noreply, new_callback_state} ->
          new_state = %{state | callback_state: new_callback_state}
          {:noreply, new_state}

        {:noreply, new_callback_state, timeout_or_hibernate} ->
          new_state = %{state | callback_state: new_callback_state}
          {:noreply, new_state, timeout_or_hibernate}

        {:stop, reason, new_callback_state} ->
          new_state = %{state | callback_state: new_callback_state}
          {:stop, reason, new_state}
      end
    else
      # Default behavior - ignore unhandled info messages
      {:noreply, state}
    end
  end

  @impl true
  def terminate(_reason, nil) do
    :ok
  end

  def terminate(reason, state) do
    {:ok, _response} = close_consumer(state.broker_pid, state.consumer_id)

    # Call callback module's terminate function if it exists
    if function_exported?(state.callback_module, :terminate, 2) do
      try do
        apply(state.callback_module, :terminate, [reason, state.callback_state])
      rescue
        error ->
          Logger.warning("Error in callback terminate function: #{inspect(error)}")
      end
    end

    :ok
  end

  @impl true
  def handle_call({:send_flow, permits}, _from, state) do
    flow_command = %Binary.CommandFlow{
      consumer_id: state.consumer_id,
      messagePermits: permits
    }

    case Pulsar.Broker.send_command(state.broker_pid, flow_command) do
      :ok ->
        new_permits = state.flow_outstanding_permits + permits
        {:reply, :ok, %{state | flow_outstanding_permits: new_permits}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:ack, message_ids}, _from, state) when is_list(message_ids) do
    ack_command = %Binary.CommandAck{
      consumer_id: state.consumer_id,
      ack_type: :Individual,
      message_id: message_ids
    }

    case Pulsar.Broker.send_request(state.broker_pid, ack_command) do
      {:ok, _response} ->
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:nack, message_ids}, _from, state) when is_list(message_ids) do
    # Manual NACK follows the same pattern as auto-NACK:
    # - Add to nacked_messages if redelivery_interval is configured (for periodic redelivery)
    # - Note: DLQ logic cannot be applied here since we don't have redelivery_count or payload
    #   from the broker. The DLQ will only trigger on subsequent redeliveries when the message
    #   comes back through handle_message with an updated redelivery_count.

    new_nacked_messages =
      if state.redelivery_interval do
        # Track all messages for periodic redelivery
        Enum.reduce(message_ids, state.nacked_messages, fn message_id, acc ->
          MapSet.put(acc, message_id)
        end)
      else
        # No periodic redelivery configured, so we don't track nacked messages
        # Note: Without redelivery_interval, messages won't be automatically redelivered
        # and DLQ won't be triggered. Consider configuring :redelivery_interval and
        # :dead_letter_policy for production use.
        Logger.debug("NACKed #{length(message_ids)} message(s), but no redelivery_interval configured")
        state.nacked_messages
      end

    {:reply, :ok, %{state | nacked_messages: new_nacked_messages}}
  end

  def handle_call(request, from, state) do
    # Delegate to callback module if it implements handle_call
    if function_exported?(state.callback_module, :handle_call, 3) do
      case apply(state.callback_module, :handle_call, [request, from, state.callback_state]) do
        {:reply, reply, new_callback_state} ->
          new_state = %{state | callback_state: new_callback_state}
          {:reply, reply, new_state}

        {:reply, reply, new_callback_state, timeout_or_hibernate} ->
          new_state = %{state | callback_state: new_callback_state}
          {:reply, reply, new_state, timeout_or_hibernate}

        {:noreply, new_callback_state} ->
          new_state = %{state | callback_state: new_callback_state}
          {:noreply, new_state}

        {:noreply, new_callback_state, timeout_or_hibernate} ->
          new_state = %{state | callback_state: new_callback_state}
          {:noreply, new_state, timeout_or_hibernate}

        {:stop, reason, reply, new_callback_state} ->
          new_state = %{state | callback_state: new_callback_state}
          {:stop, reason, reply, new_state}

        {:stop, reason, new_callback_state} ->
          new_state = %{state | callback_state: new_callback_state}
          {:stop, reason, new_state}
      end
    else
      # Default behavior - return error for unhandled calls
      {:reply, {:error, :not_implemented}, state}
    end
  end

  @impl true
  def handle_cast(request, state) do
    # Delegate to callback module if it implements handle_cast
    if function_exported?(state.callback_module, :handle_cast, 2) do
      case apply(state.callback_module, :handle_cast, [request, state.callback_state]) do
        {:noreply, new_callback_state} ->
          new_state = %{state | callback_state: new_callback_state}
          {:noreply, new_state}

        {:noreply, new_callback_state, timeout_or_hibernate} ->
          new_state = %{state | callback_state: new_callback_state}
          {:noreply, new_state, timeout_or_hibernate}

        {:stop, reason, new_callback_state} ->
          new_state = %{state | callback_state: new_callback_state}
          {:stop, reason, new_state}
      end
    else
      # Default behavior - ignore unhandled casts
      {:noreply, state}
    end
  end

  ## Private Functions

  defp initial_position(:latest), do: :Latest
  defp initial_position(:earliest), do: :Earliest

  defp subscribe_to_topic(broker_pid, topic, subscription_name, subscription_type, consumer_id, opts) do
    request_id = System.unique_integer([:positive, :monotonic])
    initial_position = opts |> Keyword.get(:initial_position) |> initial_position()
    durable = Keyword.get(opts, :durable, true)
    force_create_topic = Keyword.get(opts, :force_create_topic, true)

    subscribe_command =
      %Binary.CommandSubscribe{
        topic: topic,
        subscription: subscription_name,
        subType: subscription_type,
        consumer_id: consumer_id,
        request_id: request_id,
        initialPosition: initial_position,
        durable: durable,
        force_topic_creation: force_create_topic
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
    # Initial flow starts from 0 outstanding permits
    send_flow_command(broker_pid, consumer_id, permits, 0)
  end

  defp check_and_refill_permits(state) do
    # Only auto-refill if flow_initial > 0 (automatic flow control enabled)
    if state.flow_initial == 0 do
      # Manual flow control - don't auto-refill
      state
    else
      refill_threshold = state.flow_threshold
      refill_amount = state.flow_refill
      current_permits = state.flow_outstanding_permits

      if current_permits <= refill_threshold do
        case send_flow_command(state.broker_pid, state.consumer_id, refill_amount, current_permits) do
          :ok ->
            new_permits = current_permits + refill_amount
            %{state | flow_outstanding_permits: new_permits}

          error ->
            Logger.error("Failed to send flow command: #{inspect(error)}")
            state
        end
      else
        state
      end
    end
  end

  defp send_flow_command(broker_pid, consumer_id, permits, outstanding_permits) do
    flow_command = %Binary.CommandFlow{
      consumer_id: consumer_id,
      messagePermits: permits
    }

    # Metadata for :start event
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

        stop_metadata =
          if result == :ok do
            %{success: true, permits_after: outstanding_permits + permits}
          else
            %{success: false, permits_after: outstanding_permits}
          end

        {result, Map.merge(start_metadata, stop_metadata)}
      end
    )
  end

  defp close_consumer(nil, _consumer_id) do
    {:ok, :skipped}
  end

  defp close_consumer(broker_pid, consumer_id) do
    close_consumer_command = %Binary.CommandCloseConsumer{
      consumer_id: consumer_id
    }

    Pulsar.Broker.send_request(broker_pid, close_consumer_command)
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

  defp parse_dead_letter_policy(nil), do: {nil, nil}

  defp parse_dead_letter_policy(policy) when is_list(policy) do
    max_redelivery = Keyword.get(policy, :max_redelivery)
    topic = Keyword.get(policy, :topic)

    # Validate max_redelivery
    validated_max_redelivery =
      case max_redelivery do
        n when is_integer(n) and n >= 1 -> n
        _ -> nil
      end

    {validated_max_redelivery, topic}
  end

  defp should_init_dead_letter_producer?(state) do
    state.max_redelivery != nil and state.max_redelivery >= 1
  end

  defp start_dead_letter_producer(state) do
    # Generate default dead letter topic if not provided
    dead_letter_topic =
      state.dead_letter_topic ||
        "#{state.topic}-#{state.subscription_name}-DLQ"

    # Start a producer for the dead letter topic
    Pulsar.Producer.start_link(dead_letter_topic, [])
  end

  defp should_send_to_dead_letter?(state, redelivery_count) do
    state.max_redelivery != nil and
      state.max_redelivery >= 1 and
      redelivery_count > state.max_redelivery and
      state.dead_letter_producer_pid != nil
  end

  defp send_to_dead_letter(state, payload, _message_id) do
    if state.dead_letter_producer_pid == nil do
      {:error, :no_dead_letter_producer}
    else
      case Pulsar.Producer.send_message(state.dead_letter_producer_pid, payload) do
        {:ok, _dlq_message_id} ->
          :ok

        {:error, _reason} = error ->
          error
      end
    end
  end

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

    <<payload::bytes-size(payload_size), rest::binary>> = data

    # Build individual message as {metadata, payload} tuple
    message = {single_metadata, payload}

    parse_batch_messages(rest, count - 1, [message | acc])
  rescue
    _ -> [{nil, payload}]
  end

  defp parse_batch_messages(payload, _, _) do
    [{nil, payload}]
  end
end
