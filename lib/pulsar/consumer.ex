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
  require Logger

  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.ServiceDiscovery

  defstruct [
    :topic,
    :subscription_name,
    :subscription_type,
    :consumer_id,
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
    :start_timestamp
  ]

  @type t :: %__MODULE__{
          topic: String.t(),
          subscription_name: String.t(),
          subscription_type: String.t(),
          consumer_id: integer(),
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
          start_timestamp: non_neg_integer()
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
    - `:flow_initial` - Initial flow permits (default: 100)
    - `:flow_threshold` - Flow permits threshold (default: 50)
    - `:flow_refill` - Flow permits refill (default: 50)
    - `:initial_position` - Initial position for subscription (`:latest` or `:earliest`, defaults to `:latest`)
    - Other GenServer options

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
      start_timestamp: start_timestamp
    }

    GenServer.start_link(__MODULE__, consumer_config, genserver_opts)
  end

  @doc """
  Gracefully stops a consumer process.
  """
  @spec stop(GenServer.server(), term(), timeout()) :: :ok
  def stop(consumer, reason \\ :normal, timeout \\ :infinity) do
    GenServer.stop(consumer, reason, timeout)
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
      start_timestamp: start_timestamp
    } = consumer_config

    state = %__MODULE__{
      consumer_id: System.unique_integer([:positive, :monotonic]),
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
      start_timestamp: start_timestamp
    }

    Logger.info("Starting consumer for topic #{state.topic}")
    {:ok, state, {:continue, {:init_callback, init_args}}}
  end

  @impl true
  def handle_continue({:init_callback, init_args}, state) do
    case apply(state.callback_module, :init, [init_args]) do
      {:ok, callback_state} ->
        {:noreply, %__MODULE__{state | callback_state: callback_state}, {:continue, :subscribe}}

      {:error, reason} ->
        {:stop, reason, nil}
    end
  end

  def handle_continue(:subscribe, state) do
    with {:ok, broker_pid} <- ServiceDiscovery.lookup_topic(state.topic),
         :ok <- Pulsar.Broker.register_consumer(broker_pid, state.consumer_id, self()),
         {:ok, _response} <-
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
      {:noreply, %__MODULE__{state | consumer_id: state.consumer_id, broker_pid: broker_pid},
       {:continue, :seek_subscription}}
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
    case send_initial_flow(state.broker_pid, state.consumer_id, state.flow_initial) do
      :ok ->
        broker_monitor = Process.monitor(state.broker_pid)

        {:noreply,
         %__MODULE__{
           state
           | broker_monitor: broker_monitor,
             flow_outstanding_permits: state.flow_initial
         }}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def message_id(%Binary.CommandMessage{} = message) do
    message.message_id
  end

  @impl true
  def handle_info(
        {:broker_message, %Binary.CommandCloseConsumer{}},
        state
      ) do
    {:stop, :broker_close_requested, state}
  end

  def handle_info(
        {:broker_message, {command, metadata, payload, broker_metadata}},
        state
      ) do
    message_id = message_id(command)
    state = decrement_permits(state)

    # Payload is now always a list of {metadata, binary} tuples from protocol normalization
    messages = payload

    # Process messages with early termination on error or stop
    {final_callback_state, should_ack, should_stop} =
      messages
      |> Enum.reduce_while({state.callback_state, true, false}, fn {msg_metadata, msg_payload},
                                                                   {callback_state, ack_acc,
                                                                    _stop_acc} ->
        # Create args tuple for each normalized message
        msg_args = {command, metadata, {msg_metadata, msg_payload}, broker_metadata}

        result = apply(state.callback_module, :handle_message, [msg_args, callback_state])

        case result do
          {:ok, new_callback_state} ->
            {:cont, {new_callback_state, ack_acc, false}}

          {:error, reason, new_callback_state} ->
            Logger.warning("Message processing failed: #{inspect(reason)}")
            # Continue processing but don't ack the batch
            {:cont, {new_callback_state, false, false}}

          {:stop, new_callback_state} ->
            # Stop processing the batch
            {:halt, {new_callback_state, ack_acc, true}}

          unexpected_result ->
            Logger.warning("Unexpected callback result: #{inspect(unexpected_result)}")
            {:cont, {callback_state, false, false}}
        end
      end)

    # Create result tuple for the rest of the function
    result =
      if should_stop do
        {:stop, final_callback_state}
      else
        if should_ack do
          {:ok, final_callback_state}
        else
          {:error, :processing_failed, final_callback_state}
        end
      end

    # Handle acknowledgment and state updates based on callback result
    case result do
      {:ok, new_callback_state} ->
        # Auto-acknowledge successful messages
        ack_command = %Binary.CommandAck{
          consumer_id: state.consumer_id,
          ack_type: :Individual,
          message_id: [message_id]
        }

        Pulsar.Broker.send_command(state.broker_pid, ack_command)

        state = check_and_refill_permits(state)

        new_state = %{state | callback_state: new_callback_state}
        {:noreply, new_state}

      {:error, reason, new_callback_state} ->
        Logger.warning("Message processing failed: #{inspect(reason)}, not acknowledging")
        # Update state but don't acknowledge
        new_state = %{state | callback_state: new_callback_state}
        {:noreply, new_state}

      {:stop, new_callback_state} ->
        # Acknowledge the message before stopping
        ack_command = %Binary.CommandAck{
          consumer_id: state.consumer_id,
          ack_type: :Individual,
          message_id: [message_id]
        }

        Pulsar.Broker.send_command(state.broker_pid, ack_command)

        # Update state and stop
        new_state = %{state | callback_state: new_callback_state}
        {:stop, :normal, new_state}

      unexpected_result ->
        Logger.warning(
          "Unexpected callback result: #{inspect(unexpected_result)}, not acknowledging"
        )

        {:noreply, state}
    end
  end

  # def handle_info(
  #       {:broker_message,
  #        {%Binary.CommandMessage{message_id: message_id}, metadata, payload, broker_metadata}},
  #       state
  #     ) do
  #   state = decrement_permits(state)

  #   # Handle single message vs batch
  #   case payload do
  #     # Batch format (list of parsed messages)
  #     messages when is_list(messages) ->
  #       handle_batch_messages(messages, metadata, message_id, broker_metadata, state)

  #     # Single message (binary payload)
  #     single_payload when is_binary(single_payload) ->
  #       message = %{
  #         id: {message_id.ledgerId, message_id.entryId},
  #         metadata: metadata,
  #         payload: single_payload,
  #         partition_key: metadata.partition_key,
  #         producer_name: metadata.producer_name,
  #         publish_time: metadata.publish_time,
  #         broker_metadata: broker_metadata
  #       }

  #       handle_single_message(message, message_id, state)
  #   end
  # end

  # Handle broker crashes - stop so supervisor can restart us with fresh lookup
  @impl true
  def handle_info(
        {:DOWN, monitor_ref, :process, broker_pid, reason},
        %__MODULE__{broker_monitor: monitor_ref, broker_pid: broker_pid} = state
      ) do
    Logger.info(
      "Broker #{inspect(broker_pid)} crashed: #{inspect(reason)}, consumer will restart"
    )

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

  # defp handle_single_message(message, message_id, state) do
  #   # Call the user-provided callback with current state
  #   result =
  #     try do
  #       apply(state.callback_module, :handle_message, [message, state.callback_state])
  #     rescue
  #       error ->
  #         Logger.error("Error in callback: #{inspect(error)}")
  #         {:error, error, state.callback_state}
  #     end

  #   # Handle acknowledgment and state updates based on callback result
  #   case result do
  #     {:ok, new_callback_state} ->
  #       # Auto-acknowledge successful messages
  #       ack_command = %Binary.CommandAck{
  #         consumer_id: state.consumer_id,
  #         ack_type: :Individual,
  #         message_id: [message_id]
  #       }

  #       Pulsar.Broker.send_command(state.broker_pid, ack_command)

  #       state = check_and_refill_permits(state)

  #       new_state = %{state | callback_state: new_callback_state}
  #       {:noreply, new_state}

  #     {:error, reason, new_callback_state} ->
  #       Logger.warning("Message processing failed: #{inspect(reason)}, not acknowledging")
  #       # Update state but don't acknowledge
  #       new_state = %{state | callback_state: new_callback_state}
  #       {:noreply, new_state}

  #     {:stop, new_callback_state} ->
  #       # Acknowledge the message before stopping
  #       ack_command = %Binary.CommandAck{
  #         consumer_id: state.consumer_id,
  #         ack_type: :Individual,
  #         message_id: [message_id]
  #       }

  #       Pulsar.Broker.send_command(state.broker_pid, ack_command)

  #       # Update state and stop
  #       new_state = %{state | callback_state: new_callback_state}
  #       {:stop, :normal, new_state}

  #     unexpected_result ->
  #       Logger.warning(
  #         "Unexpected callback result: #{inspect(unexpected_result)}, not acknowledging"
  #       )

  #       {:noreply, state}
  #   end
  # end

  # defp handle_batch_messages(batch_messages, metadata, message_id, broker_metadata, state) do
  #   # For batch messages, we'll call the callback for each message in the batch
  #   # but acknowledge the whole batch as one unit

  #   {final_callback_state, should_ack, should_stop} =
  #     Enum.reduce_while(batch_messages, {state.callback_state, true, false}, fn batch_msg,
  #                                                                               {callback_state,
  #                                                                                ack_acc,
  #                                                                                stop_acc} ->
  #       # Build message structure for each message in batch
  #       message = %{
  #         id: {message_id.ledgerId, message_id.entryId},
  #         metadata: metadata,
  #         payload: batch_msg.payload,
  #         partition_key: batch_msg.metadata.partition_key || metadata.partition_key,
  #         producer_name: metadata.producer_name,
  #         publish_time: metadata.publish_time,
  #         broker_metadata: broker_metadata,
  #         single_message_metadata: batch_msg.metadata
  #       }

  #       # Call callback for this message
  #       result =
  #         try do
  #           apply(state.callback_module, :handle_message, [message, callback_state])
  #         rescue
  #           error ->
  #             Logger.error("Error in batch callback: #{inspect(error)}")
  #             {:error, error, callback_state}
  #         end

  #       case result do
  #         {:ok, new_callback_state} ->
  #           {:cont, {new_callback_state, ack_acc, stop_acc}}

  #         {:error, reason, new_callback_state} ->
  #           Logger.warning("Batch message processing failed: #{inspect(reason)}")
  #           # Continue processing but don't ack the batch
  #           {:cont, {new_callback_state, false, stop_acc}}

  #         {:stop, new_callback_state} ->
  #           # Stop processing the batch
  #           {:halt, {new_callback_state, ack_acc, true}}

  #         unexpected_result ->
  #           Logger.warning("Unexpected batch callback result: #{inspect(unexpected_result)}")
  #           {:cont, {callback_state, false, stop_acc}}
  #       end
  #     end)

  #   # Handle acknowledgment for the whole batch
  #   if should_ack do
  #     ack_command = %Binary.CommandAck{
  #       consumer_id: state.consumer_id,
  #       ack_type: :Individual,
  #       message_id: [message_id]
  #     }

  #     Pulsar.Broker.send_command(state.broker_pid, ack_command)
  #   end

  #   state = check_and_refill_permits(state)
  #   new_state = %{state | callback_state: final_callback_state}

  #   if should_stop do
  #     {:stop, :normal, new_state}
  #   else
  #     {:noreply, new_state}
  #   end
  # end

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

  defp subscribe_to_topic(
         broker_pid,
         topic,
         subscription_name,
         subscription_type,
         consumer_id,
         opts
       ) do
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

  defp decrement_permits(state) do
    new_permits = max(state.flow_outstanding_permits - 1, 0)
    %{state | flow_outstanding_permits: new_permits}
  end

  defp send_initial_flow(broker_pid, consumer_id, permits) do
    # Initial flow starts from 0 outstanding permits
    send_flow_command(broker_pid, consumer_id, permits, 0)
  end

  defp check_and_refill_permits(state) do
    refill_threshold = state.flow_threshold
    refill_amount = state.flow_refill
    current_permits = state.flow_outstanding_permits

    if current_permits <= refill_threshold do
      case send_flow_command(state.broker_pid, state.consumer_id, refill_amount, current_permits) do
        :ok ->
          %{state | flow_outstanding_permits: current_permits + refill_amount}

        error ->
          Logger.error("Failed to send flow command: #{inspect(error)}")
          state
      end
    else
      state
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
end
