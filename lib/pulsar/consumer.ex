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
  alias Pulsar.Utils

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
    :flow_outstanding_permits
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
          flow_outstanding_permits: non_neg_integer()
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
      initial_position: initial_position
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
      initial_position: initial_position
    } = consumer_config

    consumer_id = System.unique_integer([:positive, :monotonic])
    Logger.info("Starting consumer for topic #{topic}")

    # Initialize callback module state
    callback_state =
      case apply(callback_module, :init, [init_args]) do
        {:ok, state} ->
          state

        {:error, reason} ->
          Logger.error("Callback initialization failed: #{inspect(reason)}")
          raise "Callback initialization failed: #{inspect(reason)}"
      end

    with {:ok, broker_pid} <- lookup_topic(topic),
         :ok <- Pulsar.Broker.register_consumer(broker_pid, consumer_id, self()),
         {:ok, _response} <-
           subscribe_to_topic(
             broker_pid,
             topic,
             subscription_name,
             subscription_type,
             consumer_id,
             initial_position: initial_position
           ),
         :ok <- send_initial_flow(broker_pid, consumer_id, initial_permits) do
      Logger.info("Successfully subscribed to #{topic}")

      # Monitor the broker process to detect crashes
      broker_monitor = Process.monitor(broker_pid)

      state = %__MODULE__{
        topic: topic,
        subscription_name: subscription_name,
        subscription_type: subscription_type,
        consumer_id: consumer_id,
        callback_module: callback_module,
        callback_state: callback_state,
        broker_pid: broker_pid,
        broker_monitor: broker_monitor,
        flow_initial: initial_permits,
        flow_threshold: refill_threshold,
        flow_refill: refill_amount,
        flow_outstanding_permits: initial_permits
      }

      {:ok, state}
    else
      {:error, reason} ->
        Logger.error("Consumer initialization failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @impl true
  def handle_info(
        {:broker_message, {%Binary.CommandMessage{message_id: message_id}, metadata, payload}},
        state
      ) do
    state = decrement_permits(state)

    # Build message structure for callback
    message = %{
      id: {message_id.ledgerId, message_id.entryId},
      metadata: metadata,
      payload: payload,
      partition_key: metadata.partition_key,
      producer_name: metadata.producer_name,
      publish_time: metadata.publish_time
    }

    # Call the user-provided callback with current state
    result =
      try do
        apply(state.callback_module, :handle_message, [message, state.callback_state])
      rescue
        error ->
          Logger.error("Error in callback: #{inspect(error)}")
          {:error, error, state.callback_state}
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

  @impl true
  def terminate(reason, state) do
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

  defp get_broker_url(%{brokerServiceUrl: service_url, brokerServiceUrlTls: service_url_tls}) do
    service_url_tls || service_url
  end

  defp initial_position(:latest), do: :Latest
  defp initial_position(:earliest), do: :Earliest

  defp lookup_topic(topic) do
    broker = Utils.broker()

    lookup_topic(broker, topic, false)
  end

  defp lookup_topic(broker, topic, authoritative) do
    case Pulsar.Broker.lookup_topic(broker, topic, authoritative) do
      {:ok, %{response: :Connect} = response} ->
        response
        |> get_broker_url
        |> Pulsar.start_broker()

      {:ok, %{response: :Redirect, authoritative: authoritative} = response} ->
        {:ok, broker} =
          response
          |> get_broker_url
          |> Pulsar.start_broker()

        lookup_topic(broker, topic, authoritative)

      {:ok, %{response: :Failed, error: error}} ->
        Logger.error("Topic lookup failed: #{inspect(error)}")
        {:error, {:lookup_failed, error}}

      {:error, reason} ->
        Logger.error("Topic lookup error: #{inspect(reason)}")
        {:error, reason}
    end
  end

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

    subscribe_command = %Binary.CommandSubscribe{
      topic: topic,
      subscription: subscription_name,
      subType: subscription_type,
      consumer_id: consumer_id,
      request_id: request_id,
      initialPosition: initial_position
    }

    Pulsar.Broker.send_request(broker_pid, subscribe_command)
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
end
