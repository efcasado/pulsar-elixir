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

  defstruct [
    :topic,
    :subscription_name,
    :subscription_type,
    :consumer_id,
    :callback_module,
    :callback_state,
    :broker_pid,
    :broker_url,
    :broker_monitor
  ]

  @type t :: %__MODULE__{
          topic: String.t(),
          subscription_name: String.t(),
          subscription_type: String.t(),
          consumer_id: integer(),
          callback_module: module(),
          callback_state: term(),
          broker_pid: pid(),
          broker_url: String.t(),
          broker_monitor: reference()
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
    - Other GenServer options

  The consumer will automatically use any available broker for service discovery.
  """
  def start_link(topic, subscription_name, subscription_type, callback_module, opts \\ []) do
    {init_args, genserver_opts} = Keyword.pop(opts, :init_args, [])

    consumer_config = %{
      topic: topic,
      subscription_name: subscription_name,
      subscription_type: subscription_type,
      callback_module: callback_module,
      init_args: init_args
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
      init_args: init_args
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

    with {:ok, discovery_broker_pid} <- get_random_broker(),
         {:ok, lookup_result} <- Pulsar.Broker.lookup_topic(discovery_broker_pid, topic),
         broker_url <- get_broker_url(lookup_result),
         {:ok, broker_pid} <- Pulsar.start_broker(broker_url),
         :ok <- Pulsar.Broker.register_consumer(broker_pid, consumer_id, self()),
         {:ok, _response} <-
           subscribe_to_topic(
             broker_pid,
             topic,
             subscription_name,
             subscription_type,
             consumer_id
           ),
         :ok <- send_initial_flow(broker_pid, consumer_id) do
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
        broker_url: broker_url,
        broker_monitor: broker_monitor
      }

      {:ok, state}
    else
      {:error, :no_brokers_available} ->
        Logger.error("No brokers available for service discovery")
        {:error, :no_brokers_available}

      {:error, reason} ->
        Logger.error("Consumer initialization failed: #{inspect(reason)}")
        {:error, {:initialization_failed, reason}}
    end
  end

  @impl true
  def handle_info(
        {:broker_message, {%Binary.CommandMessage{message_id: message_id}, metadata, payload}},
        state
      ) do
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

        # Request more messages
        flow_command = %Binary.CommandFlow{
          consumer_id: state.consumer_id,
          messagePermits: 1
        }

        Pulsar.Broker.send_command(state.broker_pid, flow_command)

        # Update state and continue
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

  defp subscribe_to_topic(broker_pid, topic, subscription_name, subscription_type, consumer_id) do
    subscribe_command = %Binary.CommandSubscribe{
      topic: topic,
      subscription: subscription_name,
      subType: subscription_type,
      consumer_id: consumer_id
    }

    Pulsar.Broker.send_request(broker_pid, subscribe_command)
  end

  defp send_initial_flow(broker_pid, consumer_id) do
    flow_command = %Binary.CommandFlow{
      consumer_id: consumer_id,
      messagePermits: 100
    }

    Pulsar.Broker.send_command(broker_pid, flow_command)
  end

  defp get_random_broker do
    case Pulsar.list_brokers() do
      [] ->
        {:error, :no_brokers_available}

      brokers ->
        {_broker_key, broker_pid} = Enum.random(brokers)
        {:ok, broker_pid}
    end
  end
end
