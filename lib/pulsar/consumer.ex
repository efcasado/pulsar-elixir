defmodule Pulsar.Consumer do
  @moduledoc """
  Pulsar consumer process that communicates with broker processes.

  This consumer uses service discovery to find the appropriate broker
  for the topic and then communicates with that broker process.

  ## Callback Module

  The consumer requires a callback module that implements the `handle_message/1` function.
  The callback should return `:ok` for successful processing (which triggers automatic
  acknowledgment) or `{:error, reason}` to indicate processing failure.

  ### Example Callback Module

      defmodule MyApp.MessageHandler do
        require Logger
        
        alias Pulsar.Protocol.Binary.Pulsar.Proto

        @type message :: %{
          id: {integer(), integer()},
          metadata: Proto.MessageMetadata.t(),
          payload: binary(),
          partition_key: String.t() | nil,
          producer_name: String.t(),
          publish_time: integer()
        }

        @spec handle_message(message()) :: :ok | {:error, term()}
        def handle_message(message) do
          Logger.info("Received message from: \#{message.producer_name}")
          Logger.info("Payload: \#{message.payload}")
          
          # Process your message here
          # Return :ok to acknowledge, {:error, reason} to not acknowledge
          :ok
        end
      end

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
    :broker_pid,
    :broker_url,
    :state
  ]

  @type t :: %__MODULE__{
          topic: String.t(),
          subscription_name: String.t(),
          subscription_type: String.t(),
          consumer_id: integer(),
          callback_module: module(),
          broker_pid: pid() | nil,
          broker_url: String.t() | nil,
          state: :discovering | :connecting | :subscribed
        }

  ## Public API

  @doc """
  Starts a consumer process with explicit parameters.

  ## Parameters

  - `topic` - The topic to subscribe to
  - `subscription_name` - Name of the subscription
  - `subscription_type` - Type of subscription (e.g., :Exclusive, :Shared)
  - `callback_module` - Module that implements handle_message/1
  - `opts` - Additional GenServer options

  The consumer will automatically use any available broker for service discovery.
  """
  def start_link(topic, subscription_name, subscription_type, callback_module, opts \\ []) do
    consumer_config = %{
      topic: topic,
      subscription_name: subscription_name,
      subscription_type: subscription_type,
      callback_module: callback_module
    }

    GenServer.start_link(__MODULE__, consumer_config, opts)
  end

  @doc """
  Acknowledges a message.
  """
  def ack(consumer, message_id, timeout \\ 5_000) do
    GenServer.call(consumer, {:ack, message_id}, timeout)
  end

  @doc """
  Gets the current state of the consumer.
  """
  def get_state(consumer) do
    GenServer.call(consumer, :get_state)
  end

  ## GenServer Callbacks

  @impl true
  def init(consumer_config) do
    %{
      topic: topic,
      subscription_name: subscription_name,
      subscription_type: subscription_type,
      callback_module: callback_module
    } = consumer_config

    consumer_id = System.unique_integer([:positive, :monotonic])

    state = %__MODULE__{
      topic: topic,
      subscription_name: subscription_name,
      subscription_type: subscription_type,
      consumer_id: consumer_id,
      callback_module: callback_module,
      broker_pid: nil,
      broker_url: nil,
      state: :discovering
    }

    Logger.info("Starting consumer for topic #{topic}")

    # Start service discovery
    send(self(), :discover_broker)

    {:ok, state}
  end

  @impl true
  def handle_call({:ack, message_id}, _from, state) do
    case state.broker_pid do
      nil ->
        {:reply, {:error, :no_broker}, state}

      broker_pid ->
        ack_command = %Binary.CommandAck{
          consumer_id: state.consumer_id,
          ack_type: :Individual,
          message_id: [message_id]
        }

        case Pulsar.Broker.send_command(broker_pid, ack_command) do
          :ok ->
            {:reply, :ok, state}

          error ->
            {:reply, error, state}
        end
    end
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    {:reply, state.state, state}
  end

  @impl true
  def handle_info(:discover_broker, state) do
    # Use any available broker for service discovery
    case get_random_broker() do
      {:ok, broker_pid} ->
        # Perform service discovery
        case Pulsar.Broker.lookup_topic(broker_pid, state.topic) do
          {:ok, %{brokerServiceUrl: service_url, brokerServiceUrlTls: service_url_tls}} ->
            broker_url = service_url_tls || service_url
            send(self(), {:connect_to_broker, broker_url})
            {:noreply, %{state | state: :connecting}}

          {:error, reason} ->
            Logger.error("Topic lookup failed: #{inspect(reason)}")
            schedule_retry(:discover_broker, 5000)
            {:noreply, state}
        end

      {:error, :no_brokers_available} ->
        Logger.error("No brokers available for service discovery")
        schedule_retry(:discover_broker, 5000)
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:connect_to_broker, broker_url}, state) do
    case Pulsar.start_broker(broker_url, get_broker_opts()) do
      {:ok, broker_pid} ->
        # Register with the broker
        :ok = Pulsar.Broker.register_consumer(broker_pid, state.consumer_id, self())

        # Subscribe to the topic
        subscribe_command = %Binary.CommandSubscribe{
          topic: state.topic,
          subscription: state.subscription_name,
          subType: state.subscription_type,
          consumer_id: state.consumer_id
        }

        case Pulsar.Broker.send_request(broker_pid, subscribe_command) do
          {:ok, _response} ->
            Logger.info("Successfully subscribed to #{state.topic}")

            # Send initial flow command
            flow_command = %Binary.CommandFlow{
              consumer_id: state.consumer_id,
              messagePermits: 100
            }

            Pulsar.Broker.send_command(broker_pid, flow_command)

            new_state = %{
              state
              | broker_pid: broker_pid,
                broker_url: broker_url,
                state: :subscribed
            }

            {:noreply, new_state}

          {:error, reason} ->
            Logger.error("Subscription failed: #{inspect(reason)}")
            schedule_retry({:connect_to_broker, broker_url}, 5000)
            {:noreply, state}
        end

      {:error, reason} ->
        Logger.error("Failed to connect to broker #{broker_url}: #{inspect(reason)}")
        schedule_retry({:connect_to_broker, broker_url}, 5000)
        {:noreply, state}
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

    # Call the user-provided callback
    result =
      try do
        apply(state.callback_module, :handle_message, [message])
      rescue
        error ->
          Logger.error("Error in callback: #{inspect(error)}")
          {:error, error}
      end

    # Handle acknowledgment based on callback result
    case result do
      :ok ->
        # Auto-acknowledge successful messages  
        ack_command = %Binary.CommandAck{
          consumer_id: state.consumer_id,
          ack_type: :Individual,
          message_id: [message_id]
        }

        if state.broker_pid do
          Pulsar.Broker.send_command(state.broker_pid, ack_command)
        end

        # Request more messages
        flow_command = %Binary.CommandFlow{
          consumer_id: state.consumer_id,
          messagePermits: 1
        }

        if state.broker_pid do
          Pulsar.Broker.send_command(state.broker_pid, flow_command)
        end

      {:error, reason} ->
        Logger.warning("Message processing failed: #{inspect(reason)}, not acknowledging")

      _ ->
        Logger.warning("Unexpected callback result: #{inspect(result)}, not acknowledging")
    end

    {:noreply, state}
  end

  ## Private Functions

  defp get_broker_opts do
    [
      socket_opts: [],
      conn_timeout: 5_000,
      auth: []
    ]
  end

  defp schedule_retry(message, delay) do
    Process.send_after(self(), message, delay)
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
