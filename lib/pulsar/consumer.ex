defmodule Pulsar.Consumer do
  @doc """
  Extends the generic connection behaviour to implement a Pulsar consumer.
  """
  use Pulsar.Connection

  # Built-in default callback for testing/debugging
  defmodule DefaultCallback do
    require Logger

    def handle_message(message) do
      Logger.info("[DefaultCallback] Message from: #{message.producer_name}")
      Logger.info("[DefaultCallback] Key: #{message.partition_key}")
      Logger.info("[DefaultCallback] Payload size: #{byte_size(message.payload)} bytes")
      Logger.info("[DefaultCallback] Publish time: #{message.publish_time}")

      # Pretty print first 100 bytes of payload if it's text-like
      preview = binary_preview(message.payload)

      if preview do
        Logger.info("[DefaultCallback] Payload preview: #{preview}")
      end

      # Always acknowledge
      :ok
    end

    defp binary_preview(payload) when byte_size(payload) == 0, do: "<empty>"

    defp binary_preview(payload) do
      preview_size = min(100, byte_size(payload))
      preview_bytes = binary_part(payload, 0, preview_size)

      case String.valid?(preview_bytes) do
        true ->
          truncated = if byte_size(payload) > 100, do: "...", else: ""
          "\"#{preview_bytes}#{truncated}\""

        false ->
          "<binary data #{byte_size(payload)} bytes>"
      end
    end
  end

  # API

  def start_link(conn, topic, type, name, callback \\ nil) do
    # Use default callback if none provided
    callback = callback || DefaultCallback

    # TODO: support other subscription options
    args = [
      conn,
      callback,
      topic,
      type,
      name
    ]

    # TO-DO: handle redirects
    # TO-DO: handle errors
    {:ok,
     %Pulsar.Protocol.Binary.Pulsar.Proto.CommandLookupTopicResponse{
       brokerServiceUrl: service_url,
       brokerServiceUrlTls: service_url_tls
     }} = Pulsar.ServiceDiscovery.lookup_topic(conn, topic)

    broker = service_url_tls || service_url

    {:ok, socket_opts} = Pulsar.Connection.socket_opts(conn)
    {:ok, conn_timeout} = Pulsar.Connection.conn_timeout(conn)
    {:ok, auth} = Pulsar.Connection.auth(conn)

    actions = [{:next_event, :internal, {:subscribe, topic, type, name, callback}}]

    Pulsar.Connection.start_link(
      __MODULE__,
      broker,
      socket_opts: socket_opts,
      conn_timeout: conn_timeout,
      auth: auth,
      actions: actions
    )
  end

  def ack(conn, message_id, timeout \\ 5_000) do
    :gen_statem.call(conn, {:ack, message_id}, timeout)
  end

  # connection callback functions

  def connected(:internal, {:subscribe, topic, type, name, callback}, conn) do
    subscribe = %Binary.CommandSubscribe{
      topic: topic,
      subscription: name,
      subType: type,
      # System.unique_integer([:positive, :monotonic]),
      consumer_id: 1,
      request_id: System.unique_integer([:positive, :monotonic])
    }

    # Store the callback in the connection state
    conn = %Pulsar.Connection{conn | actions: [callback: callback]}

    case Pulsar.Connection.send_command(conn, subscribe) do
      {:ok, _conn} ->
        {:keep_state, conn}

      {{:error, _error}, conn} ->
        {:next_state, :disconnected, conn}
    end
  end

  def handle_call(from, {:ack, {ledger_id, entry_id}}, conn) do
    %Pulsar.Connection{requests: requests} = conn
    request_id = System.unique_integer([:positive, :monotonic])
    conn = %Pulsar.Connection{conn | requests: Map.put(requests, request_id, from)}

    command = %Binary.CommandAck{
      # TO-DO: Read from state
      consumer_id: 1,
      # TO-DO: Add support for cummulative acknowledgements
      ack_type: :Individual,
      request_id: request_id,
      message_id: [
        %Binary.MessageIdData{
          ledgerId: ledger_id,
          entryId: entry_id
        }
      ]
    }

    Pulsar.Connection.send_command(conn, command)
  end

  # TO-DO: Implement as default and overridable
  def handle_call(from, request, conn) do
    Logger.debug("Ignoring #{inspect(request)} from #{inspect(from)}")
    {:ok, conn}
  end

  def handle_command(%Binary.CommandSuccess{}, conn) do
    Logger.debug("Successfully subscribed!")
    # subscription successfully created
    flow = %Binary.CommandFlow{
      # TO-DO: Read from state
      consumer_id: 1,
      # Request more messages initially
      messagePermits: 100
    }

    Logger.info("Sending initial flow command: #{flow.messagePermits} permits")

    case Pulsar.Connection.send_command(conn, flow) do
      {:ok, _conn} ->
        Logger.info("Initial flow command sent successfully")
        :keep_state_and_data

      {{:error, error}, conn} ->
        Logger.error("Failed to send initial flow command: #{inspect(error)}")
        {:next_state, :disconnected, conn}
    end
  end

  def handle_command({%Binary.CommandMessage{message_id: message_id}, metadata, payload}, conn) do
    %Pulsar.Connection{actions: actions} = conn
    callback = Keyword.get(actions, :callback)

    Logger.info("Received message from producer: #{metadata.producer_name}")
    Logger.info("Message key: #{metadata.partition_key}")
    Logger.info("Payload size: #{byte_size(payload)} bytes")

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
      if callback do
        try do
          apply(callback, :handle_message, [message])
        rescue
          error ->
            Logger.error("Error in callback: #{inspect(error)}")
            {:error, error}
        end
      else
        Logger.warning("No callback configured, auto-acking message")
        :ok
      end

    # Handle acknowledgment based on callback result
    case result do
      :ok ->
        # Acknowledge the message
        ack_command = %Binary.CommandAck{
          consumer_id: 1,
          ack_type: :Individual,
          message_id: [message_id]
        }

        Pulsar.Connection.send_command(conn, ack_command)

      {:error, reason} ->
        Logger.warning("Message processing failed: #{inspect(reason)}, not acknowledging")

      _ ->
        Logger.warning("Unexpected callback result: #{inspect(result)}, not acknowledging")
    end

    # Always request more messages
    flow = %Binary.CommandFlow{
      consumer_id: 1,
      messagePermits: 1
    }

    Logger.info("Requesting more messages: #{flow.messagePermits} permits")

    case Pulsar.Connection.send_command(conn, flow) do
      {:ok, _conn} ->
        Logger.info("Flow command sent successfully")

      {{:error, error}, _conn} ->
        Logger.error("Failed to send flow command: #{inspect(error)}")
    end

    :keep_state_and_data
  end

  def handle_command(command, conn) do
    # TO-DO: Handle commands
    Logger.warning("Unhandled command #{inspect(command)}")
    :keep_state_and_data
  end
end
