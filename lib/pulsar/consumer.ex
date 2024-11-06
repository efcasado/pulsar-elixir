defmodule Pulsar.Consumer do
  @doc """
  Extends the generic connection behaviour to implement a Pulsar consumer.
  """
  use Pulsar.Connection

  # API

  def start_link(conn, callback, topic, type, name) do
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
    {:ok, %Pulsar.Protocol.Binary.Pulsar.Proto.CommandLookupTopicResponse{brokerServiceUrlTls: broker}} =
      Pulsar.ServiceDiscovery.lookup_topic(conn, topic)

    {:ok, socket_opts} = Pulsar.Connection.socket_opts(conn)
    {:ok, conn_timeout} = Pulsar.Connection.conn_timeout(conn)
    {:ok, auth} = Pulsar.Connection.auth(conn)
    
    actions = [{:next_event, :internal, {:subscribe, topic, type, name}}]
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

  def connected(:internal, {:subscribe, topic, type, name}, conn) do
    subscribe = %Binary.CommandSubscribe{
      topic: topic,
      subscription: name,
      subType: type,
      consumer_id: 1, #System.unique_integer([:positive, :monotonic]),
      request_id: System.unique_integer([:positive, :monotonic])
    }

    case Pulsar.Connection.send_command(conn, subscribe) do
      {:ok, _conn} ->
        :keep_state_and_data
      {{:error, _error}, conn} ->
        {:next_state, :disconnected, conn}
    end
  end

  def handle_call(from, {:ack, {ledger_id, entry_id}}, conn) do
    %Pulsar.Connection{requests: requests} = conn
    request_id = System.unique_integer([:positive, :monotonic])
    conn = %Pulsar.Connection{conn| requests: Map.put(requests, request_id, from)}

    command = %Binary.CommandAck{
      consumer_id: 1, # TO-DO: Read from state
      ack_type: :Individual, #TO-DO: Add support for cummulative acknowledgements
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
    Logger.debug("Ignoring #{inspect request} from #{inspect from}")
    {:ok, conn}
  end

  def handle_command(%Binary.CommandSuccess{}, conn) do
    Logger.debug("Successfully subscribed!")
    # subscription successfully created
    flow = %Binary.CommandFlow{
      consumer_id: 1, # TO-DO: Read from state
      messagePermits: 1
    }

    case Pulsar.Connection.send_command(conn, flow) do
      {:ok, _conn} ->
        :keep_state_and_data
      {{:error, _error}, conn} ->
        {:next_state, :disconnected, conn}
    end
  end
  def handle_command(command, conn) do
    # TO-DO: Handle commands
    Logger.warning("Unhandled command #{inspect command}")
    :keep_state_and_data
  end
end
