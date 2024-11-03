defmodule Pulsar.Consumer do
  @doc """
  Extends the generic connection behaviour to implement a Pulsar consumer.
  """
  use Pulsar.Connection

  # API

  def start_link(conn, callback, topic, type, name) do
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

  # TO-DO: Implement as default and overridable
  def handle_call(from, request, conn) do
    Logger.debug("Ignoring #{inspect request} from #{inspect from}")
    {:ok, conn}
  end

  def handle_command(%Binary.CommandSuccess{}, conn) do
    Logger.debug("Successfully subscribed!")
    # subscription successfully created
    flow = %Binary.CommandFlow{
      consumer_id: 1,
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
