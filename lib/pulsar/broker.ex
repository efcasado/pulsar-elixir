defmodule Pulsar.Broker do
  @moduledoc false

  # One connection to one broker, as a gen_statem with :disconnected and :connected states:
  # framing, handshake and auth, request/response correlation, and reconnection with backoff.
  # Consumers and producers registered here are monitored so they are cleaned up when they exit.

  @behaviour :gen_statem

  alias Pulsar.Backoff
  alias Pulsar.Broker.Options
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

  require Logger

  # Main connection state (unified from Connection)
  defstruct [
    :name,
    :connection_slot,
    :host,
    :port,
    :socket_module,
    :socket,
    {:prev_backoff, 0},
    :socket_opts,
    :conn_timeout,
    :auth,
    :max_frame_size,
    :max_message_size,
    :ping_interval,
    :cleanup_interval,
    :request_timeout,
    {:buffer, <<>>},
    {:requests, %{}},
    {:actions, []},
    # Broker-specific state
    {:consumers, %{}},
    {:producers, %{}}
  ]

  @type t :: %__MODULE__{
          name: String.t(),
          connection_slot: non_neg_integer(),
          host: String.t(),
          port: integer(),
          socket_module: :gen_tcp | :ssl,
          socket: :gen_tcp.socket() | :ssl.sslsocket() | nil,
          prev_backoff: integer(),
          socket_opts: list(),
          conn_timeout: timeout(),
          auth: list(),
          max_frame_size: pos_integer(),
          max_message_size: pos_integer() | nil,
          ping_interval: timeout(),
          cleanup_interval: timeout(),
          request_timeout: timeout(),
          buffer: Pulsar.Protocol.buffer(),
          requests: %{integer() => {GenServer.from(), integer()}},
          actions: list(),
          consumers: %{integer() => {pid(), reference()}},
          producers: %{integer() => {pid(), reference()}}
        }

  # Scoped to a single TCP connection: a frame cut in half by a dropped socket
  # must not be prepended to the first bytes read from the next one.
  @connection_fields %{
    socket: nil,
    buffer: <<>>,
    requests: %{},
    consumers: %{},
    producers: %{},
    max_message_size: nil
  }

  @client_version "Pulsar Elixir Client"

  @ssl_only_opts [:verify, :cacerts, :cacertfile, :certfile, :keyfile]

  ## Public API

  @doc """
  Starts a broker connection process.

  The target Pulsar broker is expected to be specified in the form of: `<scheme>://<host>[:<port>]`,
  where `scheme` can be either `pulsar` or `pulsar+ssl` and `port` is an optional field that
  defaults to `6650` and `6651`, respectively.

  ## Options

  #{Options.docs()}
  """
  @spec start_link(String.t(), keyword()) :: {:ok, pid()} | :ignore | {:error, term()}
  def start_link(broker_url, opts) do
    :gen_statem.start_link(__MODULE__, Keyword.put(opts, :url, broker_url), [])
  end

  @doc """
  Registers a consumer with this broker and monitors the process.
  """
  @spec register_consumer(GenServer.server(), integer(), pid()) :: :ok
  def register_consumer(broker, consumer_id, consumer_pid) do
    :gen_statem.call(broker, {:register_consumer, consumer_id, consumer_pid})
  end

  @doc """
  Registers a producer with this broker and monitors the process.
  """
  @spec register_producer(GenServer.server(), integer(), pid()) :: :ok
  def register_producer(broker, producer_id, producer_pid) do
    :gen_statem.call(broker, {:register_producer, producer_id, producer_pid})
  end

  @doc """
  Sends a command to the broker without expecting a response.
  """
  @spec send_command(GenServer.server(), struct()) :: :ok | {:error, term()}
  def send_command(broker, command) do
    :gen_statem.cast(broker, {:send_command, command})
  end

  @doc """
  Sends a command to the broker and expects a response.
  """
  @spec send_request(GenServer.server(), struct(), timeout()) :: {:ok, term()} | {:error, term()}
  def send_request(broker, command, timeout \\ 5000) do
    Logger.debug("Sending request #{inspect(command)}")
    :gen_statem.call(broker, {:send_request, command}, timeout)
  end

  @doc """
  Publishes a message to the broker.
  It expects the message to be already encoded in the Pulsar binary protocol format.
  """
  @spec publish_message(GenServer.server(), binary()) :: :ok | {:error, term()}
  def publish_message(broker, encoded_message) do
    :gen_statem.call(broker, {:publish_message, encoded_message})
  end

  @doc """
  Service discovery: lookup topic.
  """
  @spec lookup_topic(GenServer.server(), String.t(), boolean(), timeout()) ::
          {:ok, map()} | {:error, term()}
  def lookup_topic(broker, topic, authoritative \\ false, timeout \\ 5_000) do
    :gen_statem.call(broker, {:lookup_topic, topic, authoritative}, timeout)
  end

  @doc """
  Service discovery: get partitioned topic metadata.
  """
  @spec partitioned_topic_metadata(GenServer.server(), String.t(), timeout()) ::
          {:ok, map()} | {:error, term()}
  def partitioned_topic_metadata(broker, topic, timeout \\ 5_000) do
    :gen_statem.call(broker, {:partitioned_topic_metadata, topic}, timeout)
  end

  @doc """
  Gets the consumers registered with this broker connection, keyed by consumer id.
  """
  @spec get_consumers(GenServer.server()) :: %{integer() => pid()}
  def get_consumers(broker), do: :gen_statem.call(broker, :get_consumers)

  @doc """
  Gets the producers registered with this broker connection, keyed by producer id.
  """
  @spec get_producers(GenServer.server()) :: %{integer() => pid()}
  def get_producers(broker), do: :gen_statem.call(broker, :get_producers)

  @doc """
  Gets the largest message this broker accepts, as advertised in `CommandConnected`.

  `nil` until the handshake completes, and again after a reconnection until the new
  connection has handshaken.
  """
  @spec get_max_message_size(GenServer.server()) :: pos_integer() | nil
  def get_max_message_size(broker), do: :gen_statem.call(broker, :get_max_message_size)

  ## gen_statem Callbacks

  @impl true
  def callback_mode, do: [:state_functions, :state_enter]

  @impl true
  def terminate(reason, _state, broker) do
    Logger.info(
      "Broker #{broker.name} terminating: #{inspect(reason)}; #{map_size(broker.consumers)} consumers and #{map_size(broker.producers)} producers will observe the connection exit"
    )

    :ok
  end

  @impl true
  def init(opts) do
    opts = Options.validate!(opts)
    connection_slot = Keyword.fetch!(opts, :connection_slot)
    uri = URI.parse(Keyword.fetch!(opts, :url))
    host = uri.host

    {socket_module, port} =
      case uri.scheme do
        "pulsar" -> {:gen_tcp, uri.port || 6650}
        "pulsar+ssl" -> {:ssl, uri.port || 6651}
      end

    # Option names and struct field names are the same, so struct/2 carries the client's
    # settings across; only what is derived from the URL is set explicitly.
    broker = %{
      struct(__MODULE__, opts)
      | name: "#{host}:#{port}##{connection_slot}",
        host: host,
        port: port,
        socket_module: socket_module
    }

    actions = [{:next_event, :internal, :connect}]
    {:ok, :disconnected, broker, actions}
  end

  ## State Functions

  # Disconnected state
  def disconnected(:enter, :connected, broker) do
    wait = Backoff.next(broker.prev_backoff)
    Logger.error("Broker #{broker.name} connection closed. Reconnecting in #{wait}ms.")

    # Explicitly close the socket to ensure the remote broker cleans up consumers/producers.
    # This is safe to call even if the socket is already closed.
    close_socket(broker)

    # Fail all pending requests immediately to prevent timeouts
    fail_all_pending_requests(broker, :connection_lost)

    # Restart all consumers and producers by exiting their processes
    # The supervision trees will automatically restart them
    restart_consumers_and_producers(broker)

    actions = [{{:timeout, :reconnect}, wait, nil}]
    cleared_broker = %{reset_connection_state(broker) | prev_backoff: wait}

    {:keep_state, cleared_broker, actions}
  end

  def disconnected(:enter, :disconnected, _broker) do
    :keep_state_and_data
  end

  def disconnected({:timeout, :reconnect}, _content, broker) do
    actions = [{:next_event, :internal, :connect}]
    {:keep_state, broker, actions}
  end

  def disconnected(:internal, :connect, broker) do
    %__MODULE__{
      host: host,
      port: port,
      socket_module: mod,
      socket_opts: socket_opts,
      conn_timeout: conn_timeout
    } = broker

    host_charlist = String.to_charlist(host)

    filtered_socket_opts =
      case mod do
        :gen_tcp -> drop_ssl_opts(socket_opts)
        :ssl -> socket_opts
      end

    full_socket_opts =
      filtered_socket_opts ++ [:binary, nodelay: true, active: true, keepalive: true]

    result =
      case mod do
        :gen_tcp -> :gen_tcp.connect(host_charlist, port, full_socket_opts, conn_timeout)
        :ssl -> :ssl.connect(host_charlist, port, full_socket_opts, conn_timeout)
      end

    case result do
      {:ok, socket} ->
        Logger.debug("Broker #{broker.name} connection succeeded")
        actions = [{:next_event, :internal, :handshake}]
        broker = %{reset_connection_state(broker) | socket: socket, prev_backoff: 0}
        {:next_state, :connected, broker, actions}

      {:error, error} ->
        wait = Backoff.next(broker.prev_backoff)
        Logger.error("Broker #{broker.name} connection failed: #{inspect(error)}. Reconnecting in #{wait}ms.")
        actions = [{{:timeout, :reconnect}, wait, nil}]
        {:keep_state, %{broker | prev_backoff: wait}, actions}
    end
  end

  def disconnected({:call, from}, _request, _broker) do
    actions = [{:reply, from, {:error, :disconnected}}]
    {:keep_state_and_data, actions}
  end

  def disconnected(event_type, event_data, _broker) do
    Logger.warning("Discarding #{inspect(event_type)} #{inspect(event_data)} in disconnected state")

    :keep_state_and_data
  end

  # Connected state
  def connected(:enter, _old_state, broker) do
    actions = [
      {{:timeout, :ping}, broker.ping_interval, nil},
      {{:timeout, :cleanup_stale_requests}, broker.cleanup_interval, nil}
    ]

    {:keep_state_and_data, actions}
  end

  def connected(:info, {:tcp_closed, socket}, %__MODULE__{socket: socket} = broker) do
    pending_requests = map_size(broker.requests)

    Logger.error(
      "Broker #{broker.name} socket closed by remote (#{pending_requests} pending requests, #{map_size(broker.consumers)} consumers, #{map_size(broker.producers)} producers)"
    )

    {:next_state, :disconnected, broker}
  end

  def connected(:info, {:ssl_closed, socket}, %__MODULE__{socket: socket} = broker) do
    pending_requests = map_size(broker.requests)

    Logger.error(
      "Broker #{broker.name} socket closed by remote (#{pending_requests} pending requests, #{map_size(broker.consumers)} consumers, #{map_size(broker.producers)} producers)"
    )

    {:next_state, :disconnected, broker}
  end

  def connected(:info, {:tcp_error, socket, reason}, %__MODULE__{socket: socket} = broker) do
    Logger.error("Broker #{broker.name} TCP error: #{inspect(reason)}")
    {:next_state, :disconnected, broker}
  end

  def connected(:info, {:ssl_error, socket, reason}, %__MODULE__{socket: socket} = broker) do
    Logger.error("Broker #{broker.name} SSL error: #{inspect(reason)}")
    {:next_state, :disconnected, broker}
  end

  def connected(:info, {protocol, socket, data}, %__MODULE__{socket: socket} = broker) when protocol in [:tcp, :ssl] do
    case handle_data(data, broker) do
      {:ok, commands, new_broker} ->
        actions = Enum.map(commands, &{:next_event, :internal, {:command, &1}})
        {:keep_state, new_broker, actions}

      {:error, reason} ->
        # The stream cannot be resynchronised, so drop the connection and let the
        # reconnect path re-establish it from a known state.
        Logger.error("Discarding broker #{broker.name} connection: #{inspect(reason)}")

        :telemetry.execute(
          [:pulsar, :connection, :frame_error],
          %{count: 1},
          %{reason: reason, broker: broker.name, connection_slot: broker.connection_slot}
        )

        {:next_state, :disconnected, broker}
    end
  end

  # Bytes from a socket this broker has stopped reading belong to a connection that
  # is already gone, and must not reach the current parse buffer.
  def connected(:info, {protocol, _stale_socket, _data}, _broker) when protocol in [:tcp, :ssl] do
    Logger.debug("Discarding data from a stale socket")
    :keep_state_and_data
  end

  def connected(:info, {event, _stale_socket}, _broker) when event in [:tcp_closed, :ssl_closed] do
    Logger.debug("Discarding #{event} from a stale socket")
    :keep_state_and_data
  end

  def connected(:info, {event, _stale_socket, _reason}, _broker) when event in [:tcp_error, :ssl_error] do
    Logger.debug("Discarding #{event} from a stale socket")
    :keep_state_and_data
  end

  def connected({:timeout, :ping}, _content, broker) do
    ping = %Binary.CommandPing{}

    case send_command_internal(ping, broker) do
      {:ok, new_broker} ->
        actions = [{{:timeout, :ping}, broker.ping_interval, nil}]
        {:keep_state, new_broker, actions}

      {{:error, _error}, new_broker} ->
        {:next_state, :disconnected, new_broker}
    end
  end

  def connected({:timeout, :cleanup_stale_requests}, _content, broker) do
    cleaned_broker = cleanup_stale_requests(broker)
    actions = [{{:timeout, :cleanup_stale_requests}, broker.cleanup_interval, nil}]
    {:keep_state, cleaned_broker, actions}
  end

  def connected(:internal, {:command, command}, broker) do
    Logger.debug("Received #{inspect(command)}")
    handle_command(command, broker)
  end

  def connected(:internal, :handshake, broker) do
    %__MODULE__{auth: auth} = broker

    auth_method_name = get_auth_method_name(auth)
    auth_data = get_auth_data(auth)

    connect_command = %Binary.CommandConnect{
      client_version: @client_version,
      protocol_version: Pulsar.Protocol.latest_version(),
      auth_method_name: auth_method_name,
      auth_data: auth_data
    }

    case send_command_internal(connect_command, broker) do
      {:ok, new_broker} ->
        actions = [{{:timeout, :ping}, broker.ping_interval, nil}] ++ broker.actions
        {:keep_state, new_broker, actions}

      {{:error, _error}, new_broker} ->
        {:next_state, :disconnected, new_broker}
    end
  end

  # Consumer/Producer registration with monitoring
  def connected({:call, from}, {:register_consumer, consumer_id, consumer_pid}, broker) do
    # Monitor the consumer process
    monitor_ref = Process.monitor(consumer_pid)

    new_consumers = Map.put(broker.consumers, consumer_id, {consumer_pid, monitor_ref})
    new_broker = %{broker | consumers: new_consumers}

    Logger.debug("Registered consumer #{consumer_id} and monitoring process")
    actions = [{:reply, from, :ok}]
    {:keep_state, new_broker, actions}
  end

  # Automatic cleanup when monitored processes exit
  def connected(:info, {:DOWN, monitor_ref, :process, pid, reason}, broker) do
    {consumer_id, new_consumers} = remove_by_monitor_ref(broker.consumers, monitor_ref)
    {producer_id, new_producers} = remove_by_monitor_ref(broker.producers, monitor_ref)

    broker =
      broker
      |> close_after_exit(:consumer, consumer_id, pid, reason)
      |> close_after_exit(:producer, producer_id, pid, reason)

    {:keep_state, %{broker | consumers: new_consumers, producers: new_producers}}
  end

  def connected(:info, message, _broker) do
    Logger.warning("Discarding unexpected message: #{inspect(message)}")
    :keep_state_and_data
  end

  def connected({:call, from}, {:register_producer, producer_id, producer_pid}, broker) do
    # Monitor the producer process
    monitor_ref = Process.monitor(producer_pid)

    new_producers = Map.put(broker.producers, producer_id, {producer_pid, monitor_ref})
    new_broker = %{broker | producers: new_producers}

    Logger.debug("Registered producer #{producer_id} and monitoring process")
    actions = [{:reply, from, :ok}]
    {:keep_state, new_broker, actions}
  end

  # Command sending
  def connected(:cast, {:send_command, command}, broker) do
    case send_command_internal(command, broker) do
      {:ok, new_broker} ->
        {:keep_state, new_broker}

      {{:error, reason}, new_broker} ->
        Logger.error("Failed to send command #{inspect(command)}: #{inspect(reason)}")
        {:keep_state, new_broker}
    end
  end

  # An oversized frame is answered by closing the connection, which would take down every
  # consumer and producer registered here.
  def connected({:call, from}, {:publish_message, encoded_message}, %__MODULE__{max_message_size: limit} = broker)
      when is_integer(limit) and byte_size(encoded_message) > limit do
    {:keep_state, broker, [{:reply, from, {:error, :message_too_large}}]}
  end

  def connected({:call, from}, {:publish_message, encoded_message}, broker) do
    %__MODULE__{socket_module: mod, socket: socket} = broker

    result =
      case mod do
        :gen_tcp -> :gen_tcp.send(socket, encoded_message)
        :ssl -> :ssl.send(socket, encoded_message)
      end

    case result do
      :ok ->
        {:keep_state, broker, [{:reply, from, :ok}]}

      {:error, reason} ->
        {:keep_state, broker, [{:reply, from, {:error, reason}}]}
    end
  end

  def connected({:call, from}, {:send_request, command}, broker) do
    request_id = System.unique_integer([:positive, :monotonic])
    command_with_id = Map.put(command, :request_id, request_id)
    timestamp = System.monotonic_time(:millisecond)

    # Store the request with timestamp for correlation and cleanup
    new_requests = Map.put(broker.requests, request_id, {from, timestamp})
    updated_broker = %{broker | requests: new_requests}

    case send_command_internal(command_with_id, updated_broker) do
      {:ok, final_broker} ->
        {:keep_state, final_broker}

      {{:error, reason}, final_broker} ->
        # Remove the failed request
        cleaned_requests = Map.delete(final_broker.requests, request_id)
        cleaned_broker = %{final_broker | requests: cleaned_requests}
        actions = [{:reply, from, {:error, reason}}]
        {:keep_state, cleaned_broker, actions}
    end
  end

  # Service Discovery
  def connected({:call, from}, {:lookup_topic, topic, authoritative}, broker) do
    request_id = System.unique_integer([:positive, :monotonic])
    timestamp = System.monotonic_time(:millisecond)
    new_requests = Map.put(broker.requests, request_id, {from, timestamp})
    updated_broker = %{broker | requests: new_requests}

    command = %Binary.CommandLookupTopic{
      topic: topic,
      request_id: request_id,
      authoritative: authoritative
    }

    case send_command_internal(command, updated_broker) do
      {:ok, final_broker} ->
        {:keep_state, final_broker}

      {{:error, reason}, final_broker} ->
        cleaned_requests = Map.delete(final_broker.requests, request_id)
        cleaned_broker = %{final_broker | requests: cleaned_requests}
        actions = [{:reply, from, {:error, reason}}]
        {:keep_state, cleaned_broker, actions}
    end
  end

  def connected({:call, from}, {:partitioned_topic_metadata, topic}, broker) do
    request_id = System.unique_integer([:positive, :monotonic])
    timestamp = System.monotonic_time(:millisecond)
    new_requests = Map.put(broker.requests, request_id, {from, timestamp})
    updated_broker = %{broker | requests: new_requests}

    command = %Binary.CommandPartitionedTopicMetadata{
      topic: topic,
      request_id: request_id
    }

    case send_command_internal(command, updated_broker) do
      {:ok, final_broker} ->
        {:keep_state, final_broker}

      {{:error, reason}, final_broker} ->
        cleaned_requests = Map.delete(final_broker.requests, request_id)
        cleaned_broker = %{final_broker | requests: cleaned_requests}
        actions = [{:reply, from, {:error, reason}}]
        {:keep_state, cleaned_broker, actions}
    end
  end

  def connected({:call, from}, :get_consumers, broker) do
    # Return map with consumer_id -> pid (strip monitor refs)
    consumers = Map.new(broker.consumers, fn {id, {pid, _ref}} -> {id, pid} end)
    actions = [{:reply, from, consumers}]
    {:keep_state, broker, actions}
  end

  def connected({:call, from}, :get_producers, broker) do
    # Return map with producer_id -> pid (strip monitor refs)
    producers = Map.new(broker.producers, fn {id, {pid, _ref}} -> {id, pid} end)
    actions = [{:reply, from, producers}]
    {:keep_state, broker, actions}
  end

  def connected({:call, from}, :get_max_message_size, broker) do
    actions = [{:reply, from, broker.max_message_size}]
    {:keep_state, broker, actions}
  end

  def connected({:call, from}, request, broker) do
    Logger.debug("Handling request #{inspect(request)}")
    actions = [{:reply, from, {:ok, :handled}}]
    {:keep_state, broker, actions}
  end

  ## Command Handlers

  defp handle_command(%Binary.CommandPing{}, broker) do
    pong = %Binary.CommandPong{}

    case send_command_internal(pong, broker) do
      {:ok, new_broker} -> {:keep_state, new_broker}
      {{:error, _}, new_broker} -> {:next_state, :disconnected, new_broker}
    end
  end

  defp handle_command(%Binary.CommandPong{}, _broker) do
    :keep_state_and_data
  end

  defp handle_command(%Binary.CommandConnected{} = cmd, broker) do
    Logger.info(
      "Successfully connected to broker #{broker.name}: protocol_version=#{cmd.protocol_version}, server_version=#{cmd.server_version}"
    )

    :telemetry.execute(
      [:pulsar, :connection, :connected],
      %{count: 1},
      %{
        broker: broker.name,
        connection_slot: broker.connection_slot,
        max_message_size: cmd.max_message_size
      }
    )

    {:keep_state, %{broker | max_message_size: cmd.max_message_size}}
  end

  defp handle_command(%Binary.CommandLookupTopicResponse{request_id: request_id} = command, broker) do
    reply = {:ok, command}
    new_broker = reply_to_request(broker, request_id, reply)
    {:keep_state, new_broker}
  end

  defp handle_command(%Binary.CommandPartitionedTopicMetadataResponse{request_id: request_id} = command, broker) do
    reply = {:ok, command}
    new_broker = reply_to_request(broker, request_id, reply)
    {:keep_state, new_broker}
  end

  defp handle_command(%Binary.CommandError{request_id: request_id} = error, broker) do
    reply = {:error, {error.error, error.message}}
    new_broker = reply_to_request(broker, request_id, reply)
    {:keep_state, new_broker}
  end

  defp handle_command(%Binary.CommandSuccess{request_id: request_id} = success, broker) do
    reply = {:ok, success}
    new_broker = reply_to_request(broker, request_id, reply)
    {:keep_state, new_broker}
  end

  defp handle_command(
         {%Binary.CommandMessage{consumer_id: consumer_id} = command, metadata, payload, broker_metadata},
         broker
       ) do
    case Map.get(broker.consumers, consumer_id) do
      nil ->
        Logger.warning("Received message for unknown consumer #{consumer_id}")
        :keep_state_and_data

      {consumer_pid, _monitor_ref} ->
        send(consumer_pid, {:broker_message, {command, metadata, payload, broker_metadata}})
        :keep_state_and_data
    end
  end

  defp handle_command({:invalid, %Binary.CommandMessage{consumer_id: consumer_id} = command, bytes, reason}, broker) do
    case Map.get(broker.consumers, consumer_id) do
      nil ->
        Logger.warning("Received #{reason} for unknown consumer #{consumer_id}")
        :keep_state_and_data

      {consumer_pid, _monitor_ref} ->
        Logger.warning("Message for consumer #{consumer_id} failed validation: #{reason}")

        send(consumer_pid, {:broker_message, {:invalid, command, bytes, reason}})
        :keep_state_and_data
    end
  end

  # Handle broker-initiated closures - crash the consumer/producer and let supervisor restart
  defp handle_command(%Binary.CommandCloseConsumer{consumer_id: consumer_id} = command, broker) do
    case Map.get(broker.consumers, consumer_id) do
      nil ->
        Logger.warning("Received close command for unknown consumer #{consumer_id}")
        :keep_state_and_data

      {consumer_pid, _monitor_ref} ->
        Logger.warning("Broker requested consumer #{consumer_id} closure")

        send(consumer_pid, {:broker_message, command})
        :keep_state_and_data
    end
  end

  # Failover subscriptions: broker notifies a consumer when it becomes the
  # active (or passive) consumer for the subscription.
  defp handle_command(%Binary.CommandActiveConsumerChange{consumer_id: consumer_id} = command, broker) do
    case Map.get(broker.consumers, consumer_id) do
      nil ->
        Logger.warning("Received active consumer change for unknown consumer #{consumer_id}")
        :keep_state_and_data

      {consumer_pid, _monitor_ref} ->
        send(consumer_pid, {:broker_message, command})
        :keep_state_and_data
    end
  end

  # Informational, so an unknown consumer is only worth a debug line: nothing to clean up.
  defp handle_command(%Binary.CommandReachedEndOfTopic{consumer_id: consumer_id}, %__MODULE__{consumers: consumers})
       when not is_map_key(consumers, consumer_id) do
    Logger.debug("Received end of topic for unknown consumer #{consumer_id}")
    :keep_state_and_data
  end

  defp handle_command(%Binary.CommandReachedEndOfTopic{consumer_id: consumer_id} = command, broker) do
    {consumer_pid, _monitor_ref} = Map.fetch!(broker.consumers, consumer_id)
    send(consumer_pid, {:broker_message, command})

    :keep_state_and_data
  end

  defp handle_command(%Binary.CommandCloseProducer{producer_id: producer_id} = command, broker) do
    case Map.get(broker.producers, producer_id) do
      nil ->
        Logger.warning("Received close command for unknown producer #{producer_id}")
        :keep_state_and_data

      {producer_pid, _monitor_ref} ->
        Logger.warning("Broker requested producer #{producer_id} closure")

        send(producer_pid, {:broker_message, command})
        :keep_state_and_data
    end
  end

  defp handle_command(%Binary.CommandSendReceipt{producer_id: producer_id} = receipt, broker) do
    case Map.get(broker.producers, producer_id) do
      {producer_pid, _ref} ->
        send(producer_pid, {:send_receipt, receipt})
        :keep_state_and_data

      nil ->
        Logger.warning("Received send receipt for unknown producer #{producer_id}")
        :keep_state_and_data
    end
  end

  defp handle_command(%Binary.CommandSendError{producer_id: producer_id} = error, broker) do
    case Map.get(broker.producers, producer_id) do
      {producer_pid, _ref} ->
        send(producer_pid, {:send_error, error})
        :keep_state_and_data

      nil ->
        Logger.warning("Received send error for unknown producer #{producer_id}")
        :keep_state_and_data
    end
  end

  defp handle_command(%Binary.CommandProducerSuccess{} = command, broker) do
    # CommandProducerSuccess can arrive twice for WaitForExclusive mode:
    # 1. First with producer_ready: false (pending state, request_id in broker.requests)
    # 2. Second with producer_ready: true (final state, request_id NOT in broker.requests, find pid by name)
    request_id = command.request_id

    if Map.has_key?(broker.requests, request_id) do
      # Initial registration response - request is still pending
      # handle_producer_registration_response(command, broker, request_id)
      new_broker = reply_to_request(broker, request_id, {:ok, command})
      {:keep_state, new_broker}
    else
      # Subsequent notification: broadcast to all producers and let the correct one handle it
      Enum.each(broker.producers, fn {_id, {producer_pid, _ref}} ->
        send(producer_pid, {:broker_message, command})
      end)

      :keep_state_and_data
    end
  end

  defp handle_command(%Binary.CommandAckResponse{request_id: request_id} = command, broker) do
    reply = {:ok, command}
    new_broker = reply_to_request(broker, request_id, reply)
    {:keep_state, new_broker}
  end

  defp handle_command(command, _broker) do
    Logger.debug("Unhandled command: #{inspect(command)}")
    :keep_state_and_data
  end

  ## Private Functions

  # Socket option lists are not keyword lists: `:inet6` and `{:raw, level, opt, value}`
  # are both valid and match neither `{key, value}` nor `Keyword.drop/2`.
  @doc false
  @spec drop_ssl_opts([:gen_tcp.connect_option()]) :: [:gen_tcp.connect_option()]
  def drop_ssl_opts(socket_opts) do
    Enum.reject(socket_opts, &match?({key, _value} when key in @ssl_only_opts, &1))
  end

  defp restart_consumers_and_producers(broker) do
    # Exit all consumer processes - supervision trees will restart them
    Enum.each(broker.consumers, fn {consumer_id, {consumer_pid, _monitor_ref}} ->
      if Process.alive?(consumer_pid) do
        Logger.debug("Restarting consumer #{consumer_id}")
        Process.exit(consumer_pid, :broker_disconnected)
      end
    end)

    # Exit all producer processes - supervision trees will restart them
    Enum.each(broker.producers, fn {producer_id, {producer_pid, _monitor_ref}} ->
      if Process.alive?(producer_pid) do
        Logger.debug("Restarting producer #{producer_id}")
        Process.exit(producer_pid, :broker_disconnected)
      end
    end)

    Logger.info(
      "Restarted #{map_size(broker.consumers)} consumers and #{map_size(broker.producers)} producers due to broker disconnect"
    )
  end

  defp cleanup_stale_requests(broker) do
    current_time = System.monotonic_time(:millisecond)
    timeout_threshold = broker.request_timeout

    {stale_requests, active_requests} =
      Enum.split_with(broker.requests, fn {_request_id, {_from, timestamp}} ->
        current_time - timestamp > timeout_threshold
      end)

    # Reply with timeout errors to stale requests
    Enum.each(stale_requests, fn {request_id, {from, _timestamp}} ->
      Logger.warning("Request #{request_id} timed out after #{timeout_threshold}ms")
      :gen_statem.reply(from, {:error, :timeout})
    end)

    if !Enum.empty?(stale_requests) do
      Logger.info("Cleaned up #{length(stale_requests)} stale requests")
    end

    # Keep only active requests
    %{broker | requests: Map.new(active_requests)}
  end

  defp fail_all_pending_requests(broker, reason) do
    Enum.each(broker.requests, fn {_request_id, {from, _timestamp}} ->
      :gen_statem.reply(from, {:error, reason})
    end)
  end

  defp reset_connection_state(broker) do
    struct!(broker, @connection_fields)
  end

  defp send_command_internal(command, broker) do
    %__MODULE__{socket_module: mod, socket: socket} = broker

    try do
      encoded_command = Pulsar.Protocol.encode(command)

      result =
        case mod do
          :gen_tcp -> :gen_tcp.send(socket, encoded_command)
          :ssl -> :ssl.send(socket, encoded_command)
        end

      case result do
        :ok -> {:ok, broker}
        {:error, reason} -> {{:error, reason}, broker}
      end
    rescue
      error -> {{:error, error}, broker}
    end
  end

  defp reply_to_request(broker, request_id, reply) do
    case Map.get(broker.requests, request_id) do
      nil ->
        Logger.warning("No requester found for request #{request_id}")
        broker

      {from, _timestamp} ->
        :gen_statem.reply(from, reply)
        # Remove the request after replying
        new_requests = Map.delete(broker.requests, request_id)
        %{broker | requests: new_requests}

      # Handle legacy format during transition
      from when not is_tuple(from) ->
        :gen_statem.reply(from, reply)
        new_requests = Map.delete(broker.requests, request_id)
        %{broker | requests: new_requests}
    end
  end

  defp handle_data(data, broker) do
    case Pulsar.Protocol.decode_stream(broker.buffer, data, broker.max_frame_size) do
      {:ok, commands, buffer} -> {:ok, commands, %{broker | buffer: buffer}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp get_auth_method_name(type: type, opts: opts) do
    type.auth_method_name(opts)
  end

  defp get_auth_method_name(_), do: ""

  defp get_auth_data(type: type, opts: opts) do
    type.auth_data(opts)
  end

  defp get_auth_data(_), do: ""

  defp close_socket(%__MODULE__{socket: nil}), do: :ok

  defp close_socket(%__MODULE__{socket_module: :gen_tcp, socket: socket}) do
    :gen_tcp.close(socket)
  rescue
    _ -> :ok
  end

  defp close_socket(%__MODULE__{socket_module: :ssl, socket: socket}) do
    :ssl.close(socket)
  rescue
    _ -> :ok
  end

  # A registered consumer or producer that exits owes the server a close. A failed send is
  # dropped rather than retried: the entry goes either way, so a reconnect never carries a stale one.
  defp close_after_exit(broker, _kind, nil, _pid, _reason), do: broker

  defp close_after_exit(broker, kind, id, pid, reason) do
    label = kind |> Atom.to_string() |> String.capitalize()
    request_id = System.unique_integer([:positive, :monotonic])
    timestamp = System.monotonic_time(:millisecond)

    # The reply is addressed to the process that just exited, so sending it is a no-op. Registering
    # it anyway keeps one bookkeeping path: the ack clears the entry, or the sweeper times it out.
    new_requests = Map.put(broker.requests, request_id, {{pid, make_ref()}, timestamp})
    broker = %{broker | requests: new_requests}

    Logger.info("#{label} #{id} exited: #{inspect(reason)}, sending Close#{label} to server")

    case send_command_internal(close_command(kind, id, request_id), broker) do
      {:ok, updated_broker} ->
        updated_broker

      {{:error, send_error}, updated_broker} ->
        Logger.warning("Failed to send Close#{label} for #{kind} #{id}: #{inspect(send_error)}")
        updated_broker
    end
  end

  defp close_command(:consumer, consumer_id, request_id) do
    %Binary.CommandCloseConsumer{consumer_id: consumer_id, request_id: request_id}
  end

  defp close_command(:producer, producer_id, request_id) do
    %Binary.CommandCloseProducer{producer_id: producer_id, request_id: request_id}
  end

  defp remove_by_monitor_ref(map, monitor_ref) do
    case Enum.find(map, fn {_id, {_pid, ref}} -> ref == monitor_ref end) do
      nil -> {nil, map}
      {id, _entry} -> {id, Map.delete(map, id)}
    end
  end
end
