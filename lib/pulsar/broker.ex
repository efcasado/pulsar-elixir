defmodule Pulsar.Broker do
  @moduledoc """
  Unified Pulsar broker connection process.

  This module combines:
  - TCP connection management with reconnection logic
  - Protocol handshake and authentication
  - Service discovery functionality
  - Consumer and producer registration and message routing
  - Request/response correlation

  Uses gen_statem for robust state management with states:
  - :disconnected - Not connected to broker
  - :connected - Connected and authenticated, ready for operations

  Consumer and producer processes are monitored by this broker process
  for automatic cleanup when they exit.
  """

  @behaviour :gen_statem

  require Logger
  alias Pulsar.Config
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

  # Main connection state (unified from Connection)
  defstruct [
    :name,
    :host,
    :port,
    :socket_module,
    :socket,
    :prev_backoff,
    :socket_opts,
    :conn_timeout,
    :auth,
    :buffer,
    :pending_bytes,
    :requests,
    :actions,
    # Broker-specific state
    :consumers,
    :producers
  ]

  @type t :: %__MODULE__{
          name: String.t(),
          host: String.t(),
          port: integer(),
          socket_module: :gen_tcp | :ssl,
          socket: :gen_tcp.socket() | :ssl.sslsocket() | nil,
          prev_backoff: integer(),
          socket_opts: list(),
          conn_timeout: integer(),
          auth: list(),
          buffer: binary(),
          pending_bytes: integer(),
          requests: %{integer() => {GenServer.from(), integer()}},
          actions: list(),
          consumers: %{integer() => {pid(), reference()}},
          producers: %{integer() => {pid(), reference()}}
        }

  ## Public API

  @doc """
  Starts a broker connection process.

  The target Pulsar broker is expected to be specified in the form of: `<scheme>://<host>[:<port>]`,
  where `scheme` can be either `pulsar` or `pulsar+ssl` and `port` is an optional field that
  defaults to `6650` and `6651`, respectively.
  """
  @spec start_link(String.t(), keyword()) :: {:ok, pid()} | :ignore | {:error, term()}
  def start_link(broker_url, opts \\ []) do
    name = Keyword.get(opts, :name, nil)

    args = [
      name,
      broker_url,
      Keyword.get(opts, :socket_opts, []),
      Keyword.get(opts, :conn_timeout, 5_000),
      Keyword.get(opts, :auth, type: Pulsar.Auth.None, opts: []),
      Keyword.get(opts, :actions, [])
    ]

    start_opts = Keyword.take(opts, [:name])

    case name do
      nil ->
        :gen_statem.start_link(__MODULE__, args, start_opts)

      name ->
        :gen_statem.start_link(name, __MODULE__, args, start_opts)
    end
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
    :gen_statem.call(broker, {:send_command, command})
  end

  @doc """
  Sends a command to the broker and expects a response.
  """
  @spec send_request(GenServer.server(), struct(), timeout()) :: {:ok, term()} | {:error, term()}
  def send_request(broker, command, timeout \\ 5000) do
    :gen_statem.call(broker, {:send_request, command}, timeout)
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
  Gets the list of registered consumers.

  Accepts either a broker PID or a broker URL string.
  """
  @spec get_consumers(GenServer.server() | String.t()) :: %{integer() => pid()}
  def get_consumers(broker) when is_pid(broker) do
    :gen_statem.call(broker, :get_consumers)
  end

  def get_consumers(broker_url) when is_binary(broker_url) do
    case Pulsar.lookup_broker(broker_url) do
      {:ok, broker_pid} -> get_consumers(broker_pid)
      {:error, :not_found} -> %{}
    end
  end

  @doc """
  Gets the list of registered producers.

  Accepts either a broker PID or a broker URL string.
  """
  @spec get_producers(GenServer.server() | String.t()) :: %{integer() => pid()}
  def get_producers(broker) when is_pid(broker) do
    :gen_statem.call(broker, :get_producers)
  end

  def get_producers(broker_url) when is_binary(broker_url) do
    case Pulsar.lookup_broker(broker_url) do
      {:ok, broker_pid} -> get_producers(broker_pid)
      {:error, :not_found} -> %{}
    end
  end

  @doc """
  Gracefully stops the broker by closing all consumers/producers first.
  """
  @spec stop(GenServer.server(), term(), timeout()) :: :ok
  def stop(broker, reason \\ :normal, timeout \\ :infinity) do
    :gen_statem.stop(broker, reason, timeout)
  end

  ## gen_statem Callbacks

  @impl true
  def callback_mode, do: [:state_functions, :state_enter]

  @impl true
  def terminate(reason, _state, broker) do
    Logger.info(
      "Broker terminating: #{inspect(reason)}, gracefully stopping #{map_size(broker.consumers)} consumers and #{map_size(broker.producers)} producers"
    )

    # Gracefully stop all consumer processes
    Enum.each(broker.consumers, fn {consumer_id, {consumer_pid, _monitor_ref}} ->
      if Process.alive?(consumer_pid) do
        Logger.debug("Gracefully stopping consumer #{consumer_id}")
        Pulsar.Consumer.stop(consumer_pid)
      end
    end)

    # Gracefully stop all producer processes (when we add Producer.stop/1)
    Enum.each(broker.producers, fn {producer_id, {producer_pid, _monitor_ref}} ->
      if Process.alive?(producer_pid) do
        Logger.debug("Gracefully stopping producer #{producer_id}")
        # TODO: Add Pulsar.Producer.stop(producer_pid) when we implement producers
      end
    end)

    :ok
  end

  @impl true
  def init([name, uri, socket_opts, conn_timeout, auth, post_actions]) do
    uri = URI.parse(uri)
    host = Map.get(uri, :host, "localhost")
    port = Map.get(uri, :port, default_port(uri.scheme))

    socket_module =
      case Map.get(uri, :scheme, "pulsar") do
        "pulsar+ssl" -> :ssl
        "pulsar" -> :gen_tcp
      end

    broker = %__MODULE__{
      name: name || broker_key(to_string(uri)),
      host: host,
      port: port,
      socket_module: socket_module,
      socket_opts: socket_opts,
      conn_timeout: conn_timeout,
      auth: auth,
      actions: post_actions,
      buffer: <<>>,
      pending_bytes: 0,
      requests: %{},
      consumers: %{},
      producers: %{},
      prev_backoff: 0
    }

    actions = [{:next_event, :internal, :connect}]
    {:ok, :disconnected, broker, actions}
  end

  ## State Functions

  # Disconnected state
  def disconnected(:enter, :connected, broker) do
    wait = next_backoff(broker)
    Logger.error("Connection closed. Reconnecting in #{wait}ms.")

    # Restart all consumers and producers by exiting their processes
    # The supervision trees will automatically restart them
    restart_consumers_and_producers(broker)

    actions = [{{:timeout, :reconnect}, wait, nil}]
    # Clear consumers and producers since we've restarted them
    cleared_broker = %__MODULE__{
      broker
      | socket: nil,
        prev_backoff: wait,
        consumers: %{},
        producers: %{}
    }

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
    full_socket_opts = socket_opts ++ [:binary, nodelay: true, active: true, keepalive: true]

    case apply(mod, :connect, [host_charlist, port, full_socket_opts, conn_timeout]) do
      {:ok, socket} ->
        Logger.debug("Connection succeeded")
        actions = [{:next_event, :internal, :handshake}]
        {:next_state, :connected, %__MODULE__{broker | socket: socket, prev_backoff: 0}, actions}

      {:error, error} ->
        wait = next_backoff(broker)

        Logger.error("Connection failed: #{inspect(error)}. Reconnecting in #{wait}ms.")

        actions = [{{:timeout, :reconnect}, wait, nil}]
        {:keep_state, %__MODULE__{broker | prev_backoff: wait}, actions}
    end
  end

  def disconnected({:call, from}, _request, _broker) do
    actions = [{:reply, from, {:error, :disconnected}}]
    {:keep_state_and_data, actions}
  end

  def disconnected(event_type, event_data, _broker) do
    Logger.warning(
      "Discarding #{inspect(event_type)} #{inspect(event_data)} in disconnected state"
    )

    :keep_state_and_data
  end

  # Connected state
  def connected(:enter, _old_state, _broker) do
    actions = [
      {{:timeout, :ping}, Config.ping_interval(), nil},
      {{:timeout, :cleanup_stale_requests}, Config.cleanup_interval(), nil}
    ]

    {:keep_state_and_data, actions}
  end

  def connected(:info, {:tcp_closed, socket}, %__MODULE__{socket: socket} = broker) do
    Logger.debug("Socket closed")
    {:next_state, :disconnected, broker}
  end

  def connected(:info, {:ssl_closed, socket}, %__MODULE__{socket: socket} = broker) do
    Logger.debug("Socket closed")
    {:next_state, :disconnected, broker}
  end

  def connected(:info, {:tcp_error, socket, reason}, %__MODULE__{socket: socket} = broker) do
    Logger.error("TCP error: #{inspect(reason)}")
    {:next_state, :disconnected, broker}
  end

  def connected(:info, {:ssl_error, socket, reason}, %__MODULE__{socket: socket} = broker) do
    Logger.error("SSL error: #{inspect(reason)}")
    {:next_state, :disconnected, broker}
  end

  def connected(:info, {protocol, _socket, data}, broker) when protocol in [:tcp, :ssl] do
    {commands, new_broker} = handle_data(data, broker)
    actions = Enum.map(commands, &{:next_event, :internal, {:command, &1}})
    {:keep_state, new_broker, actions}
  end

  def connected({:timeout, :ping}, _content, broker) do
    ping = %Binary.CommandPing{}

    case send_command_internal(ping, broker) do
      {:ok, new_broker} ->
        actions = [{{:timeout, :ping}, Config.ping_interval(), nil}]
        {:keep_state, new_broker, actions}

      {{:error, _error}, new_broker} ->
        {:next_state, :disconnected, new_broker}
    end
  end

  def connected({:timeout, :cleanup_stale_requests}, _content, broker) do
    cleaned_broker = cleanup_stale_requests(broker)
    actions = [{{:timeout, :cleanup_stale_requests}, Config.cleanup_interval(), nil}]
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
      client_version: Config.client_version(),
      protocol_version: Config.protocol_version(),
      auth_method_name: auth_method_name,
      auth_data: auth_data
    }

    case send_command_internal(connect_command, broker) do
      {:ok, new_broker} ->
        actions = [{{:timeout, :ping}, Config.ping_interval(), nil}] ++ broker.actions
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
    # Find and remove the consumer/producer that died
    {consumer_id, new_consumers} = remove_by_monitor_ref(broker.consumers, monitor_ref, pid)
    {producer_id, new_producers} = remove_by_monitor_ref(broker.producers, monitor_ref, pid)

    if consumer_id do
      Logger.info("Consumer #{consumer_id} exited: #{inspect(reason)}")
    end

    if producer_id do
      Logger.info("Producer #{producer_id} exited: #{inspect(reason)}")
    end

    new_broker = %{broker | consumers: new_consumers, producers: new_producers}
    {:keep_state, new_broker}
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
  def connected({:call, from}, {:send_command, command}, broker) do
    case send_command_internal(command, broker) do
      {:ok, new_broker} ->
        actions = [{:reply, from, :ok}]
        {:keep_state, new_broker, actions}

      {{:error, reason}, new_broker} ->
        actions = [{:reply, from, {:error, reason}}]
        {:keep_state, new_broker, actions}
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

  defp handle_command(%Binary.CommandConnected{}, _broker) do
    :keep_state_and_data
  end

  defp handle_command(
         %Binary.CommandLookupTopicResponse{request_id: request_id} = command,
         broker
       ) do
    reply = {:ok, command}
    new_broker = reply_to_request(broker, request_id, reply)
    {:keep_state, new_broker}
  end

  defp handle_command(
         %Binary.CommandPartitionedTopicMetadataResponse{request_id: request_id} = command,
         broker
       ) do
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

  defp handle_command(%Binary.CommandMessage{consumer_id: consumer_id} = command, broker) do
    case Map.get(broker.consumers, consumer_id) do
      nil ->
        Logger.warning("Received message for unknown consumer #{consumer_id}")
        :keep_state_and_data

      {consumer_pid, _monitor_ref} ->
        send(consumer_pid, {:broker_message, command})
        :keep_state_and_data
    end
  end

  defp handle_command(
         {%Binary.CommandMessage{consumer_id: consumer_id} = command, metadata, payload},
         broker
       ) do
    case Map.get(broker.consumers, consumer_id) do
      nil ->
        Logger.warning("Received message for unknown consumer #{consumer_id}")
        :keep_state_and_data

      {consumer_pid, _monitor_ref} ->
        send(consumer_pid, {:broker_message, {command, metadata, payload}})
        :keep_state_and_data
    end
  end

  # Handle broker-initiated closures - crash the consumer/producer and let supervisor restart
  defp handle_command(%Binary.CommandCloseConsumer{consumer_id: consumer_id}, broker) do
    case Map.get(broker.consumers, consumer_id) do
      nil ->
        Logger.warning("Received close command for unknown consumer #{consumer_id}")
        :keep_state_and_data

      {consumer_pid, _monitor_ref} ->
        Logger.info(
          "Broker requested consumer #{consumer_id} closure, will restart with fresh lookup"
        )

        Process.exit(consumer_pid, :broker_close_requested)
        :keep_state_and_data
    end
  end

  defp handle_command(%Binary.CommandCloseProducer{producer_id: producer_id}, broker) do
    case Map.get(broker.producers, producer_id) do
      nil ->
        Logger.warning("Received close command for unknown producer #{producer_id}")
        :keep_state_and_data

      {producer_pid, _monitor_ref} ->
        Logger.info(
          "Broker requested producer #{producer_id} closure, will restart with fresh lookup"
        )

        Process.exit(producer_pid, :broker_close_requested)
        :keep_state_and_data
    end
  end

  defp handle_command(command, broker) when is_map(command) do
    # Handle responses with request_id
    case Map.get(command, :request_id) do
      nil ->
        Logger.debug("Received command without request_id: #{inspect(command)}")
        :keep_state_and_data

      request_id ->
        reply = {:ok, command}
        new_broker = reply_to_request(broker, request_id, reply)
        {:keep_state, new_broker}
    end
  end

  defp handle_command(command, _broker) do
    Logger.debug("Unhandled command: #{inspect(command)}")
    :keep_state_and_data
  end

  ## Private Functions

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
    timeout_threshold = Config.request_timeout()

    {stale_requests, active_requests} =
      Enum.split_with(broker.requests, fn {_request_id, {_from, timestamp}} ->
        current_time - timestamp > timeout_threshold
      end)

    # Reply with timeout errors to stale requests
    Enum.each(stale_requests, fn {request_id, {from, _timestamp}} ->
      Logger.warning("Request #{request_id} timed out after #{timeout_threshold}ms")
      :gen_statem.reply(from, {:error, :timeout})
    end)

    if length(stale_requests) > 0 do
      Logger.info("Cleaned up #{length(stale_requests)} stale requests")
    end

    # Keep only active requests
    %{broker | requests: Map.new(active_requests)}
  end

  defp send_command_internal(command, broker) do
    %__MODULE__{socket_module: mod, socket: socket} = broker

    try do
      encoded_command = Pulsar.Protocol.encode(command)

      case apply(mod, :send, [socket, encoded_command]) do
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
    parse_data(data, broker.buffer, broker.pending_bytes, broker, [])
  end

  defp parse_data(<<>>, buffer, pending_bytes, broker, commands) do
    new_broker = %{broker | buffer: buffer, pending_bytes: pending_bytes}
    {Enum.reverse(commands), new_broker}
  end

  defp parse_data(data, buffer, pending_bytes, broker, commands) when pending_bytes > 0 do
    case data do
      <<missing_chunk::bytes-size(pending_bytes), rest::binary>> ->
        command = Pulsar.Protocol.decode(buffer <> missing_chunk)
        parse_data(rest, <<>>, 0, broker, [command | commands])

      missing_chunk ->
        new_buffer = buffer <> missing_chunk
        new_pending = pending_bytes - byte_size(missing_chunk)
        parse_data(<<>>, new_buffer, new_pending, broker, commands)
    end
  end

  defp parse_data(<<total_size::32, _rest::binary>> = data, buffer, 0, broker, commands)
       when total_size + 4 > byte_size(data) do
    # Incomplete message
    new_buffer = buffer <> data
    new_pending = total_size + 4 - byte_size(data)
    parse_data(<<>>, new_buffer, new_pending, broker, commands)
  end

  defp parse_data(
         <<total_size::32, size::32, command_data::bytes-size(total_size - 4), rest::binary>>,
         _buffer,
         0,
         broker,
         commands
       ) do
    command = Pulsar.Protocol.decode(<<total_size::32, size::32, command_data::bytes>>)
    parse_data(rest, <<>>, 0, broker, [command | commands])
  end

  defp next_backoff(%__MODULE__{prev_backoff: 0}) do
    1000 + :rand.uniform(1000)
  end

  defp next_backoff(%__MODULE__{prev_backoff: prev}) do
    next = round(prev * 2)
    max_backoff = Application.get_env(:pulsar, :max_backoff, 30_000)
    next = min(next, max_backoff)
    next + :rand.uniform(1000)
  end

  defp get_auth_method_name(type: type, opts: opts) do
    apply(type, :auth_method_name, [opts])
  end

  defp get_auth_method_name(_), do: ""

  defp get_auth_data(type: type, opts: opts) do
    apply(type, :auth_data, [opts])
  end

  defp get_auth_data(_), do: ""

  defp default_port("pulsar+ssl"), do: 6651
  defp default_port("pulsar"), do: 6650
  defp default_port(_), do: 6650

  defp broker_key(broker_url) do
    %URI{host: host, port: port} = URI.parse(broker_url)
    "#{host}:#{port}"
  end

  # Helper to find and remove entries by monitor reference
  defp remove_by_monitor_ref(map, target_monitor_ref, target_pid) do
    Enum.reduce(map, {nil, map}, fn
      {id, {pid, monitor_ref}}, {found_id, acc_map} ->
        if monitor_ref == target_monitor_ref and pid == target_pid do
          # Found the matching entry, remove it
          {id, Map.delete(acc_map, id)}
        else
          {found_id, acc_map}
        end
    end)
  end
end
