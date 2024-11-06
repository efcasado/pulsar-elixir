defmodule Pulsar.Connection do
  # https://github.com/elixir-ecto/connection
  # https://andrealeopardi.com/posts/connection-managers-with-gen-statem/
  # https://www.erlang.org/doc/system/statem.html
  # https://www.erlang.org/doc/apps/stdlib/gen_statem.html
  @doc """
  Process responsible for managing a persistent TCP connection towards a Pulsar
  broker.
  """

  defstruct name: "",
    host: "",
    port: 6650,
    socket_module: :gen_tcp,
    socket: nil,
    prev_backoff: 0,
    socket_opts: [],
    conn_timeout: 5_000,
    auth: [type: Pulsar.Auth.None, opts: []],
    buffer: <<>>,
    pending_bytes: 0,
    requests: Map.new(),
    actions: []

  @type t :: %__MODULE__{
    name: String.t(),
    host: String.t(),
    port: integer(),
    socket_module: :gen_tcp | :ssl,
    socket: :gen_tcp.socket() | :ssl.sslsocket(),
    prev_backoff: 0,
    socket_opts: list(),
    conn_timeout: integer(),
    auth: list(),
    buffer: binary(),
    pending_bytes: integer(),
    requests: Map.t(),
    actions: list()
  }
  
  @doc """
  Starts a persistent connection towards the provided Pulsar broker.
  
  The target Pulsar broker is expected to be specified in the form of: `<scheme>://<host>[:<port>]`,
  where `scheme` can be either `pulsar` or `pulsar+ssl` and `port` is an optional field that
  defaults to `6650` and `6651`, respectively.
  
  ## Options
  
  - `:name` - used for name registration of the `:gen_statem` process
  - `:connection_timeout` - ...
  - `:auth` - ...
  - `:socket_opts` - ...
  """
  @spec start_link(atom(), String.t(), list(), list()) :: {:ok, pid()} | :ignore | {:error, term()}
  def start_link(module, host, opts \\ [], start_opts \\ []) do
    # TO-DO: handle different types of name registration (eg. :local, :global, :via)
    name = Keyword.get(opts, :name, nil)
    
    args = [
      name,
      host,
      Keyword.get(opts, :socket_opts, []),
      Keyword.get(opts, :conn_timeout, 5_000),
      Keyword.get(opts, :auth, [type: Pulsar.Auth.None, opts: []]),
      Keyword.get(opts, :actions, [])
    ]
        
    case name do
      nil ->
        :gen_statem.start_link(module, args, start_opts)
      name ->
        :gen_statem.start_link(name, module, args, start_opts)
    end
  end

  def socket_opts(conn, timeout \\ 5_000) do
    :gen_statem.call(conn, :socket_opts, timeout)
  end

  def conn_timeout(conn, timeout \\ 5_000) do
    :gen_statem.call(conn, :conn_timeout, timeout)
  end

  def auth(conn, timeout \\ 5_000) do
    :gen_statem.call(conn, :auth, timeout)
  end
  # TO-DO: stop/1
  
  def send_command(conn, command) do
    %Pulsar.Connection{
      socket_module: mod,
      socket: socket
    } = conn

    encoded_command = Pulsar.Protocol.encode(command)

    resp = apply(mod, :send, [socket, encoded_command])
    {resp, conn}
  end

  def reply(conn, request_id, reply) do
    conn
    |> Map.get(:requests)
    |> Map.get(request_id)
    |> :gen_statem.reply(reply)
  end
  
  defmacro __using__(_opts) do
    quote do  
      require Logger

      alias Pulsar.Config
      alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
      
      @behaviour :gen_statem

      @impl true
      def callback_mode, do: [:state_functions, :state_enter]

      @impl true
      def init([name, uri, socket_opts, conn_timeout, auth, post_actions]) do
        uri = URI.parse(uri)
        host = Map.get(uri, :host, "localhost")
        port = Map.get(uri, :port, 6650)
        socket_module =
          case Map.get(uri, :scheme, "pulsar") do
            "pulsar+ssl" -> :ssl
            "pulsar" -> :gen_tcp
          end

        conn = %Pulsar.Connection{
          name: name,
          host: host,
          port: port,
          socket_module: socket_module,
          socket_opts: socket_opts,
          conn_timeout: conn_timeout,
          auth: auth,
          actions: post_actions
        }
        actions = [{:next_event, :internal, :connect}]
        {:ok, :disconnected, conn, actions}
      end

      # connection state machine (:disconnected, :connected, :ready)

      def disconnected(:enter, :connected, conn) do
        wait = next_backoff(conn)
        Logger.error("Connection closed. Reconnecting in #{wait}ms.")
        actions = [{{:timeout, :reconnect}, wait, nil}]
        {:keep_state, %Pulsar.Connection{conn | socket: nil, prev_backoff: wait}, actions}
      end
      def disconnected(:enter, :disconnected, _conn) do
        :keep_state_and_data
      end
      def disconnected({:timeout, :reconnect}, _content, conn) do
        actions = [{:next_event, :internal, :connect}]
        {:keep_state, conn, actions}
      end
      def disconnected(:internal, :connect, conn) do
        %Pulsar.Connection{
          host: host,
          port: port,
          socket_module: mod,
          socket_opts: socket_opts,
          conn_timeout: conn_timeout
        } = conn
        host = String.to_charlist(host)

        case apply(mod, :connect, [host, port, socket_opts ++ [:binary, nodelay: true, active: true, keepalive: true], conn_timeout]) do
          {:ok, socket} ->
            Logger.debug("Connection succeeded")
            actions = [{:next_event, :internal, :handshake}]
            {:next_state, :connected, %Pulsar.Connection{conn| socket: socket, prev_backoff: 0}, actions}
          {:error, error} ->
            wait = next_backoff(conn)
            Logger.error("Connection failed: #{apply(mod, :format_error, [error])}. Reconnecting in #{wait}ms.")
            actions = [{{:timeout, :reconnect}, wait, nil}]
            {:keep_state, %Pulsar.Connection{conn| prev_backoff: wait}, actions}
        end
      end
      def disconnected({:call, from}, {:request, _request}, _conn) do
        actions = [{:reply, from, {:error, :disconnected}}]
        {:keep_state_and_data, actions}
      end
      def disconnected(event_type, event_data, _conn) do
        Logger.warning("Discarding #{inspect event_type} #{inspect event_data}")
        :keep_state_and_data
      end

      def connected(:enter, _old_state, _conn) do
        actions = [{{:timeout, :ping}, Config.ping_interval, nil}]
        {:keep_state_and_data, actions}
      end
      def connected(:info, {:tcp_closed, socket}, %Pulsar.Connection{socket: socket} = conn) do
        Logger.debug("Socket closed")
        {:next_state, :disconnected, conn}
      end
      def connected(:info, {:ssl_closed, socket}, %Pulsar.Connection{socket: socket} = conn) do
        Logger.debug("Socket closed")
        {:next_state, :disconnected, conn}
      end
      def connected(:info, {_, _socket, data}, conn) do
        {commands, conn} = handle_data(data, conn)
        actions = Enum.map(commands, &({:next_event, :internal, {:command, &1}}))
        {:keep_state, conn, actions}
      end
      def connected({:timeout, :ping}, _content, conn) do
        ping = %Binary.CommandPing{}

        case Pulsar.Connection.send_command(conn, ping) do
          {:ok, conn} ->
            actions = [{{:timeout, :ping}, Config.ping_interval, nil}]
            {:keep_state_and_data, actions}
          {{:error, _error}, conn} ->
            {:next_state, :disconnected, conn} 
        end
      end
      def connected(:internal, {:command, command}, conn) do
        Logger.debug "Received #{inspect command}"
        handle_command(command, conn)
      end
      def connected(:internal, :handshake, conn) do
        %Pulsar.Connection{auth: auth, actions: post_actions} = conn
        auth_method_name = auth_method_name(auth)
        auth_data = auth_data(auth)
        
        connect = %Binary.CommandConnect{
          client_version: Config.client_version,
          protocol_version: Config.protocol_version,
          auth_method_name: auth_method_name,
          auth_data: auth_data
        }
        case Pulsar.Connection.send_command(conn, connect) do
          {:ok, conn} ->
            actions = [{{:timeout, :ping}, Config.ping_interval, nil}| post_actions]
            {:keep_state_and_data, actions}
          {{:error, _error}, conn} ->
            {:next_state, :disconnected, conn}
        end
      end
      def connected({:call, from}, request, conn) do
        Logger.debug("Handling request #{inspect request}")

        case handle_call(from, request, conn) do
          {:ok, conn} ->
            {:keep_state, conn}
          {{:error, error}, conn} ->
            {:next_state, :disconnected, %Pulsar.Connection{conn| requests: %{}}}
        end
      end

      defp auth_method_name(type: type, opts: opts) do
        apply(type, :auth_method_name, [opts])
      end

      defp auth_data(type: type, opts: opts) do
        apply(type, :auth_data, [opts])
      end

      # TCP buffer
      # <<0, 0, 0, 9, 0, 0, 0, 5, 8, 19, 154, 1, 0, 0, 0, 0, 9, 0, 0, 0, 5, 8, 18, 146, 1, 0>>
      def handle_data(_data, _conn, _commands \\ [])
      def handle_data(<<>>, conn, commands) do
        {Enum.reverse(commands), conn}
      end
      def handle_data(
        data,
        %Pulsar.Connection{buffer: buffer, pending_bytes: pending_bytes} = conn,
        commands
      ) when pending_bytes > 0 do
        case data do
          <<missing_chunk::bytes-size(pending_bytes), rest::binary>> ->
            command = Pulsar.Protocol.decode(buffer <> missing_chunk)
            handle_data(rest, %Pulsar.Connection{conn | buffer: <<>>, pending_bytes: 0}, [command| commands])
          missing_chunk ->
            buffer = buffer <> missing_chunk
            pending_bytes = pending_bytes - byte_size(missing_chunk)
            handle_data(<<>>, %Pulsar.Connection{conn | buffer: buffer, pending_bytes: pending_bytes}, commands)
        end
      end
      def handle_data(
        <<total_size::32, _rest::binary>> = data,
        %Pulsar.Connection{buffer: buffer} = conn,
        commands
      ) when (total_size + 4) > byte_size(data) do
        # 1 chunked message
        {commands, %Pulsar.Connection{conn | buffer: buffer <> data, pending_bytes: (total_size + 4) - byte_size(data)}}
      end
      def handle_data(
        <<total_size::32, size::32, command::bytes-size(total_size - 4), rest::binary>> = data,
        conn,
        commands
      ) do
        command = Pulsar.Protocol.decode(<<total_size :: 32, size :: 32, command :: bytes-size(total_size - 4)>>)
        handle_data(rest, conn, [command| commands])
      end

      def handle_call(from, :socket_opts, conn) do
        %Pulsar.Connection{socket_opts: socket_opts} = conn
        reply = {:ok, socket_opts}
        :gen_statem.reply(from, reply)
        {:ok, conn}
      end
      def handle_call(from, :conn_timeout, conn) do
        %Pulsar.Connection{conn_timeout: conn_timeout} = conn
        reply = {:ok, conn_timeout}
        :gen_statem.reply(from, reply)
        {:ok, conn}
      end
      def handle_call(from, :auth, conn) do
        %Pulsar.Connection{auth: auth} = conn
        reply = {:ok, auth}
        :gen_statem.reply(from, reply)
        {:ok, conn}
      end
      
      def handle_command(%Binary.CommandPing{}, conn) do
        pong = %Binary.CommandPong{}

        # ignore return
        # if pong isn't successfully sent, the connection will be closed
        Pulsar.Connection.send_command(conn, pong)
        :keep_state_and_data
      end
      def handle_command(%Binary.CommandPong{}, _conn) do
        :keep_state_and_data
      end
      def handle_command(%Binary.CommandConnected{}, _conn) do
        :keep_state_and_data
      end
      # def handle_command(command, conn) do
      #   # TO-DO: Use with
      #   Logger.warning("Unhandled command #{inspect command}")
      #   %Pulsar.Connection{requests: requests} = conn
      #   case Map.get(command, :request_id) do
      #     nil ->
      #       :ok
      #     request_id ->
      #       case Map.get(requests, request_id) do
      #         nil ->
      #           Logger.warning("No requester found for #{inspect command}")
      #         from ->
      #           :gen_statem.reply(from, {:ok, command})
      #       end
      #   end
      #   :keep_state_and_data
      # end

      defp command_name(command) do
        command
        |> Map.get(:__struct__)
        |> Atom.to_string
        |> String.split(".")
        |> Enum.at(-1)
      end
      
      defp next_backoff(%Pulsar.Connection{prev_backoff: prev}) do
        next = round(prev * 2)
        next = min(next, Config.max_backoff)
        next + Enum.random(0..1000)
      end
    end
  end
end
