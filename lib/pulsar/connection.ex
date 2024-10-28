defmodule Pulsar.Connection do
  # https://andrealeopardi.com/posts/connection-managers-with-gen-statem/
  # https://www.erlang.org/doc/system/statem.html
  # https://www.erlang.org/doc/apps/stdlib/gen_statem.html
  @doc false

  require Logger

  alias Pulsar.Config
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  
  @behaviour :gen_statem

  defstruct host: "",
    port: 6650,
    socket_module: :gen_tcp,
    socket: nil,
    prev_backoff: 0,
    socket_opts: [],
    conn_timeout: 5_000,
    auth: [type: :none, opts: []],
    buffer: <<>>,
    pending_bytes: 0

  @type t :: %__MODULE__{
    host: String.t(),
    port: integer(),
    socket_module: :gen_tcp | :ssl,
    socket: :gen_tcp.socket() | :ssl.sslsocket(),
    prev_backoff: 0,
    socket_opts: list(),
    conn_timeout: integer(),
    auth: list(),
    buffer: binary(),
    pending_bytes: integer()
  }

  def start_link(host, opts \\ []) do
    socket_opts = Keyword.get(opts, :socket_opts, [])
    conn_timeout = Keyword.get(opts, :conn_timeout, 5_000)
    auth = Keyword.get(opts, :auth, :none)

    :gen_statem.start_link(__MODULE__, [host, socket_opts, conn_timeout, auth], [])
  end

  ## State Machine
  
  @impl true
  def callback_mode, do: [:state_functions, :state_enter]

  @impl true
  def init([uri, socket_opts, conn_timeout, auth]) do
    uri = URI.parse(uri)
    host = Map.get(uri, :host, "localhost")
    port = Map.get(uri, :port, 6650)
    socket_module =
      case Map.get(uri, :scheme, "pulsar") do
        "pulsar+ssl" -> :ssl
        "pulsar" -> :gen_tcp
      end

    conn = %__MODULE__{
      host: host,
      port: port,
      socket_module: socket_module,
      socket_opts: socket_opts,
      conn_timeout: conn_timeout,
      auth: auth
    }
    actions = [{:next_event, :internal, :connect}]
    {:ok, :disconnected, conn, actions}
  end

  def disconnected(:enter, :connected, conn) do
    wait = next_backoff(conn)
    Logger.error("Connection closed. Reconnecting in #{wait}ms.")
    actions = [{{:timeout, :reconnect}, wait, nil}]
    {:keep_state, %__MODULE__{conn | socket: nil, prev_backoff: wait}, actions}
  end
  def disconnected(:enter, :disconnected, _conn) do
    :keep_state_and_data
  end
  def disconnected({:timeout, :reconnect}, _content, conn) do
    actions = [{:next_event, :internal, :connect}]
    {:keep_state, conn, actions}
  end
  def disconnected(:internal, :connect, conn) do
    %__MODULE__{
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
        {:next_state, :connected, %__MODULE__{conn| socket: socket, prev_backoff: 0}, actions}
      {:error, error} ->
        wait = next_backoff(conn)
        Logger.error("Connection failed: #{apply(mod, :format_error, [error])}. Reconnecting in #{wait}ms.")
        actions = [{{:timeout, :reconnect}, wait, nil}]
        {:keep_state, %__MODULE__{conn| prev_backoff: wait}, actions}
    end
  end
  def disconnected({:call, from}, {:request, _request}, _conn) do
    actions = [{:reply, from, {:error, :disconnected}}]
    {:keep_state_and_data, actions}
  end

  def connected(:enter, _old_state, _conn) do
    actions = [{{:timeout, :ping}, Config.ping_interval, nil}]
    {:keep_state_and_data, actions}
  end
  def connected(:info, {:tcp_closed, socket}, %__MODULE__{socket: socket} = conn) do
    {:next_state, :disconnected, conn}
  end
  def connected(:info, {:ssl_closed, socket}, %__MODULE__{socket: socket} = conn) do
    {:next_state, :disconnected, conn}
  end
  def connected(:info, {_, _socket, data}, conn) do
    {commands, conn} = handle_data(data, conn)
    actions = Enum.map(commands, &({:next_event, :internal, {:command, &1}}))
    {:keep_state, conn, actions}
  end
  def connected({:timeout, :ping}, _content, conn) do
    ping = %Binary.CommandPing{}

    case send_command(conn, ping) do
      :ok ->
        actions = [{{:timeout, :ping}, Config.ping_interval, nil}]
        {:keep_state_and_data, actions}
      {:error, _error} ->
        {:next_state, :disconnected, conn} 
    end
  end
  def connected(:internal, {:command, command}, conn) do
    Logger.debug "Received #{inspect command}"
    handle_command(command, conn)
  end
  def connected(:internal, :handshake, conn) do
    connect = %Binary.CommandConnect{
          client_version: Config.client_version,
          protocol_version: Config.protocol_version
    }
    case send_command(conn, connect) do
      :ok ->
        actions = [{{:timeout, :ping}, Config.ping_interval, nil}]
        {:keep_state_and_data, actions}
      {:error, _error} ->
        {:next_state, :disconnected, conn}
    end
  end
  def connected({:call, from}, {:request, _request}, conn) do
    Logger.debug("Handling request")
    :gen_statem.reply(from, :ok)
    {:keep_state, conn}
  end

  # TCP buffer
  # <<0, 0, 0, 9, 0, 0, 0, 5, 8, 19, 154, 1, 0, 0, 0, 0, 9, 0, 0, 0, 5, 8, 18, 146, 1, 0>>
  def handle_data(_data, _conn, _commands \\ [])
  def handle_data(<<>>, conn, commands) do
    {Enum.reverse(commands), conn}
  end
  def handle_data(
    data,
    %__MODULE__{buffer: buffer, pending_bytes: pending_bytes} = conn,
    commands
  ) when pending_bytes > 0 do
    case data do
      <<missing_chunk::bytes-size(pending_bytes), rest::binary>> ->
        command = Pulsar.Protocol.decode(buffer <> missing_chunk)
        handle_data(rest, %__MODULE__{conn | buffer: <<>>, pending_bytes: 0}, [command| commands])
      missing_chunk ->
        buffer = buffer <> missing_chunk
        pending_bytes = pending_bytes - byte_size(missing_chunk)
        handle_data(<<>>, %__MODULE__{conn | buffer: buffer, pending_bytes: pending_bytes}, commands)
    end
  end
  def handle_data(
    <<total_size::32, size::32, command::bytes-size(size), rest::binary>>,
    conn,
    commands
  ) do
    command = Pulsar.Protocol.decode(<<total_size :: 32, size :: 32, command :: bytes-size(size)>>)
    handle_data(rest, conn, [command| commands])
  end
  def handle_data(
    <<total_size::32, _rest::binary>> = data,
    %__MODULE__{buffer: buffer} = conn,
    commands
  ) when (total_size + 4) > byte_size(data) do
    # 1 chunked message
    {commands, %__MODULE__{conn | buffer: buffer <> data, pending_bytes: (total_size + 4) - byte_size(data)}}
  end

  defp handle_command(%Binary.CommandPing{}, conn) do
    pong = %Binary.CommandPong{}

    # ignore return
    # if pong isn't successfully sent, the connection will be closed
    send_command(conn, pong)
    :keep_state_and_data
  end
  defp handle_command(command, _conn) do
    Logger.warning("Unhandled command #{inspect command}")
    :keep_state_and_data
  end

  defp send_command(conn, command) do
    %__MODULE__{
      socket_module: mod,
      socket: socket
    } = conn

    encoded_command = Pulsar.Protocol.encode(command)

    case apply(mod, :send, [socket, encoded_command]) do
      :ok ->
        Logger.debug("Successfully sent #{command_name(command)}")
        :ok
      {:error, error} ->
        Logger.error("Failed to send #{command_name(command)}: #{apply(mod, :format_error, [error])}.")
        {:error, error}
    end
  end

  defp command_name(command) do
    command
    |> Map.get(:__struct__)
    |> Atom.to_string
    |> String.split(".")
    |> Enum.at(-1)
  end
  
  defp next_backoff(%__MODULE__{prev_backoff: prev}) do
    next = round(prev * 2)
    next = min(next, Config.max_backoff)
    next + Enum.random(0..1000)
  end

  def protocol_version() do
    %Binary.ProtocolVersion{}
    |> Map.keys
    |> Enum.map(&(Atom.to_string(&1)))
    |> Enum.reduce([], fn(<<"v", version::binary>>, acc) -> [String.to_integer(version)| acc]; (_, acc) -> acc end)
    |> Enum.sort
    |> Enum.at(-1)
  end
end
