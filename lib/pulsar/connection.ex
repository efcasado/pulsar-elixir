defmodule Pulsar.Connection do
  # https://andrealeopardi.com/posts/connection-managers-with-gen-statem/
  # https://www.erlang.org/doc/system/statem.html
  # https://www.erlang.org/doc/apps/stdlib/gen_statem.html
  @doc false

  require Logger
  
  @behaviour :gen_statem

  defstruct host: "",
    port: 6650,
    socket_module: :gen_tcp,
    socket: nil,
    prev_backoff: 0,
    socket_opts: [],
    conn_timeout: 5_000,
    auth: [type: :none, opts: []]

  @type t :: %__MODULE__{
    host: String.t(),
    port: integer(),
    socket_module: :gen_tcp | :ssl,
    socket: :gen_tcp.socket() | :ssl.sslsocket(),
    prev_backoff: 0,
    socket_opts: list(),
    conn_timeout: integer(),
    auth: list()
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

    data = %__MODULE__{
      host: host,
      port: port,
      socket_module: socket_module,
      socket_opts: socket_opts,
      conn_timeout: conn_timeout,
      auth: auth
    }
    actions = [{:next_event, :internal, :connect}]
    {:ok, :disconnected, data, actions}
  end

  def disconnected(:enter, :connected, data) do
    wait = next_backoff(data)
    Logger.error("Connection closed. Reconnecting in #{wait}ms.")
    actions = [{{:timeout, :reconnect}, wait, nil}]
    {:keep_state, %__MODULE__{data | socket: nil, prev_backoff: wait}, actions}
  end
  def disconnected(:enter, :disconnected, _data) do
    :keep_state_and_data
  end
  def disconnected({:timeout, :reconnect}, _content, data) do
    actions = [{:next_event, :internal, :connect}]
    {:keep_state, data, actions}
  end
  def disconnected(:internal, :connect, data) do
    %__MODULE__{
      host: host,
      port: port,
      socket_module: mod,
      socket_opts: socket_opts,
      conn_timeout: conn_timeout
    } = data
    host = String.to_charlist(host)

    case apply(mod, :connect, [host, port, socket_opts ++ [:binary, nodelay: true, active: true, keepalive: true], conn_timeout]) do
      {:ok, socket} ->
        Logger.debug("Connection succeeded")
        actions = [{:next_event, :internal, :handshake}]
        {:next_state, :connected, %__MODULE__{data| socket: socket, prev_backoff: 0}, actions}
      {:error, error} ->
        wait = next_backoff(data)
        Logger.error("Connection failed: #{apply(mod, :format_error, [error])}. Reconnecting in #{wait}ms.")
        actions = [{{:timeout, :reconnect}, wait, nil}]
        {:keep_state, %__MODULE__{data| prev_backoff: wait}, actions}
    end
  end
  def disconnected({:call, from}, {:request, _request}, _data) do
    actions = [{:reply, from, {:error, :disconnected}}]
    {:keep_state_and_data, actions}
  end

  def connected(:enter, _old_state, _data) do
    actions = [{{:timeout, :ping}, Application.get_env(:pulsar, :ping_interval, 60_000), nil}]
    {:keep_state_and_data, actions}
  end
  def connected(:info, {:tcp_closed, socket}, %__MODULE__{socket: socket} = data) do
    {:next_state, :disconnected, data}
  end
  def connected(:info, {:ssl_closed, socket}, %__MODULE__{socket: socket} = data) do
    {:next_state, :disconnected, data}
  end
  def connected(:info, {_, socket, message}, data) do
    command = Pulsar.Framing.decode(message)
    Logger.debug "Received #{inspect command}"
    handle_command(command, data)
  end
  def connected({:timeout, :ping}, _content, data) do
    %__MODULE__{
      socket_module: mod,
      socket: socket
    } = data

    command = Pulsar.Framing.encode(%Pulsar.Proto.CommandPing{})
    case apply(mod, :send, [socket, command]) do
      :ok ->
        Logger.debug("Sent ping")
        actions = [{{:timeout, :ping}, Application.get_env(:pulsar, :ping_interval, 60_000), nil}]
        {:keep_state_and_data, actions}
      {:error, error} ->
        Logger.error("Failed to send ping: #{apply(mod, :format_error, [error])}.")
        {:next_state, :disconnected, data}
    end
  end
  def connected(:internal, :handshake, data) do
    client_version = Application.get_env(:pulsar, :client_version, "Pulsar Elixir Client #{Mix.Project.config[:version]}")
    protocol_version = Application.get_env(:pulsar, :protocol_version, protocol_version())
    
    command = Pulsar.Framing.encode(%Pulsar.Proto.CommandConnect{
          client_version: client_version,
          protocol_version: protocol_version
    })
    case send_command(data, command) do
      :ok ->
        actions = [{{:timeout, :ping}, Application.get_env(:pulsar, :ping_interval, 60_000), nil}]
        {:keep_state_and_data, actions}
      {:error, error} ->
        {:next_state, :disconnected, data}
    end
  end
  def connected({:call, from}, {:request, _request}, data) do
    Logger.debug("Handling request")
    :gen_statem.reply(from, :ok)
    {:keep_state, data}
  end

  defp handle_command(%Pulsar.Proto.CommandPing{}, data) do
    pong =
      %Pulsar.Proto.CommandPong{}
      |> Pulsar.Framing.encode
    # ignore return
    # if pong isn't successfully sent, the connection will be closed
    send_command(data, pong)
    :keep_state_and_data
  end
  defp handle_command(command, _data) do
    Logger.warning("Unhandled command #{inspect command}")
    :keep_state_and_data
  end

  defp send_command(conn, command) do
    %__MODULE__{
      socket_module: mod,
      socket: socket
    } = conn
    case apply(mod, :send, [socket, command]) do
      :ok ->
        Logger.debug("Successfully sent message")
        :ok
      {:error, error} ->
        Logger.error("Failed to send message: #{apply(mod, :format_error, [error])}.")
        {:error, error}
    end
  end
  
  defp next_backoff(%__MODULE__{prev_backoff: prev}) do
    next = round(prev * 2)
    next = min(next, Application.get_env(:pulsar, :max_backoff, 60_000))
    next + Enum.random(0..1000)
  end

  def protocol_version() do
    %Pulsar.Proto.ProtocolVersion{}
    |> Map.keys
    |> Enum.map(&(Atom.to_string(&1)))
    |> Enum.reduce([], fn(<<"v", version::binary>>, acc) -> [String.to_integer(version)| acc]; (_, acc) -> acc end)
    |> Enum.sort
    |> Enum.at(-1)
  end
end
