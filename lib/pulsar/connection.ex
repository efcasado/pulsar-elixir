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

    case apply(mod, :connect, [host, port, socket_opts, conn_timeout]) do
      {:ok, socket} ->
        Logger.debug("Connection succeeded")
        {:next_state, :connected, %__MODULE__{data| socket: socket, prev_backoff: 0}}
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

  def connected(:enter, _old_state, _data), do: :keep_state_and_data
  def connected(:info, {:tcp_closed, socket}, %__MODULE__{socket: socket} = data) do
    {:next_state, :disconnected, data}
  end
  def connected(:info, {:ssl_closed, socket}, %__MODULE__{socket: socket} = data) do
    {:next_state, :disconnected, data}
  end
  def connected({:call, from}, {:request, _request}, data) do
    Logger.debug("Handling request")
    :gen_statem.reply(from, :ok)
    {:keep_state, data}
  end

  defp next_backoff(%__MODULE__{prev_backoff: prev}) do
    next = round(prev * 2)
    next = min(next, Application.get_env(:pulsar, :max_backoff, 60_000))
    next + Enum.random(0..1000)
  end
end
