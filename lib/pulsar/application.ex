defmodule Pulsar.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    connections = Application.get_env(:pulsar, :connections, [])

    children =
      connections
      |> Enum.map(fn({name, opts}) ->
        host = Keyword.fetch!(opts, :host)
        socket_opts = Keyword.get(opts, :socket_opts, [])
        conn_timeout = Keyword.get(opts, :conn_timeout, 5_000)
        auth = Keyword.get(opts, :auth, [type: Pulsar.Auth.None, opts: []])

        args = [name, host, [socket_opts: socket_opts, timeout: conn_timeout, auth: auth]]
        %{id: name, start: {Pulsar.Connection, :start_link, args}}
      end)

    opts = [strategy: :one_for_one, name: Pulsar.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
