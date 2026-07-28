defmodule Pulsar.Broker.Options do
  @moduledoc false

  @schema [
    auth: [
      type: :keyword_list,
      default: [type: Pulsar.Auth.None, opts: []],
      doc: "Authentication configuration, as `[type: module, opts: keyword]`."
    ],
    conn_timeout: [
      type: :timeout,
      default: 1_000,
      doc: """
      Milliseconds to wait for a connection to a broker. `:infinity`
      waits indefinitely, which leaves the broker process blocked in `connect` with no
      reconnect timer and no way to answer calls until the network gives up.
      """
    ],
    max_frame_size: [
      type: :pos_integer,
      default: Pulsar.Protocol.default_max_frame_size(),
      doc: """
      Largest frame accepted from this cluster, in bytes. Raise it to match a broker
      configured with a larger `maxMessageSize`.
      """
    ],
    ping_interval: [
      type: :pos_integer,
      default: 60_000,
      doc: "Milliseconds between keepalive pings to each broker in this cluster."
    ],
    cleanup_interval: [
      type: :pos_integer,
      default: 30_000,
      doc: "Milliseconds between sweeps for requests that never got a response."
    ],
    request_timeout: [
      type: :pos_integer,
      default: 60_000,
      doc: "Milliseconds after which a request without a response is failed."
    ],
    max_backoff: [
      type: :pos_integer,
      default: 30_000,
      doc: "Longest wait between attempts to reconnect to a broker, in milliseconds."
    ],
    socket_opts: [
      type: {:list, :any},
      doc: """
      Options passed to `:gen_tcp.connect/4` or `:ssl.connect/4`. Defaults to verifying
      the broker's certificate against the CA bundle from `:castore`. Not a keyword list:
      bare atoms such as `:inet6` and tuples such as `{:raw, level, opt, value}` are
      valid entries.
      """
    ]
  ]

  @spec schema() :: keyword()
  def schema, do: @schema

  @spec docs() :: String.t()
  def docs, do: NimbleOptions.docs(@schema)

  @spec keys() :: [atom()]
  def keys, do: Keyword.keys(@schema)

  @doc """
  Fills in the defaults for the connection tunables, leaving every other option alone.

  Anything outside the schema — `:url` and `:name`, and whatever else a caller threads
  through to `Pulsar.Broker.start_link/2` — passes through untouched, so a broker started
  on its own is configured the same as one a client starts.
  """
  @spec validate!(keyword()) :: keyword()
  def validate!(opts) do
    {known, rest} = Keyword.split(opts, keys())

    Keyword.merge(rest, default_socket_opts(NimbleOptions.validate!(known, @schema)))
  end

  defp default_socket_opts(opts) do
    Keyword.put_new(opts, :socket_opts, verify: :verify_peer, cacertfile: CAStore.file_path())
  end
end
