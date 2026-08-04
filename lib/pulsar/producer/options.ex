defmodule Pulsar.Producer.Options do
  @moduledoc false

  alias Pulsar.Hash

  @schema [
    topic: [
      type: :string,
      required: true,
      doc: "Topic to publish to."
    ],
    client: [
      type: :atom,
      default: :default,
      doc: "Client the producer belongs to."
    ],
    name: [
      type: {:or, [:string, :atom]},
      doc: "Name the producer is registered under. Defaults to `\"<topic>-producer\"`."
    ],
    producer_count: [
      type: :pos_integer,
      default: 1,
      doc: "Number of producer processes to start for the topic, or for each partition."
    ],
    access_mode: [
      type: {:in, [:shared, :exclusive, :wait_for_exclusive, :exclusive_with_fencing]},
      default: :shared,
      doc: """
      How the topic is shared with other producers. `:shared` allows several,
      `:exclusive` fails if one is already connected, `:wait_for_exclusive` waits for
      it to disconnect, and `:exclusive_with_fencing` evicts it.
      """
    ],
    compression: [
      type: {:in, [:none, :lz4, :zlib, :snappy, :zstd]},
      default: :none,
      doc: "Compression applied to the payload."
    ],
    hashing_scheme: [
      type: {:in, Hash.schemes()},
      default: Hash.default_scheme(),
      doc: """
      How a message's `:partition_key` is hashed to pick a partition of a partitioned topic.

      `:murmur3_32` is implemented identically by every Pulsar client, so keys co-locate with
      producers in other languages. `:java_string_hash` matches what the Java and Go clients
      use when left at their own default.

      `:phash2_legacy` is the non-standard `:erlang.phash2/2` routing used before 3.0. No
      other client can reproduce it, so it is only for upgrading a partitioned topic without
      remapping keys mid-flight: keeping a key's existing partition preserves its ordering,
      which switching schemes breaks until the old partition drains. Prefer draining and
      moving to `:murmur3_32`.
      """
    ],
    batch_enabled: [
      type: :boolean,
      default: false,
      doc: "Collect messages and publish them as one batch."
    ],
    batch_size: [
      type: :pos_integer,
      default: 100,
      doc: "Messages to collect before flushing a batch. Only used when batching."
    ],
    flush_interval: [
      type: :pos_integer,
      default: 10,
      doc: "Milliseconds between batch flushes. Only used when batching."
    ],
    chunking_enabled: [
      type: :boolean,
      default: false,
      doc: """
      Split payloads larger than `:max_message_size` across several messages. Cannot be
      combined with `:batch_enabled`. A payload is compressed before it is measured, so
      one that compresses below the limit is sent whole.
      """
    ],
    max_message_size: [
      type: :pos_integer,
      default: 5_242_880,
      doc: """
      Largest chunk payload to send, in bytes. Only used when chunking. Capped by the limit
      the broker advertises when the producer connects, minus the metadata each chunk carries.
      """
    ],
    schema: [
      type: :keyword_list,
      doc: """
      Schema to register with the topic, as `[type: atom, definition: term]`. See
      `Pulsar.Schema`.
      """
    ],
    partition_discovery_interval_ms: [
      type: {:or, [:pos_integer, {:in, [false]}]},
      default: 60_000,
      doc: """
      For a partitioned topic, how often to look for partitions added since startup.
      `false` disables later metadata checks, but not initial topic discovery or local
      recovery of groups that have stopped.
      """
    ],
    startup_delay_ms: [
      type: :non_neg_integer,
      default: 0,
      doc:
        "Delay before a producer connects. A broker that is not connected yet is retried, so this is only needed to stagger a large number of restarts."
    ],
    startup_jitter_ms: [
      type: :non_neg_integer,
      default: 0,
      doc: "Random extra delay on top of `:startup_delay_ms`, to spread out restarts."
    ]
  ]

  @spec schema() :: keyword()
  def schema, do: @schema

  @spec docs() :: String.t()
  def docs, do: NimbleOptions.docs(@schema)

  @doc """
  Validates producer options.
  """
  @spec validate!(keyword()) :: keyword()
  def validate!(opts) do
    case opts |> NimbleOptions.validate!(@schema) |> validate_chunking() do
      {:ok, opts} -> opts
      {:error, error} -> raise error
    end
  end

  @spec validate(keyword()) :: {:ok, keyword()} | {:error, NimbleOptions.ValidationError.t()}
  def validate(opts) do
    with {:ok, opts} <- NimbleOptions.validate(opts, @schema) do
      validate_chunking(opts)
    end
  end

  # A batch is one entry holding many messages and a chunked message is one message spread
  # over many entries, so a producer cannot do both at once. Java refuses the pair as well.
  defp validate_chunking(opts) do
    if opts[:batch_enabled] and opts[:chunking_enabled] do
      {:error,
       %NimbleOptions.ValidationError{
         key: :chunking_enabled,
         value: true,
         message: "invalid value for :chunking_enabled option: cannot be enabled together with :batch_enabled"
       }}
    else
      {:ok, opts}
    end
  end
end
