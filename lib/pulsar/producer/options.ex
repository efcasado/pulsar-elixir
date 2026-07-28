defmodule Pulsar.Producer.Options do
  @moduledoc false

  require Logger

  @schema [
    partitions: [
      type: :non_neg_integer,
      doc: false
    ],
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
      type: {:in, [:Shared, :Exclusive, :WaitForExclusive, :ExclusiveWithFencing]},
      default: :Shared,
      doc: """
      How the topic is shared with other producers. `:Shared` allows several,
      `:Exclusive` fails if one is already connected, `:WaitForExclusive` waits for
      it to disconnect, and `:ExclusiveWithFencing` evicts it.
      """
    ],
    compression: [
      type: {:in, [:NONE, :LZ4, :ZLIB, :SNAPPY, :ZSTD]},
      default: :NONE,
      doc: "Compression applied to the payload."
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
      doc: "Split payloads larger than `:max_message_size` across several messages."
    ],
    max_message_size: [
      type: :pos_integer,
      default: 5_242_880,
      doc: "Largest chunk to send, in bytes. Only used when chunking."
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
      `false` disables it.
      """
    ],
    max_restarts: [
      type: :non_neg_integer,
      default: 100,
      doc: "Restarts the producer group tolerates in a minute."
    ],
    max_seconds: [
      type: :pos_integer,
      default: 60,
      doc: "Window, in seconds, over which `:max_restarts` is counted."
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
  Validates producer options, warning about any the schema does not know.

  Unknown options will be rejected in the next major version.
  """
  @spec validate!(keyword()) :: keyword()
  def validate!(opts) do
    {known, unknown} = Keyword.split(opts, Keyword.keys(@schema))

    if unknown != [] do
      Logger.warning("Pulsar producer ignoring unknown options: #{inspect(Keyword.keys(unknown))}")
    end

    NimbleOptions.validate!(known, @schema)
  end
end
