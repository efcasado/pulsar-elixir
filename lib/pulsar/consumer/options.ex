defmodule Pulsar.Consumer.Options do
  @moduledoc false

  @schema [
    topic: [
      type: :string,
      required: true,
      doc: "Topic to subscribe to."
    ],
    subscription_name: [
      type: :string,
      required: true,
      doc: "Name of the subscription."
    ],
    callback_module: [
      type: :atom,
      required: true,
      doc: "Module implementing `Pulsar.Consumer.Callback`."
    ],
    client: [
      type: :atom,
      default: :default,
      doc: "Client the consumer belongs to."
    ],
    name: [
      type: {:or, [:string, :atom]},
      doc: """
      Name the consumer group is registered under. Defaults to
      `"<topic>-<subscription_name>"`. Consumers within it are named after the group
      and their index on the broker.
      """
    ],
    subscription_type: [
      type: {:in, [:exclusive, :shared, :failover, :key_shared]},
      default: :shared,
      doc: "How the subscription is shared between consumers."
    ],
    consumer_count: [
      type: :pos_integer,
      default: 1,
      doc: "Number of consumer processes to start for the topic, or for each partition."
    ],
    init_args: [
      type: :any,
      default: [],
      doc: "Passed to the callback module's `init/1`."
    ],
    flow_initial: [
      type: :non_neg_integer,
      default: 100,
      doc: """
      Permits granted to the broker on subscribe. `0` disables automatic flow control,
      leaving it to `Pulsar.Consumer.send_flow/2`. Permits belong to a worker instance, so
      replacement workers also start with `0` and must be granted permits again.
      """
    ],
    flow_threshold: [
      type: :non_neg_integer,
      default: 50,
      doc: "Outstanding permits at which more are requested. Ignored when `:flow_initial` is 0."
    ],
    flow_refill: [
      type: :non_neg_integer,
      default: 50,
      doc: "Permits requested on each refill. Ignored when `:flow_initial` is 0."
    ],
    initial_position: [
      type: {:in, [:earliest, :latest]},
      default: :latest,
      doc: "Where a new subscription starts reading."
    ],
    start_message_id: [
      type: {:tuple, [:non_neg_integer, :non_neg_integer]},
      doc: "Seek to a `{ledger_id, entry_id}` before reading."
    ],
    start_timestamp: [
      type: :non_neg_integer,
      doc: "Seek to a publish time, in milliseconds since the epoch, before reading."
    ],
    durable: [
      type: :boolean,
      default: true,
      doc: "Whether the broker persists the subscription's position."
    ],
    read_compacted: [
      type: :boolean,
      default: false,
      doc: "Read only the latest value per key from a compacted topic."
    ],
    force_create_topic: [
      type: :boolean,
      default: true,
      doc: "Create the topic if it does not exist."
    ],
    redelivery_interval: [
      type: :pos_integer,
      doc: """
      Milliseconds between redelivery requests for negatively acknowledged messages.
      Absent by default, in which case they are not redelivered.
      """
    ],
    dead_letter_policy: [
      type: :keyword_list,
      keys: [
        max_redelivery: [
          type: :pos_integer,
          required: true,
          doc: "Deliveries to attempt before diverting the message."
        ],
        topic: [
          type: :string,
          doc: "Topic to divert to. Defaults to `\"<topic>-<subscription>-DLQ\"`."
        ]
      ],
      doc: """
      Diverts a message to another topic once it has been redelivered too often.
      Omit it entirely for no dead letter topic.

      The producer this needs runs under the consumer, so it restarts on its own and a dead
      letter topic that is unavailable leaves the message nacked rather than disturbing the
      subscription. A partitioned consumer diverts every partition into one dead letter topic.

      A diverted message keeps its key, ordering key, properties and event time, and gains
      `REAL_TOPIC` and `ORIGIN_MESSAGE_ID` properties naming where it came from.

      Diverting replaces delivery rather than accompanying it, so neither `c:handle_message/2`
      nor `c:handle_invalid_message/2` is called for a message that reaches the threshold.
      """
    ],
    max_pending_chunked_messages: [
      type: :pos_integer,
      default: 10,
      doc: "Incomplete chunked messages to hold before evicting the oldest."
    ],
    expire_incomplete_chunked_message_after: [
      type: :pos_integer,
      default: 60_000,
      doc: "Milliseconds before an incomplete chunked message is given up on."
    ],
    chunk_cleanup_interval: [
      type: {:or, [:pos_integer, {:in, [false, nil]}]},
      default: 30_000,
      doc: """
      Milliseconds between sweeps for expired chunked messages. `false` disables the
      sweep, leaving incomplete chunks to accumulate until the consumer restarts;
      `nil` is accepted as an alias for `false`.
      """
    ],
    schema: [
      type: :keyword_list,
      doc: """
      Schema to register with the subscription, as `[type: atom, definition: term]`.
      See `Pulsar.Schema`.
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
        "Delay before a consumer subscribes. A broker that is not connected yet is retried, so this is only needed to stagger a large number of restarts."
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
  Validates consumer options.
  """
  @spec validate!(keyword()) :: keyword()
  def validate!(opts), do: NimbleOptions.validate!(opts, @schema)
end
