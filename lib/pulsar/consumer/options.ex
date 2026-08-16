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
    flow_policy: [
      type: {:custom, __MODULE__, :validate_flow_policy, []},
      default: :auto,
      doc: """
      What refills permits once the consumer is running. `:auto` grants `:flow_refill` whenever
      outstanding permits reach `:flow_threshold`. A `{module, function, args}` decides for
      itself; see `Pulsar.Consumer` for what it is passed, what it may answer, and what it
      cannot do.
      """
    ],
    flow_initial: [
      type: :non_neg_integer,
      default: 100,
      doc: """
      Permits granted to the broker when a consumer subscribes, under either policy. Every
      worker instance grants them, so a replacement comes back with its window rather than
      waiting to be given one.
      """
    ],
    flow_threshold: [
      type: :non_neg_integer,
      default: 50,
      doc: "Outstanding permits at which the default `:auto` policy requests more."
    ],
    flow_refill: [
      type: :non_neg_integer,
      default: 50,
      doc: "Permits requested by each refill the default `:auto` policy makes."
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
      doc: """
      Read only the latest value per key from a compacted topic. When enabled, messages
      compaction has replaced are filtered out of a batch rather than delivered. When disabled,
      the consumer can receive the original history, including superseded values.
      """
    ],
    force_create_topic: [
      type: :boolean,
      default: true,
      doc: "Create the topic if it does not exist."
    ],
    ack_type: [
      type: {:in, [:individual, :cumulative]},
      default: :individual,
      doc: """
      What an acknowledgement covers. `:individual` acknowledges the messages it names.
      `:cumulative` acknowledges everything up to and including them, sending only the
      furthest one, which moves the subscription cursor in one command instead of one per
      message.

      **Only `:exclusive` and `:failover` subscriptions may ack cumulatively**, since a shared
      subscription has no single cursor to move; the broker rejects it otherwise, so this is
      refused at startup instead.

      Cumulative acknowledgement covers messages that were never acked, and a nacked message
      the cursor passes is acknowledged along with the rest. It also acknowledges whole
      entries, so acking one message of a batch acknowledges the messages batched with it;
      `:batch_index_ack_enabled` does not narrow that.
      """
    ],
    batch_index_ack_enabled: [
      type: :boolean,
      default: false,
      doc: """
      Tell the broker which messages of a batch an ack was for, so that a nack redelivers only
      the rest of the entry instead of all of it. Costs one ack command per message rather
      than one per entry.

      **Requires `acknowledgmentAtBatchIndexLevelEnabled=true` on the broker.** Without it the
      broker ignores the set and acknowledges the whole entry, losing the messages batched
      alongside the acked one. Nothing in the protocol reports the setting, so this cannot be
      detected: Pulsar's shipped `broker.conf` enables it, `standalone.conf` does not.
      """
    ],
    redelivery_interval: [
      type: :pos_integer,
      doc: """
      Milliseconds between redelivery requests for negatively acknowledged messages.
      Absent by default, in which case they are not redelivered and a nacked message from a
      batch leaves its entry unacknowledged until it is acked or the consumer restarts.
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
        ],
        producer: [
          type: {:custom, Pulsar.Consumer.DeadLetter, :validate_producer_options, []},
          doc: """
          Options for the producer that publishes to the dead letter topic, such as
          `:compression` or `:batch_enabled`. Takes `Pulsar.Producer`'s defaults otherwise.
          Its `:topic`, `:client` and `:name` come from the consumer and cannot be set here.
          """
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

      A batch diverts as one entry, so if any message in it fails to publish the entry is
      redelivered and the rest are diverted a second time. Deduplicate on `ORIGIN_MESSAGE_ID`
      if that matters.

      Diverting replaces delivery rather than accompanying it, so neither
      `c:Pulsar.Consumer.Callback.handle_message/2` nor
      `c:Pulsar.Consumer.Callback.handle_invalid_message/2` is called for a message that
      reaches the threshold.
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
  def validate!(opts), do: opts |> NimbleOptions.validate!(@schema) |> validate_flow!() |> validate_ack_type!()

  defp validate_flow!(opts) do
    if Keyword.fetch!(opts, :flow_policy) == :auto and Keyword.fetch!(opts, :flow_initial) == 0 do
      raise ArgumentError,
            "flow_policy: :auto with flow_initial: 0 never receives a message: the broker is " <>
              "granted nothing, so no delivery arrives to trigger a refill. Set a positive " <>
              ":flow_initial, or a {module, function, args} policy if permits will come from " <>
              "Pulsar.Consumer.send_flow/3."
    end

    opts
  end

  @cumulative_subscription_types [:exclusive, :failover]

  defp validate_ack_type!(opts) do
    subscription_type = Keyword.fetch!(opts, :subscription_type)

    if Keyword.fetch!(opts, :ack_type) == :cumulative and subscription_type not in @cumulative_subscription_types do
      raise ArgumentError,
            "ack_type: :cumulative is not supported on a #{inspect(subscription_type)} subscription, which " <>
              "has no single cursor to move: the broker rejects the acknowledgement. Use " <>
              ":exclusive or :failover, or ack individually."
    end

    opts
  end

  @doc false
  @spec validate_flow_policy(term()) :: {:ok, term()} | {:error, String.t()}
  def validate_flow_policy(:auto), do: {:ok, :auto}

  # Checked here rather than at the first delivery, where an unloadable policy would take the
  # consumer down once per redelivery.
  def validate_flow_policy({module, function, args} = policy)
      when is_atom(module) and is_atom(function) and is_list(args) do
    if Code.ensure_loaded?(module) and function_exported?(module, function, length(args) + 1) do
      {:ok, policy}
    else
      {:error, "expected #{inspect(module)} to export #{function}/#{length(args) + 1}"}
    end
  end

  def validate_flow_policy(other) do
    {:error, "expected :auto or a {module, function, args} tuple, got: #{inspect(other)}"}
  end
end
