defmodule Pulsar.Consumer.DeadLetter do
  @moduledoc false

  # The dead letter producer belongs to the consumer that diverts into it, so it hangs off that
  # consumer's Pulsar.Topology root rather than the client's producer branch. It is a producer
  # like any other below that point: its own topology, its own discovery, its own partitions.

  alias Pulsar.Message
  alias Pulsar.Producer
  alias Pulsar.Producer.Options, as: ProducerOptions
  alias Pulsar.Producer.Worker, as: ProducerWorker

  # Read by a consumer of the dead letter topic to trace a message back to where it failed.
  # The names match the Java client's, so both ends agree on them.
  @real_topic_property "REAL_TOPIC"
  @origin_message_id_property "ORIGIN_MESSAGE_ID"

  @doc """
  Tells a consumer's workers where to find the dead letter producer.

  Called with the logical consumer's options, before `Pulsar.Topology.Group` rewrites `:name`
  and `:topic` per partition and per worker, so what every worker inherits describes the whole
  consumer rather than the one partition it happens to hold.

  What they inherit is the topology root, not a pid or a registered name. The producer is a
  child of that root, so resolving through it is what makes a restart of the producer invisible
  to the workers, and keeps them off a registry owned by a branch that restarts separately.
  """
  @spec annotate(keyword(), pid()) :: keyword()
  def annotate(opts, root) do
    case topic(opts) do
      nil -> opts
      _topic -> Keyword.put(opts, :dead_letter_root, root)
    end
  end

  @doc """
  The dead letter producer to run under a consumer's topology, or none when it has no policy.
  """
  @spec child_specs(keyword()) :: [Supervisor.child_spec()]
  def child_specs(opts) do
    case topic(opts) do
      nil ->
        []

      topic ->
        # Validated here rather than started through Pulsar.Producer, which registers what it
        # starts in the client's producer registry. This one is reached through the consumer
        # that owns it, so registering would only couple it to a branch it does not belong to.
        # Everything below the root is an ordinary producer, defaults included.
        producer_opts =
          ProducerOptions.validate!(
            topic: topic,
            client: Keyword.fetch!(opts, :client),
            name: producer_name(opts)
          )

        [
          %{
            id: {:dead_letter, topic},
            start: {Pulsar.Topology, :start_link, [ProducerWorker, nil, :producer_count, producer_opts]},
            restart: :permanent,
            type: :supervisor
          }
        ]
    end
  end

  @doc """
  Publishes one message to the dead letter topic, carrying what identifies it.

  The key is preserved so a `Key_Shared` dead letter consumer sees the same partitioning as the
  origin, and the origin's coordinates are added to the properties rather than replacing them.
  """
  @spec divert(map(), Message.t()) :: :ok | {:error, term()}
  def divert(state, %Message{} = message) do
    opts = [
      client: state.client,
      partition_key: Message.key(message),
      ordering_key: Message.ordering_key(message),
      properties: origin_properties(state, message),
      event_time: Message.event_time(message)
    ]

    # Pulsar.Producer.send/3 answers {:error, :not_ready} while the dead letter topic is still
    # being discovered and turns a worker dying mid-send into an error, so a dead letter topic
    # that is unavailable or slow leaves the message nacked instead of reaching the consumer.
    with {:ok, producer} <- producer(state.dead_letter_root),
         {:ok, _message_id} <- Producer.send(producer, message.payload, opts) do
      :ok
    end
  end

  defp producer(nil), do: {:error, :no_dead_letter_producer}

  defp producer(root) do
    root
    |> Supervisor.which_children()
    |> Enum.find_value({:error, :no_dead_letter_producer}, fn
      {{:dead_letter, _topic}, pid, :supervisor, _modules} when is_pid(pid) -> {:ok, pid}
      _child -> false
    end)
  catch
    :exit, _reason -> {:error, :no_dead_letter_producer}
  end

  defp origin_properties(state, message) do
    message
    |> Message.properties()
    |> Map.put(@real_topic_property, state.topic)
    |> put_origin_message_id(message)
  end

  # A chunked message answers with a list of ids, one per chunk; the first is where it began.
  defp put_origin_message_id(properties, message) do
    case message.message_id |> List.wrap() |> List.first() do
      nil -> properties
      message_id -> Map.put(properties, @origin_message_id_property, format_message_id(message_id))
    end
  end

  # The Java client's MessageId.toString(), which is what reads this property. Its partition is
  # already -1 when the origin topic was not partitioned.
  defp format_message_id(%{ledgerId: ledger_id, entryId: entry_id, partition: partition}) do
    "#{ledger_id}:#{entry_id}:#{partition}"
  end

  defp format_message_id(message_id), do: inspect(message_id)

  # Defaults to the base topic rather than the partition a worker happens to hold, so every
  # partition of a partitioned consumer diverts into one dead letter topic.
  defp topic(opts) do
    case Keyword.get(opts, :dead_letter_policy) do
      nil ->
        nil

      policy ->
        Keyword.get(policy, :topic) ||
          "#{Keyword.fetch!(opts, :topic)}-#{Keyword.fetch!(opts, :subscription_name)}-DLQ"
    end
  end

  # Named after the consumer, not the dead letter topic: two subscriptions may be configured to
  # divert into the same topic, and the name keys this one's producer epochs and telemetry.
  defp producer_name(opts) do
    case topic(opts) do
      nil -> nil
      _topic -> "#{Keyword.fetch!(opts, :name)}-dead-letter-producer"
    end
  end
end
