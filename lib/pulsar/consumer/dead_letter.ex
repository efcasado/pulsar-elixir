defmodule Pulsar.Consumer.DeadLetter do
  @moduledoc false

  # The dead letter producer belongs to the consumer that diverts into it, so it hangs off that
  # consumer's Pulsar.Topology root rather than the client's producer branch. It is a producer
  # like any other below that point: its own topology, its own discovery, its own partitions.

  alias Pulsar.Message
  alias Pulsar.Producer

  # Read by a consumer of the dead letter topic to trace a message back to where it failed.
  # The names match the Java client's, so both ends agree on them.
  @real_topic_property "REAL_TOPIC"
  @origin_message_id_property "ORIGIN_MESSAGE_ID"

  @doc """
  Attaches a dead letter producer to a consumer's topology. `Pulsar.Topology`'s `:companions`.

  Called with the logical consumer's options, before `Pulsar.Topology.Group` rewrites `:name`
  and `:topic` per partition and per worker, so both the producer and what the workers are told
  about it describe the whole consumer rather than the one partition a worker happens to hold.

  A consumer with no dead letter policy attaches nothing.
  """
  @spec attach(keyword(), pid()) :: {keyword(), [Supervisor.child_spec()]}
  def attach(opts, root), do: {annotate(opts, root), child_specs(opts)}

  # What the workers inherit is the topology root, not a pid or a registered name. The producer
  # is a child of that root, so resolving through it is what makes a restart of the producer
  # invisible to them, and keeps them off a registry owned by a branch that restarts separately.
  defp annotate(opts, root) do
    case topic(opts) do
      nil -> opts
      _topic -> Keyword.put(opts, :dead_letter_root, root)
    end
  end

  defp child_specs(opts) do
    case topic(opts) do
      nil ->
        []

      topic ->
        producer_opts = [topic: topic, client: Keyword.fetch!(opts, :client), name: producer_name(opts)]

        [
          %{
            id: {:dead_letter, topic},
            start: {Producer, :start_link_unregistered, [producer_opts]},
            restart: :permanent,
            type: :supervisor
          }
        ]
    end
  end

  @doc """
  The dead letter producer a consumer attached, given the root the workers were told about.

  Resolved once per diverted delivery rather than once per message: this is a call into the
  topology root, which is also the supervisor discovery adds partitions to.
  """
  @spec producer(pid() | nil) :: {:ok, pid()} | {:error, :no_dead_letter_producer}
  def producer(nil), do: {:error, :no_dead_letter_producer}

  def producer(root) do
    root
    |> Supervisor.which_children()
    |> Enum.find_value({:error, :no_dead_letter_producer}, fn
      {{:dead_letter, _topic}, pid, :supervisor, _modules} when is_pid(pid) -> {:ok, pid}
      _child -> false
    end)
  catch
    :exit, _reason -> {:error, :no_dead_letter_producer}
  end

  @doc """
  Publishes one message to the dead letter topic, carrying what identifies it.

  `origin` is the consumer this message failed on: its `:client` and the `:topic` it was consumed
  from, which is the partition for a partitioned consumer.

  The key is preserved so a `Key_Shared` dead letter consumer sees the same partitioning as the
  origin, and the origin's coordinates are added to the properties rather than replacing them.
  """
  @spec divert(pid(), Message.t(), keyword()) :: :ok | {:error, term()}
  def divert(producer, %Message{} = message, origin) do
    opts = [
      client: Keyword.fetch!(origin, :client),
      partition_key: Message.key(message),
      ordering_key: Message.ordering_key(message),
      properties: origin_properties(Keyword.fetch!(origin, :topic), message),
      event_time: Message.event_time(message)
    ]

    # Pulsar.Producer.send/3 answers {:error, :not_ready} while the dead letter topic is still
    # being discovered and turns a worker dying mid-send into an error, so a dead letter topic
    # that is unavailable or slow leaves the message nacked instead of reaching the consumer.
    case Producer.send(producer, message.payload, opts) do
      {:ok, _message_id} -> :ok
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec origin_properties(String.t(), Message.t()) :: %{optional(String.t()) => String.t()}
  def origin_properties(origin_topic, message) do
    message
    |> Message.properties()
    |> Map.put(@real_topic_property, origin_topic)
    |> put_origin_message_id(message)
  end

  # Pulsar.Message owns how an id is read and printed, batch entries and chunks included, and
  # the string it answers is the one the Java client writes into this property.
  defp put_origin_message_id(properties, message) do
    case Message.message_id_string(message) do
      nil -> properties
      message_id -> Map.put(properties, @origin_message_id_property, message_id)
    end
  end

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
