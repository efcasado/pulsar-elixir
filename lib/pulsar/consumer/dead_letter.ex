defmodule Pulsar.Consumer.DeadLetter do
  @moduledoc false

  # The dead letter producer belongs to the consumer that diverts into it, so it hangs off that
  # consumer's Pulsar.Topology root rather than the client's producer branch.

  alias Pulsar.Message
  alias Pulsar.Producer
  alias Pulsar.Producer.Options, as: ProducerOptions

  @managed_producer_options [:topic, :client, :name]

  # The Java client's names, so both ends of a dead letter topic agree on them.
  @real_topic_property "REAL_TOPIC"
  @origin_message_id_property "ORIGIN_MESSAGE_ID"

  @doc """
  Attaches a dead letter producer to a consumer's topology. `Pulsar.Topology`'s `:companions`.

  Called with the logical consumer's options, before `Pulsar.Topology.Group` rewrites `:name` and
  `:topic` per partition, so what it builds describes the whole consumer rather than one partition.
  """
  @spec attach(keyword(), pid()) :: {keyword(), [Supervisor.child_spec()]}
  def attach(opts, root), do: {annotate(opts, root), child_specs(opts)}

  # Workers inherit the topology root rather than a pid, and find the producer among its children
  # when they need it, so a producer that restarts under them goes unnoticed.
  defp annotate(opts, root) do
    case topic(opts) do
      nil -> opts
      topic -> Keyword.merge(opts, dead_letter_root: root, dead_letter_topic: topic)
    end
  end

  defp child_specs(opts) do
    case topic(opts) do
      nil ->
        []

      topic ->
        producer_opts =
          opts
          |> policy()
          |> Keyword.get(:producer, [])
          |> Keyword.merge(topic: topic, client: Keyword.fetch!(opts, :client), name: producer_name(opts))

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

  `origin_topic` is where the message was consumed from, which is the partition for a partitioned
  consumer. The key is preserved, so a `:key_shared` dead letter consumer partitions as it did.
  """
  @spec divert(pid(), Message.t(), String.t()) :: :ok | {:error, term()}
  def divert(producer, %Message{} = message, origin_topic) do
    opts = [
      partition_key: Message.key(message),
      ordering_key: Message.ordering_key(message),
      properties: origin_properties(origin_topic, message),
      event_time: Message.event_time(message)
    ]

    # send/3 already answers {:error, :not_ready} during discovery and turns a worker dying
    # mid-send into an error, so an unavailable dead letter topic never reaches the consumer.
    case Producer.send(producer, message.payload, opts) do
      # {:ok, :deduplicated} is acknowledged along with the rest. An error would nack the message
      # into a divert that is still over the redelivery threshold, so it would divert again, and
      # this producer's name is shared by every node running the consumer -- a collision between
      # them need not clear. The producer already logs and reports what the broker discarded.
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

  defp put_origin_message_id(properties, message) do
    case Message.message_id_string(message) do
      nil -> properties
      message_id -> Map.put(properties, @origin_message_id_property, message_id)
    end
  end

  @doc false
  @spec validate_producer_options(term()) :: {:ok, keyword()} | {:error, String.t()}
  def validate_producer_options(opts) when is_list(opts) do
    case Enum.filter(@managed_producer_options, &Keyword.has_key?(opts, &1)) do
      [] -> validate_against_producer_schema(opts)
      managed -> {:error, "#{inspect(managed)} belongs to the consumer and cannot be set here"}
    end
  end

  def validate_producer_options(other), do: {:error, "expected a keyword list, got: #{inspect(other)}"}

  # :topic is required of a producer and supplied by the consumer, so it stands in here only to
  # let the rest be checked while the caller is still holding the consumer's options.
  defp validate_against_producer_schema(opts) do
    case ProducerOptions.validate(Keyword.put(opts, :topic, "validation")) do
      {:ok, _validated} -> {:ok, opts}
      {:error, error} -> {:error, Exception.message(error)}
    end
  end

  defp policy(opts), do: Keyword.get(opts, :dead_letter_policy)

  # Defaults to the base topic rather than the partition a worker happens to hold, so every
  # partition of a partitioned consumer diverts into one dead letter topic.
  defp topic(opts) do
    case policy(opts) do
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
