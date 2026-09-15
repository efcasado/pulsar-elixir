defmodule Pulsar.Topology.Resolver do
  @moduledoc false

  # Resolves broker-owned topology metadata: which broker owns a topic and how many
  # partitions it has. A topic lookup may be redirected across several brokers before
  # reaching the authoritative one.

  require Logger

  @doc """
  Returns the number of partitions for `topic`.

  Performs a single `partitioned-topic-metadata` lookup against a random broker.
  Returns `{:ok, partitions}` where `partitions` is `0` for a non-partitioned
  topic, or `{:error, reason}` if the lookup failed.
  """
  @spec partition_count(String.t(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def partition_count(topic, opts \\ []) do
    client = Keyword.get(opts, :client, :default)

    :telemetry.span(
      [:pulsar, :topology, :resolver, :partition_count],
      %{topic: topic, client: client},
      fn ->
        result = do_partition_count(Pulsar.Client.random_broker(client), topic)

        metadata =
          case result do
            {:ok, partitions} ->
              %{success: true, topic: topic, client: client, partition_count: partitions}

            {:error, reason} ->
              %{success: false, topic: topic, client: client, error: reason}
          end

        {result, metadata}
      end
    )
  end

  defp do_partition_count(nil, _topic), do: {:error, :no_broker_available}

  defp do_partition_count(broker, topic) do
    case Pulsar.Broker.partitioned_topic_metadata(broker, topic) do
      {:ok, %{response: :Success, partitions: partitions}} ->
        {:ok, partitions}

      {:ok, %{response: :Failed, error: error}} ->
        {:error, {:partition_metadata_check_failed, error}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Returns a connection to the broker that owns `topic`, following lookup redirects to the
  authoritative one.

  Redirect lookups are stateless request/response operations, so they may use any live sibling
  and fail fast when its socket is disconnected. The final connection is checked out from
  `:connection_slot` when supplied, keeping a producer or consumer on the slot its topology owns.
  """
  @spec lookup_topic(String.t(), keyword()) :: {:ok, pid()} | {:error, any()}
  def lookup_topic(topic, opts \\ []) do
    client = Keyword.get(opts, :client, :default)
    connection_slot = Keyword.get(opts, :connection_slot, :random)

    :telemetry.span(
      [:pulsar, :topology, :resolver, :lookup_topic],
      %{topic: topic, client: client},
      fn ->
        result =
          lookup_topic(
            Pulsar.Client.random_broker(client),
            topic,
            false,
            client,
            connection_slot
          )

        metadata = %{success: match?({:ok, _}, result), topic: topic, client: client}
        {result, metadata}
      end
    )
  end

  defp lookup_topic(nil, _topic, _authoritative, _client, _connection_slot), do: {:error, :no_broker_available}

  defp lookup_topic(broker, topic, authoritative, client, connection_slot) do
    case Pulsar.Broker.lookup_topic(broker, topic, authoritative) do
      {:ok, %{response: :Connect} = response} ->
        Pulsar.Client.start_broker(get_broker_url(response),
          client: client,
          connection_slot: connection_slot
        )

      {:ok, %{response: :Redirect, authoritative: authoritative} = response} ->
        case Pulsar.Client.start_broker(get_broker_url(response), client: client) do
          {:ok, broker} ->
            lookup_topic(broker, topic, authoritative, client, connection_slot)

          {:error, reason} ->
            Logger.error("Cannot reach the broker a lookup redirected to: #{inspect(reason)}")
            {:error, reason}
        end

      {:ok, %{response: :Failed, error: error}} ->
        Logger.error("Topic lookup failed: #{inspect(error)}")
        {:error, {:lookup_failed, error}}

      {:error, reason} ->
        Logger.error("Topic lookup error: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp get_broker_url(%{brokerServiceUrl: service_url, brokerServiceUrlTls: service_url_tls}) do
    service_url_tls || service_url
  end
end
