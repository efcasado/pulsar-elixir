defmodule Pulsar.ServiceDiscovery do
  @moduledoc false

  # Topic lookup: which broker owns a topic, and how many partitions it has. A lookup may be
  # redirected across several brokers before reaching the authoritative one.

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
      [:pulsar, :service_discovery, :partition_count],
      %{},
      fn ->
        result = do_partition_count(Pulsar.Client.random_broker(client), topic)

        metadata = %{success: match?({:ok, _}, result), client: client}
        {result, metadata}
      end
    )
  end

  @doc """
  Same as `partition_count/2`, retrying while the cluster is not answering yet.

  A client's bootstrap broker connects asynchronously, so the first lookup after a
  client starts routinely arrives before there is a connection to ask.
  """
  @spec partition_count_with_retry(String.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def partition_count_with_retry(topic, opts \\ []) do
    attempts = Keyword.get(opts, :attempts, 10)
    delay_ms = Keyword.get(opts, :delay_ms, 500)

    retry_partition_count(topic, Keyword.take(opts, [:client]), attempts, delay_ms)
  end

  defp retry_partition_count(_topic, _opts, 0, _delay_ms), do: {:error, :partition_check_failed}

  defp retry_partition_count(topic, opts, attempts, delay_ms) do
    case partition_count(topic, opts) do
      {:ok, partitions} ->
        {:ok, partitions}

      {:error, reason} ->
        Logger.warning("Error checking partitioned topic metadata for #{topic}: #{inspect(reason)}")
        Process.sleep(delay_ms)
        retry_partition_count(topic, opts, attempts - 1, delay_ms)
    end
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

  @spec lookup_topic(String.t(), keyword()) :: {:ok, pid()} | {:error, any()}
  def lookup_topic(topic, opts \\ []) do
    client = Keyword.get(opts, :client, :default)

    :telemetry.span(
      [:pulsar, :service_discovery, :lookup_topic],
      %{},
      fn ->
        result = lookup_topic(Pulsar.Client.random_broker(client), topic, false, client)

        metadata = %{success: match?({:ok, _}, result), client: client}
        {result, metadata}
      end
    )
  end

  defp lookup_topic(nil, _topic, _authoritative, _client), do: {:error, :no_broker_available}

  defp lookup_topic(broker, topic, authoritative, client) do
    case Pulsar.Broker.lookup_topic(broker, topic, authoritative) do
      {:ok, %{response: :Connect} = response} ->
        response
        |> get_broker_url()
        |> Pulsar.Client.start_broker(client: client)

      {:ok, %{response: :Redirect, authoritative: authoritative} = response} ->
        {:ok, broker} =
          response
          |> get_broker_url()
          |> Pulsar.Client.start_broker(client: client)

        lookup_topic(broker, topic, authoritative, client)

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
