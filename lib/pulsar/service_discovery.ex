defmodule Pulsar.ServiceDiscovery do
  @moduledoc """
  This module handles topic lookup operations that discover which broker owns a particular topic.
  The lookup process may involve following redirects across multiple brokers in a cluster before
  finding the authoritative broker for a topic.

  ## Example

      {:ok, broker_pid} = Pulsar.ServiceDiscovery.lookup_topic("persistent://public/default/my-topic")
  """

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
