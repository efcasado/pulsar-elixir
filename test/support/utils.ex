defmodule Pulsar.Test.Support.Utils do
  @moduledoc false
  def wait_for(_fun, attempts \\ 100, interval_ms \\ 100)

  def wait_for(_fun, 0, _interval_ms) do
    :error
  end

  def wait_for(fun, attempts, interval_ms) do
    if fun.() do
      :ok
    else
      Process.sleep(interval_ms)
      wait_for(fun, attempts - 1, interval_ms)
    end
  end

  @doc """
  Collects flow control telemetry events and returns aggregated statistics.
  Returns a map with statistics grouped by consumer_id.
  """
  def collect_flow_stats do
    [:pulsar, :consumer, :flow_control, :stop]
    |> collect_events()
    |> aggregate_flow_stats()
  end

  @doc """
  Collects lookup telemetry events and returns aggregated statistics.
  Returns a map with total, success, and failure counts.
  Filters by client (defaults to :default).
  """
  def collect_lookup_stats(opts \\ []) do
    client = Keyword.get(opts, :client, :default)

    [:pulsar, :service_discovery, :lookup_topic, :stop]
    |> collect_events()
    |> filter_by_client(client)
    |> aggregate_success_stats()
  end

  @doc """
  Collects producer opened telemetry events and returns aggregated statistics.
  Returns a map with total, success, and failure counts.
  """
  def collect_producer_opened_stats do
    [:pulsar, :producer, :opened, :stop]
    |> collect_events()
    |> aggregate_success_stats()
  end

  def collect_producer_closed_stats do
    [:pulsar, :producer, :closed, :stop]
    |> collect_events()
    |> aggregate_success_stats()
  end

  @doc """
  Collects message published telemetry events and returns aggregated statistics.
  Returns a map with total count.
  """
  def collect_message_published_stats do
    [:pulsar, :producer, :message, :published]
    |> collect_events()
    |> then(fn events -> %{total_count: length(events)} end)
  end

  @doc """
  Collects all raw telemetry events for the given event name.
  Returns a list of events with merged measurements and metadata.
  """
  def collect_events(event_name) do
    collect_events(event_name, [])
  end

  defp aggregate_flow_stats(events) do
    events
    |> Enum.group_by(& &1.consumer_id)
    |> Map.new(fn {consumer_id, consumer_events} ->
      stats = %{
        consumer_id: consumer_id,
        event_count: length(consumer_events),
        requested_total: Enum.sum(Enum.map(consumer_events, & &1.permits_requested))
      }

      {consumer_id, stats}
    end)
  end

  defp filter_by_client(events, client) do
    Enum.filter(events, fn event ->
      Map.get(event, :client) == client
    end)
  end

  defp aggregate_success_stats(events) do
    %{
      total_count: length(events),
      success_count: Enum.count(events, &(&1.success == true)),
      failure_count: Enum.count(events, &(&1.success == false))
    }
  end

  defp collect_events(event_name, acc) do
    receive do
      {:telemetry_event,
       %{
         event: ^event_name,
         measurements: measurements,
         metadata: metadata
       }} ->
        event = Map.merge(measurements, metadata)
        collect_events(event_name, [event | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
