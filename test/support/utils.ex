defmodule Pulsar.Test.Support.Utils do
  @moduledoc false
  import ExUnit.Assertions, only: [flunk: 1]

  @default_timeout 10_000
  @default_interval_ms 100

  @doc """
  Polls `fun` until its result satisfies `:until`, and returns that result.

  Fails the test when `:timeout` expires. The budget is a monotonic deadline, so time spent
  inside `fun` counts against it too. `:until` defaults to a truthiness check.
  """
  def wait_for(fun, opts \\ []) do
    until = Keyword.get(opts, :until, &truthy?/1)
    interval = Keyword.get(opts, :interval, @default_interval_ms)
    description = Keyword.get(opts, :description, "condition")
    deadline = System.monotonic_time(:millisecond) + Keyword.get(opts, :timeout, @default_timeout)

    poll(fun, until, deadline, interval, description)
  end

  defp truthy?(result), do: result not in [false, nil]

  defp poll(fun, until, deadline, interval, description) do
    observation = fun.()

    if until.(observation) do
      observation
    else
      retry(fun, until, deadline, interval, description, observation)
    end
  end

  defp retry(fun, until, deadline, interval, description, observation) do
    case deadline - System.monotonic_time(:millisecond) do
      remaining when remaining > 0 ->
        Process.sleep(min(interval, remaining))
        poll(fun, until, deadline, interval, description)

      _expired ->
        flunk("Timed out waiting for #{description}; last observation: #{inspect(observation)}")
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

    [:pulsar, :topology, :resolver, :lookup_topic, :stop]
    |> collect_events()
    |> filter_by_client(client)
    |> aggregate_success_stats()
  end

  @doc """
  Collects producer opened telemetry events and returns aggregated statistics.
  Returns a map with total, success, and failure counts.

  ## Options
    - `:producer_names` - list of producer names to filter by (optional)
  """
  def collect_producer_opened_stats(opts \\ []) do
    [:pulsar, :producer, :opened, :stop]
    |> collect_events(opts)
    |> aggregate_success_stats()
  end

  @doc """
  Collects producer closed telemetry events and returns aggregated statistics.
  Returns a map with total, success, and failure counts.

  ## Options
    - `:producer_names` - list of producer names to filter by (optional)
  """
  def collect_producer_closed_stats(opts \\ []) do
    [:pulsar, :producer, :closed, :stop]
    |> collect_events(opts)
    |> aggregate_success_stats()
  end

  @doc """
  Collects message published telemetry events and returns aggregated statistics.
  Returns a map with total count.

  ## Options
    - `:producer_names` - list of producer names to filter by (optional)
  """
  def collect_message_published_stats(opts \\ []) do
    [:pulsar, :producer, :message, :published]
    |> collect_events(opts)
    |> then(fn events -> %{total_count: length(events)} end)
  end

  @doc """
  Collects all raw telemetry events for the given event name.
  Returns a list of events with merged measurements and metadata.

  ## Options
    - `:producer_names` - list of producer names to filter by (optional)
  """
  def collect_events(event_name, opts \\ []) do
    event_name
    |> do_collect_events([])
    |> filter_by_producer_names(opts)
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

  defp filter_by_producer_names(events, opts) do
    case Keyword.get(opts, :producer_names) do
      nil ->
        events

      names when is_list(names) ->
        Enum.filter(events, &(&1 |> Map.get(:producer_name) |> in_group?(names)))
    end
  end

  # A group's workers are named after it with an index suffix, so filtering by the group's
  # name matches every producer in it.
  defp in_group?(producer_name, names) do
    Enum.any?(names, fn name ->
      producer_name == name or String.starts_with?(to_string(producer_name), "#{name}-")
    end)
  end

  defp aggregate_success_stats(events) do
    %{
      total_count: length(events),
      success_count: Enum.count(events, &(&1.success == true)),
      failure_count: Enum.count(events, &(&1.success == false))
    }
  end

  defp do_collect_events(event_name, acc) do
    receive do
      {:telemetry_event,
       %{
         event: ^event_name,
         measurements: measurements,
         metadata: metadata
       }} ->
        event = Map.merge(measurements, metadata)
        do_collect_events(event_name, [event | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  @doc """
  Waits for the specified number of consumers to be ready.

  Consumers are considered ready when they send a `{:consumer_ready, pid}` message.
  This is typically used with the DummyConsumer callback which notifies when initialized.

  Returns a list of consumer PIDs in the order they became ready.
  """
  def wait_for_consumer_ready(count, timeout \\ 5000) do
    import ExUnit.Assertions, only: [flunk: 1]

    Enum.map(1..count, fn _ ->
      receive do
        {:consumer_ready, pid} -> pid
      after
        timeout -> flunk("Timeout waiting for consumer to be ready")
      end
    end)
  end
end
