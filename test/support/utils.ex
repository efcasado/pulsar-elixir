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
  What each consumer's flow control has cost so far, keyed by consumer id.
  """
  def collect_flow_stats do
    [:pulsar, :consumer, :flow_control, :stop]
    |> collect_events()
    |> aggregate_flow_stats()
  end

  @doc """
  Counts how many of `event_name`'s events reported success and how many reported failure.

  Takes the same filters as `collect_events/2`.
  """
  def collect_stats(event_name, opts \\ []) do
    event_name
    |> collect_events(opts)
    |> aggregate_success_stats()
  end

  @doc """
  Every event named `event_name` collected so far, oldest first, each one its measurements
  merged with its metadata.

  ## Options
    - `:producer_names` - keep only the events a producer in this list emitted
    - `:client` - keep only the events this client emitted
  """
  def collect_events(event_name, opts \\ []) do
    event_name
    |> do_collect_events([])
    |> filter_by(:producer_name, Keyword.get(opts, :producer_names), &in_group?/2)
    |> filter_by(:client, Keyword.get(opts, :client), &==/2)
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

  defp filter_by(events, _key, nil, _match?), do: events

  defp filter_by(events, key, expected, match?) do
    Enum.filter(events, &match?.(Map.get(&1, key), expected))
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
  The pids of `count` consumers, in the order they announced themselves ready.

  A consumer announces itself by sending `{:consumer_ready, pid}`, which
  `Pulsar.Test.Support.DummyConsumer` does when started with `notify_pid: self()`.
  """
  def wait_for_consumer_ready(count, timeout \\ 5000) do
    Enum.map(1..count, fn _ ->
      receive do
        {:consumer_ready, pid} -> pid
      after
        timeout -> flunk("Timeout waiting for consumer to be ready")
      end
    end)
  end
end
