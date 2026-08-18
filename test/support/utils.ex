defmodule Pulsar.Test.Support.Utils do
  @moduledoc false
  import ExUnit.Assertions, only: [flunk: 1]

  @default_timeout 10_000
  @default_interval_ms 100

  @doc """
  Publishes `messages` to `topic`, and returns their message ids once the broker has stored
  every one.

  A message is a payload, or a `{partition_key, payload}` pair for a topic whose tests care
  which partition or which key a message lands under. Ids come back in the order the messages
  were handed over, which is the order the producer publishes them in.

  The sends are in flight together rather than one at a time, so seeding a topic costs about a
  sixth of what it did serially. Seeding more than the producer's `:max_pending_messages` in
  one call would be refused rather than queued.

  Messages published together share a publish time, which is stamped to the millisecond. A test
  that seeks by timestamp needs them stamped apart, and publishes its own rather than using
  this.

  ## Options
    - `:client` - the client to publish through (required)
    - `:producer` - the producer's name, by default one derived from the topic
    - `:producer_opts` - anything else the producer should be started with
  """
  def seed_topic(topic, messages, opts) do
    client = Keyword.fetch!(opts, :client)
    name = Keyword.get_lazy(opts, :producer, fn -> "#{topic}-seed" end)
    producer_opts = Keyword.get(opts, :producer_opts, [])

    {:ok, producer} = Pulsar.Producer.start(topic, [client: client, name: name] ++ producer_opts)
    :ok = Pulsar.Producer.await_ready(producer)

    messages
    |> Enum.map(fn message ->
      {payload, send_opts} = publish(message)
      {:ok, ref} = Pulsar.Producer.send_async(producer, payload, send_opts)
      ref
    end)
    |> Enum.map(fn ref ->
      {:ok, message_id} = Pulsar.Producer.await(ref)
      message_id
    end)
  end

  defp publish({key, payload}), do: {payload, [partition_key: key]}
  defp publish(payload), do: {payload, []}

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
end
