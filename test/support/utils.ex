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
end
