defmodule Pulsar.Integration.Consumer.ScalableConsumerTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :scalable_consumer_test_client
  @consumer_callback Pulsar.Test.Support.DummyConsumer

  setup_all do
    {:ok, _client_pid} =
      Pulsar.Client.start_link(name: @client, host: System.broker().service_url)

    on_exit(fn -> Pulsar.Client.stop(@client) end)

    :ok
  end

  test "consumes all messages across a multi-segment scalable topic" do
    # A scalable topic is addressed by the `topic://` scheme. Passing such a
    # topic to `Pulsar.start_consumer/4` transparently starts a scalable
    # consumer that fans out one consumer group per segment.
    path = unique_topic("scalable-multi")
    topic = "topic://" <> path
    System.create_scalable_topic(path, _segments = 2)

    messages = for i <- 1..6, do: {"key-#{i}", "message-#{i}"}
    System.produce_messages(topic, messages)

    {:ok, consumer} =
      Pulsar.start_consumer(topic, "multi-segment-sub", @consumer_callback, subscription_options())

    assert :ok = Utils.wait_for(fn -> total_consumed(consumer) == length(messages) end)

    # One consumer group per segment; every produced message is consumed
    # exactly once, regardless of how keys hash across the two segments.
    assert length(Pulsar.ScalableConsumer.get_segment_groups(consumer)) == 2
    assert total_consumed(consumer) == length(messages)

    :ok = Pulsar.stop_consumer(consumer)
  end

  test "grows consumers when a segment splits" do
    path = unique_topic("scalable-split")
    topic = "topic://" <> path
    System.create_scalable_topic(path, _segments = 1)

    {:ok, consumer} =
      Pulsar.start_consumer(topic, "split-sub", @consumer_callback, subscription_options())

    # A single-segment topic starts with one consumer group.
    assert length(Pulsar.ScalableConsumer.get_segment_groups(consumer)) == 1

    # Splitting segment 0 seals it and adds two active child segments. The
    # watcher receives the pushed topology update and reconciles: the sealed
    # parent is kept (to drain its backlog) and the two children get their own
    # consumer groups, so the count grows from 1 to 3.
    System.split_scalable_segment(path, 0)
    assert :ok = Utils.wait_for(fn -> length(Pulsar.ScalableConsumer.get_segment_groups(consumer)) == 3 end)

    # Messages produced after the split route to the active child segments and
    # are consumed by the newly added groups.
    messages = for i <- 1..8, do: {"key-#{i}", "message-#{i}"}
    System.produce_messages(topic, messages)
    assert :ok = Utils.wait_for(fn -> total_consumed(consumer) == length(messages) end)

    :ok = Pulsar.stop_consumer(consumer)
  end

  test "stream consumer consumes all messages across its assigned segments" do
    # `scalable_type: :stream` registers with the controller and consumes the
    # assigned subset of segments. A lone stream consumer is assigned all of them.
    path = unique_topic("stream-multi")
    topic = "topic://" <> path
    System.create_scalable_topic(path, _segments = 2)

    messages = for i <- 1..6, do: {"key-#{i}", "message-#{i}"}
    System.produce_messages(topic, messages)

    {:ok, consumer} =
      Pulsar.start_consumer(topic, "stream-multi-sub", @consumer_callback, stream_options())

    assert :ok = Utils.wait_for(fn -> total_consumed(consumer) == length(messages) end)
    assert length(Pulsar.ScalableConsumer.get_segment_groups(consumer)) == 2

    :ok = Pulsar.stop_consumer(consumer)
  end

  test "stream consumer is assigned the children once a segment drains and splits" do
    path = unique_topic("stream-split")
    topic = "topic://" <> path
    System.create_scalable_topic(path, _segments = 1)

    {:ok, consumer} =
      Pulsar.start_consumer(topic, "stream-split-sub", @consumer_callback, stream_options())

    assert length(Pulsar.ScalableConsumer.get_segment_groups(consumer)) == 1

    # The consumer drains segment 0 (it acks as it consumes). The controller
    # withholds the children until the parent is drained, so only after that does
    # splitting yield an assignment update that adds the child segment groups.
    before = for i <- 1..5, do: {"key-#{i}", "before-#{i}"}
    System.produce_messages(topic, before)
    assert :ok = Utils.wait_for(fn -> total_consumed(consumer) == length(before) end)

    System.split_scalable_segment(path, 0)
    assert :ok = Utils.wait_for(fn -> length(Pulsar.ScalableConsumer.get_segment_groups(consumer)) > 1 end)

    # Messages produced after the split route to the active children and are
    # consumed by the newly assigned groups.
    after_split = for i <- 6..10, do: {"key-#{i}", "after-#{i}"}
    System.produce_messages(topic, after_split)
    assert :ok = Utils.wait_for(fn -> total_consumed(consumer) == length(before) + length(after_split) end)

    :ok = Pulsar.stop_consumer(consumer)
  end

  defp subscription_options do
    [
      client: @client,
      initial_position: :earliest,
      flow_initial: 1,
      flow_threshold: 0,
      flow_refill: 1
    ]
  end

  defp stream_options, do: Keyword.put(subscription_options(), :scalable_type, :stream)

  # Sums consumed messages across every segment consumer. Tolerates the churn
  # while the topology reconciles: a group being (re)started can briefly surface
  # an `:undefined` child or a pid that exits between lookup and call.
  defp total_consumed(consumer) do
    consumer
    |> Pulsar.get_consumers()
    |> Enum.filter(&is_pid/1)
    |> Enum.reduce(0, fn consumer_pid, acc ->
      try do
        @consumer_callback.count_messages(consumer_pid) + acc
      catch
        :exit, _reason -> acc
      end
    end)
  end

  defp unique_topic(prefix), do: "public/default/#{prefix}-#{:erlang.unique_integer([:positive])}"
end
