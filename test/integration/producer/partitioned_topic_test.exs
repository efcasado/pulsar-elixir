defmodule Pulsar.Integration.Producer.PartitionedTopicTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils
  alias Pulsar.Topology

  @moduletag :integration
  @client :partitioned_producer_test_client
  @topic "persistent://public/default/partitioned-producer-test"
  @discovery_interval_ms 200

  setup_all do
    broker = System.broker()

    System.create_topic(@topic, 3)

    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)

    :ok
  end

  test "creates producer groups for each partition" do
    {:ok, producer_pid} =
      Pulsar.Producer.start(@topic,
        client: @client,
        name: "partitioned-producer-test-1"
      )

    assert Pulsar.Client.producers(@client) == [producer_pid]

    :ok = Pulsar.Producer.await_ready(producer_pid)

    assert Enum.count(Topology.workers(producer_pid)) == 3

    :ok = Pulsar.Producer.stop(producer_pid)
  end

  test "messages with same key consumed from same partition" do
    test_id = :erlang.unique_integer([:positive])
    subscription = "partitioned-test-#{test_id}"
    producer_name = "partitioned-producer-test-#{test_id}"

    {:ok, producer_pid} =
      Pulsar.Producer.start(@topic,
        client: @client,
        name: producer_name
      )

    :ok = Pulsar.Producer.await_ready(producer_pid)

    {:ok, consumer_pid} =
      Pulsar.Consumer.start(
        @topic,
        subscription,
        DummyConsumer,
        client: @client,
        initial_position: :latest,
        init_args: [notify_pid: self()]
      )

    consumers = Utils.wait_for_consumer_ready(3)

    partition_key = "same-partition-key-#{test_id}"
    messages = ["e2e-msg-1-#{test_id}", "e2e-msg-2-#{test_id}", "e2e-msg-3-#{test_id}"]

    for msg <- messages do
      {:ok, _} = Pulsar.Producer.send(producer_pid, msg, partition_key: partition_key)
    end

    Utils.wait_for(fn ->
      all_msgs = Enum.flat_map(consumers, &DummyConsumer.get_messages/1)
      our_msgs = Enum.filter(all_msgs, fn msg -> msg.payload in messages end)
      Enum.count(our_msgs) == 3
    end)

    our_messages =
      consumers
      |> Enum.flat_map(&DummyConsumer.get_messages/1)
      |> Enum.filter(fn msg -> msg.payload in messages end)

    # All messages should have the same partition_key
    assert [^partition_key] =
             our_messages
             |> Enum.map(fn msg -> Pulsar.Message.key(msg) end)
             |> Enum.uniq()

    # All messages should have been routed to the same partition
    assert [_single_partition] =
             our_messages
             |> Enum.map(fn msg -> msg.raw.command.message_id.partition end)
             |> Enum.uniq()

    :ok = Pulsar.Producer.stop(producer_pid)
    :ok = Pulsar.Consumer.stop(consumer_pid)
  end

  test "messages without partition_key are distributed randomly across partitions" do
    test_id = :erlang.unique_integer([:positive])
    subscription = "partitioned-random-test-#{test_id}"
    producer_name = "partitioned-producer-random-test-#{test_id}"

    {:ok, producer_pid} =
      Pulsar.Producer.start(@topic,
        client: @client,
        name: producer_name
      )

    :ok = Pulsar.Producer.await_ready(producer_pid)

    {:ok, consumer_pid} =
      Pulsar.Consumer.start(
        @topic,
        subscription,
        DummyConsumer,
        client: @client,
        initial_position: :latest,
        init_args: [notify_pid: self()]
      )

    consumers = Utils.wait_for_consumer_ready(3)

    messages =
      for i <- 1..30 do
        msg = "random-msg-#{i}-#{test_id}"
        {:ok, _} = Pulsar.Producer.send(producer_pid, msg)
        msg
      end

    Utils.wait_for(fn ->
      all_msgs = Enum.flat_map(consumers, &DummyConsumer.get_messages/1)
      our_msgs = Enum.filter(all_msgs, fn msg -> msg.payload in messages end)
      Enum.count(our_msgs) == 30
    end)

    partitions =
      consumers
      |> Enum.flat_map(&DummyConsumer.get_messages/1)
      |> Enum.filter(fn msg -> msg.payload in messages end)
      |> Enum.map(fn msg -> msg.raw.command.message_id.partition end)

    # With 30 messages and 3 partitions, random distribution should hit all partitions
    assert partitions |> Enum.uniq() |> Enum.count() == 3
    partition_counts = Enum.frequencies(partitions)
    assert Enum.all?(partition_counts, fn {_partition, count} -> count >= 1 end)

    :ok = Pulsar.Producer.stop(producer_pid)
    :ok = Pulsar.Consumer.stop(consumer_pid)
  end

  test "discovers partitions added to the topic" do
    test_id = :erlang.unique_integer([:positive])
    topic = "persistent://public/default/partition-discovery-producer-#{test_id}"

    System.create_topic(topic, 3)

    {:ok, producer_pid} =
      Pulsar.Producer.start(topic,
        client: @client,
        name: "partition-discovery-producer-#{test_id}",
        partition_discovery_interval_ms: @discovery_interval_ms
      )

    initial_workers =
      Utils.wait_for(fn -> Topology.workers(producer_pid) end,
        until: &(length(&1) == 3),
        description: "initial producer partitions to start"
      )

    System.update_partitions(topic, 6)

    # The discovery poller should pick up the new partitions and start a
    # producer group for each one, without restarting the existing groups.
    current_workers =
      Utils.wait_for(fn -> Topology.workers(producer_pid) end,
        until: &(length(&1) == 6),
        description: "added producer partitions to start"
      )

    assert Enum.all?(initial_workers, &(&1 in current_workers))

    :ok = Pulsar.Producer.stop(producer_pid)
  end
end
