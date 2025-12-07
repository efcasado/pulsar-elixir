defmodule Pulsar.Integration.Producer.PartitionedTopicTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :partitioned_producer_test_client
  @topic "persistent://public/default/partitioned-producer-test"

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
      Pulsar.start_producer(@topic,
        client: @client,
        name: "partitioned-producer-test-1"
      )

    :ok = wait_for_producers_ready(producer_pid)

    partition_groups = Pulsar.PartitionedProducer.get_partition_groups(producer_pid)
    all_producers = Pulsar.PartitionedProducer.get_producers(producer_pid)

    assert Enum.count(partition_groups) == 3
    assert Enum.count(all_producers) == 3

    :ok = Pulsar.stop_producer(producer_pid)
  end

  test "messages with same key consumed from same partition" do
    test_id = :erlang.unique_integer([:positive])
    subscription = "partitioned-test-#{test_id}"
    producer_name = "partitioned-producer-test-#{test_id}"

    {:ok, producer_pid} =
      Pulsar.start_producer(@topic,
        client: @client,
        name: producer_name
      )

    :ok = wait_for_producers_ready(producer_pid)

    {:ok, consumer_pid} =
      Pulsar.start_consumer(
        @topic,
        subscription,
        DummyConsumer,
        client: @client,
        initial_position: :latest,
        init_args: [notify_pid: self()]
      )

    consumers = wait_for_consumer_ready(3)

    partition_key = "same-partition-key-#{test_id}"
    messages = ["e2e-msg-1-#{test_id}", "e2e-msg-2-#{test_id}", "e2e-msg-3-#{test_id}"]

    for msg <- messages do
      {:ok, _} = Pulsar.send(producer_pid, msg, partition_key: partition_key)
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
             |> Enum.map(fn msg -> hd(msg.metadata).partition_key end)
             |> Enum.uniq()

    # All messages should have been routed to the same partition
    assert [_single_partition] =
             our_messages
             |> Enum.map(fn msg -> hd(msg.command).message_id.partition end)
             |> Enum.uniq()

    :ok = Pulsar.stop_producer(producer_pid)
    :ok = Pulsar.stop_consumer(consumer_pid)
  end

  test "messages without partition_key are distributed randomly across partitions" do
    test_id = :erlang.unique_integer([:positive])
    subscription = "partitioned-random-test-#{test_id}"
    producer_name = "partitioned-producer-random-test-#{test_id}"

    {:ok, producer_pid} =
      Pulsar.start_producer(@topic,
        client: @client,
        name: producer_name
      )

    :ok = wait_for_producers_ready(producer_pid)

    {:ok, consumer_pid} =
      Pulsar.start_consumer(
        @topic,
        subscription,
        DummyConsumer,
        client: @client,
        initial_position: :latest,
        init_args: [notify_pid: self()]
      )

    consumers = wait_for_consumer_ready(3)

    messages =
      for i <- 1..30 do
        msg = "random-msg-#{i}-#{test_id}"
        {:ok, _} = Pulsar.send(producer_pid, msg)
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
      |> Enum.map(fn msg -> hd(msg.command).message_id.partition end)

    # With 30 messages and 3 partitions, random distribution should hit all partitions
    assert partitions |> Enum.uniq() |> Enum.count() == 3
    partition_counts = Enum.frequencies(partitions)
    assert Enum.all?(partition_counts, fn {_partition, count} -> count >= 1 end)

    :ok = Pulsar.stop_producer(producer_pid)
    :ok = Pulsar.stop_consumer(consumer_pid)
  end

  defp wait_for_producers_ready(partitioned_producer_pid) do
    Utils.wait_for(fn ->
      producers = Pulsar.PartitionedProducer.get_producers(partitioned_producer_pid)

      Enum.count(producers) == 3 and
        Enum.all?(producers, fn producer ->
          state = :sys.get_state(producer)
          state.producer_name != nil
        end)
    end)
  end

  defp wait_for_consumer_ready(count, timeout \\ 5000) do
    Enum.map(1..count, fn _ ->
      receive do
        {:consumer_ready, pid} -> pid
      after
        timeout -> flunk("Timeout waiting for consumer to be ready")
      end
    end)
  end
end
