defmodule Pulsar.Integration.ConsumerTest do
  use ExUnit.Case
  require Logger
  alias Pulsar.TestHelper

  @moduletag :integration
  @test_topic "persistent://public/default/integration-test-topic"
  @test_subscription "integration-test-subscription"
  @messages [
    {"key1", "Message 1 for key1 - #{:os.system_time(:millisecond)}"},
    {"key2", "Message 1 for key2 - #{:os.system_time(:millisecond)}"},
    {"key1", "Message 2 for key1 - #{:os.system_time(:millisecond)}"},
    {"key2", "Message 2 for key2 - #{:os.system_time(:millisecond)}"},
    {"key3", "Message 1 for key3 - #{:os.system_time(:millisecond)}"},
    {"key4", "Message 1 for key4 - #{:os.system_time(:millisecond)}"}
  ]

  setup_all do
    TestHelper.start_pulsar()

    on_exit(fn ->
      TestHelper.stop_pulsar()
    end)

    :ok
  end

  setup do
    # Use a random broker for each test since Pulsar is leaderless
    pulsar_url = TestHelper.random_broker_url()
    {:ok, _broker_pid} = Pulsar.start_broker(pulsar_url)

    on_exit(fn ->
      Pulsar.stop_broker(pulsar_url)
    end)

    {:ok, pulsar_url: pulsar_url}
  end

  describe "Consumer Integration" do
    test "produce and consume messages" do
      {:ok, group_pid} =
        Pulsar.start_consumer(
          @test_topic,
          @test_subscription <> "-e2e",
          :Shared,
          Pulsar.DummyConsumer
        )

      # Get the individual consumer PID from the group
      [consumer_pid] = Pulsar.ConsumerGroup.list_consumers(group_pid)

      Process.sleep(3000)
      TestHelper.produce_messages(@test_topic, @messages)
      Process.sleep(3000)

      message_count = Pulsar.DummyConsumer.count_messages(consumer_pid)

      assert message_count == Enum.count(@messages),
             "Expected to receive as many messages as were produced"
    end

    test "Key_Shared subscription with multiple consumers" do
      {:ok, group_pid} =
        Pulsar.start_consumer(
          @test_topic,
          @test_subscription <> "-key-shared",
          :Key_Shared,
          Pulsar.DummyConsumer,
          consumer_count: 2
        )

      consumer_pids = Pulsar.ConsumerGroup.list_consumers(group_pid)
      assert length(consumer_pids) == 2
      [consumer1_pid, consumer2_pid] = consumer_pids

      Process.sleep(3000)
      TestHelper.produce_messages(@test_topic, @messages)
      Process.sleep(3000)

      # Get messages from each consumer
      consumer1_messages = Pulsar.DummyConsumer.get_messages(consumer1_pid)
      consumer2_messages = Pulsar.DummyConsumer.get_messages(consumer2_pid)

      consumer1_count = length(consumer1_messages)
      consumer2_count = length(consumer2_messages)
      total_messages = consumer1_count + consumer2_count

      Logger.info("Consumer 1 received #{consumer1_count} messages")
      Logger.info("Consumer 2 received #{consumer2_count} messages")
      Logger.info("Total messages received: #{total_messages}")

      # Extract partition keys from consumed messages
      extract_keys = fn messages ->
        messages
        |> Enum.map(& &1.partition_key)
        |> Enum.filter(&(&1 != nil))
        |> MapSet.new()
      end

      consumer1_keys = extract_keys.(consumer1_messages)
      consumer2_keys = extract_keys.(consumer2_messages)

      Logger.info("Consumer 1 keys: #{inspect(MapSet.to_list(consumer1_keys))}")
      Logger.info("Consumer 2 keys: #{inspect(MapSet.to_list(consumer2_keys))}")

      # With 4 different keys and Key_Shared mode, both consumers should receive messages
      # Key_Shared distributes messages based on key hashing, so different keys should
      # go to different consumers
      assert consumer1_count > 0, "Consumer 1 should receive at least one message"
      assert consumer2_count > 0, "Consumer 2 should receive at least one message"

      # Verify key partitioning - no key should be consumed by both consumers
      key_overlap = MapSet.intersection(consumer1_keys, consumer2_keys)

      assert MapSet.size(key_overlap) == 0,
             "Keys should be partitioned between consumers, but found overlap: #{inspect(MapSet.to_list(key_overlap))}"
    end

    test "Shared subscription with multiple consumers (round-robin)" do
      {:ok, group_pid} =
        Pulsar.start_consumer(
          @test_topic,
          @test_subscription <> "-shared",
          :Shared,
          Pulsar.DummyConsumer,
          consumer_count: 2
        )

      consumer_pids = Pulsar.ConsumerGroup.list_consumers(group_pid)
      assert length(consumer_pids) == 2
      [consumer1_pid, consumer2_pid] = consumer_pids

      Process.sleep(3000)
      TestHelper.produce_messages(@test_topic, @messages)
      Process.sleep(5000)

      # Check message distribution across consumers
      consumer1_count = Pulsar.DummyConsumer.count_messages(consumer1_pid)
      consumer2_count = Pulsar.DummyConsumer.count_messages(consumer2_pid)
      total_messages = consumer1_count + consumer2_count

      Logger.info("Consumer 1 received #{consumer1_count} messages")
      Logger.info("Consumer 2 received #{consumer2_count} messages")
      Logger.info("Total messages received: #{total_messages}")

      # In Shared mode with round-robin distribution, both consumers should receive messages
      # This is more predictable than Key_Shared since it's not based on key hashing
      assert consumer1_count > 0, "Consumer 1 should receive at least one message"
      assert consumer2_count > 0, "Consumer 2 should receive at least one message"
    end

    test "Failover subscription with multiple consumers" do
      {:ok, group_pid} =
        Pulsar.start_consumer(
          @test_topic,
          @test_subscription <> "-failover",
          :Failover,
          Pulsar.DummyConsumer,
          consumer_count: 2
        )

      consumer_pids = Pulsar.ConsumerGroup.list_consumers(group_pid)
      assert length(consumer_pids) == 2
      [consumer1_pid, consumer2_pid] = consumer_pids

      Process.sleep(3000)
      TestHelper.produce_messages(@test_topic, @messages)
      Process.sleep(3000)

      # Get messages from each consumer
      consumer1_messages = Pulsar.DummyConsumer.get_messages(consumer1_pid)
      consumer2_messages = Pulsar.DummyConsumer.get_messages(consumer2_pid)

      consumer1_count = length(consumer1_messages)
      consumer2_count = length(consumer2_messages)
      total_messages = consumer1_count + consumer2_count

      Logger.info("Consumer 1 received #{consumer1_count} messages")
      Logger.info("Consumer 2 received #{consumer2_count} messages")
      Logger.info("Total messages received: #{total_messages}")

      # In Failover mode, only one consumer (the active one) should receive all messages
      # The other consumer should be in standby mode and receive no messages
      assert total_messages == Enum.count(@messages), "All messages should be consumed"

      # One consumer should receive all messages, the other should receive none
      assert (consumer1_count == Enum.count(@messages) and consumer2_count == 0) or
               (consumer1_count == 0 and consumer2_count == Enum.count(@messages)),
             "In Failover mode, only one consumer should be active and receive all messages. Got consumer1: #{consumer1_count}, consumer2: #{consumer2_count}"
    end

    test "Exclusive subscription with multiple consumers" do
      # In Exclusive mode, only one consumer should be allowed to subscribe
      # When we try to start multiple consumers, the consumer group should fail
      # because exclusive subscriptions only allow one consumer at a time
      result =
        Pulsar.start_consumer(
          @test_topic,
          @test_subscription <> "-exclusive-multi",
          :Exclusive,
          Pulsar.DummyConsumer,
          consumer_count: 2
        )

      # This should fail because exclusive subscriptions don't allow multiple consumers
      assert {:error, _reason} = result
    end

    test "Exclusive subscription with single consumer" do
      # Test that exclusive subscription works correctly with a single consumer
      {:ok, group_pid} =
        Pulsar.start_consumer(
          @test_topic,
          @test_subscription <> "-exclusive-single",
          :Exclusive,
          Pulsar.DummyConsumer,
          consumer_count: 1
        )

      consumer_pids = Pulsar.ConsumerGroup.list_consumers(group_pid)
      assert length(consumer_pids) == 1
      [consumer1_pid] = consumer_pids

      Process.sleep(3000)
      TestHelper.produce_messages(@test_topic, @messages)
      Process.sleep(3000)

      consumer1_messages = Pulsar.DummyConsumer.get_messages(consumer1_pid)
      consumer1_count = length(consumer1_messages)

      assert consumer1_count == Enum.count(@messages),
             "Exclusive consumer should receive all messages. Expected: #{Enum.count(@messages)}, Got: #{consumer1_count}"
    end
  end
end
