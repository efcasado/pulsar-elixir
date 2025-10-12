defmodule Pulsar.Integration.ConsumerTest do
  use ExUnit.Case
  require Logger
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @topic "persistent://public/default/integration-test-topic"
  @subscription "integration-test-subscription"
  @consumer_callback Pulsar.Test.Support.DummyConsumer
  @messages [
    {"key1", "Message 1 for key1 - #{:os.system_time(:millisecond)}"},
    {"key2", "Message 1 for key2 - #{:os.system_time(:millisecond)}"},
    {"key1", "Message 2 for key1 - #{:os.system_time(:millisecond)}"},
    {"key2", "Message 2 for key2 - #{:os.system_time(:millisecond)}"},
    {"key3", "Message 1 for key3 - #{:os.system_time(:millisecond)}"},
    {"key4", "Message 1 for key4 - #{:os.system_time(:millisecond)}"}
  ]

  setup_all do
    :ok = System.start_pulsar()

    on_exit(fn ->
      :ok = System.stop_pulsar()
    end)
  end

  setup do
    broker = System.broker()

    config = [
      host: broker.service_url
    ]

    {:ok, app_pid} = Pulsar.start(config)

    on_exit(fn ->
      Process.exit(app_pid, :shutdown)
      Utils.wait_for(fn -> not Process.alive?(app_pid) end)
    end)
  end

  describe "Consumer Integration" do
    test "produce and consume messages" do
      {:ok, group_pid} =
        Pulsar.start_consumer(
          topic: @topic,
          subscription_name: @subscription <> "-e2e",
          subscription_type: :Shared,
          callback_module: @consumer_callback
        )

      [consumer_pid] = Pulsar.consumers_for_group(group_pid)

      System.produce_messages(@topic, @messages)

      Utils.wait_for(fn ->
        consumer_count = @consumer_callback.count_messages(consumer_pid)
        Enum.count(@messages) == consumer_count
      end)

      message_count = @consumer_callback.count_messages(consumer_pid)

      assert message_count == Enum.count(@messages)
    end

    test "Key_Shared subscription with multiple consumers" do
      {:ok, group_pid} =
        Pulsar.start_consumer(
          topic: @topic,
          subscription_name: @subscription <> "-key-shared",
          subscription_type: :Key_Shared,
          callback_module: @consumer_callback,
          opts: [consumer_count: 2]
        )

      [consumer1_pid, consumer2_pid] = Pulsar.consumers_for_group(group_pid)

      System.produce_messages(@topic, @messages)

      Utils.wait_for(fn ->
        consumer1_count = @consumer_callback.count_messages(consumer1_pid)
        consumer2_count = @consumer_callback.count_messages(consumer2_pid)
        Enum.count(@messages) == consumer1_count + consumer2_count
      end)

      consumer1_messages = @consumer_callback.get_messages(consumer1_pid)
      consumer2_messages = @consumer_callback.get_messages(consumer2_pid)

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
      assert consumer1_count > 0
      assert consumer2_count > 0

      # Verify key partitioning - no key should be consumed by both consumers
      key_overlap = MapSet.intersection(consumer1_keys, consumer2_keys)

      assert MapSet.size(key_overlap) == 0
    end

    test "Shared subscription with multiple consumers (round-robin)" do
      {:ok, group_pid} =
        Pulsar.start_consumer(
          topic: @topic,
          subscription_name: @subscription <> "-shared",
          subscription_type: :Shared,
          callback_module: @consumer_callback,
          opts: [consumer_count: 2]
        )

      [consumer1_pid, consumer2_pid] = Pulsar.consumers_for_group(group_pid)

      System.produce_messages(@topic, @messages)

      Utils.wait_for(fn ->
        consumer1_count = @consumer_callback.count_messages(consumer1_pid)
        consumer2_count = @consumer_callback.count_messages(consumer2_pid)
        Enum.count(@messages) == consumer1_count + consumer2_count
      end)

      consumer1_count = @consumer_callback.count_messages(consumer1_pid)
      consumer2_count = @consumer_callback.count_messages(consumer2_pid)
      total_messages = consumer1_count + consumer2_count

      # In Shared mode with round-robin distribution, both consumers should receive messages
      # This is more predictable than Key_Shared since it's not based on key hashing
      assert total_messages == Enum.count(@messages)
      assert consumer1_count > 0
      assert consumer2_count > 0
    end

    test "Failover subscription with multiple consumers" do
      {:ok, group_pid} =
        Pulsar.start_consumer(
          topic: @topic,
          subscription_name: @subscription <> "-failover",
          subscription_type: :Failover,
          callback_module: @consumer_callback,
          opts: [consumer_count: 2]
        )

      [consumer1_pid, consumer2_pid] = Pulsar.consumers_for_group(group_pid)

      System.produce_messages(@topic, @messages)

      Utils.wait_for(fn ->
        consumer1_count = @consumer_callback.count_messages(consumer1_pid)
        consumer2_count = @consumer_callback.count_messages(consumer2_pid)
        Enum.count(@messages) == consumer1_count + consumer2_count
      end)

      consumer1_messages = @consumer_callback.get_messages(consumer1_pid)
      consumer2_messages = @consumer_callback.get_messages(consumer2_pid)

      consumer1_count = length(consumer1_messages)
      consumer2_count = length(consumer2_messages)
      total_messages = consumer1_count + consumer2_count

      # In Failover mode, only one consumer (the active one) should receive all messages
      # The other consumer should be in standby mode and receive no messages
      assert total_messages == Enum.count(@messages)

      assert (consumer1_count == Enum.count(@messages) and consumer2_count == 0) or
               (consumer1_count == 0 and consumer2_count == Enum.count(@messages))
    end

    test "Exclusive subscription with multiple consumers" do
      # In Exclusive mode, only one consumer should be allowed to subscribe
      # When we try to start multiple consumers, the consumer group should fail
      # because exclusive subscriptions only allow one consumer at a time
      result =
        Pulsar.start_consumer(
          topic: @topic,
          subscription_name: @subscription <> "-exclusive-multi",
          subscription_type: :Exclusive,
          callback_module: @consumer_callback,
          opts: [consumer_count: 2]
        )

      assert {:error, _reason} = result
    end

    test "Exclusive subscription with single consumer" do
      # Test that exclusive subscription works correctly with a single consumer
      {:ok, group_pid} =
        Pulsar.start_consumer(
          topic: @topic,
          subscription_name: @subscription <> "-exclusive-single",
          subscription_type: :Exclusive,
          callback_module: @consumer_callback,
          opts: [consumer_count: 1]
        )

      [consumer1_pid] = Pulsar.consumers_for_group(group_pid)

      System.produce_messages(@topic, @messages)

      Utils.wait_for(fn ->
        consumer1_count = @consumer_callback.count_messages(consumer1_pid)
        Enum.count(@messages) == consumer1_count
      end)

      consumer1_messages = @consumer_callback.get_messages(consumer1_pid)
      consumer1_count = length(consumer1_messages)

      assert consumer1_count == Enum.count(@messages)
    end
  end
end
