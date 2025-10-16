defmodule Pulsar.Integration.ConsumerTest do
  use ExUnit.Case
  import TelemetryTest

  require Logger
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @topic_prefix "persistent://public/default/integration-test-topic-"
  @subscription_prefix "integration-test-subscription-"
  @consumer_callback Pulsar.Test.Support.DummyConsumer
  @messages [
    {"key1", "Message 1 for key1"},
    {"key2", "Message 1 for key2"},
    {"key1", "Message 2 for key1"},
    {"key2", "Message 2 for key2"},
    {"key3", "Message 1 for key3"},
    {"key4", "Message 1 for key4"}
  ]

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

  setup [:telemetry_listen]

  describe "Consumer Integration" do
    test "produce and consume messages" do
      topic = @topic_prefix <> "e2e"

      {:ok, [group_pid]} =
        Pulsar.start_consumer(
          topic: topic,
          subscription_name: @subscription_prefix <> "e2e",
          subscription_type: :Shared,
          callback_module: @consumer_callback
        )

      [consumer_pid] = Pulsar.consumers_for_group(group_pid)

      System.produce_messages(topic, @messages)

      Utils.wait_for(fn ->
        consumer_count = @consumer_callback.count_messages(consumer_pid)
        Enum.count(@messages) == consumer_count
      end)

      message_count = @consumer_callback.count_messages(consumer_pid)

      assert message_count == Enum.count(@messages)
    end

    test "Key_Shared subscription with multiple consumers" do
      topic = @topic_prefix <> "key-shared"

      {:ok, [group_pid]} =
        Pulsar.start_consumer(
          topic: topic,
          subscription_name: @subscription_prefix <> "key-shared",
          subscription_type: :Key_Shared,
          callback_module: @consumer_callback,
          opts: [consumer_count: 2]
        )

      [consumer1_pid, consumer2_pid] = Pulsar.consumers_for_group(group_pid)

      System.produce_messages(topic, @messages)

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
      topic = @topic_prefix <> "shared"

      {:ok, [group_pid]} =
        Pulsar.start_consumer(
          topic: topic,
          subscription_name: @subscription_prefix <> "shared",
          subscription_type: :Shared,
          callback_module: @consumer_callback,
          opts: [consumer_count: 2]
        )

      [consumer1_pid, consumer2_pid] = Pulsar.consumers_for_group(group_pid)

      System.produce_messages(topic, @messages)

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
      topic = @topic_prefix <> "failover"

      {:ok, [group_pid]} =
        Pulsar.start_consumer(
          topic: topic,
          subscription_name: @subscription_prefix <> "failover",
          subscription_type: :Failover,
          callback_module: @consumer_callback,
          opts: [consumer_count: 2]
        )

      [consumer1_pid, consumer2_pid] = Pulsar.consumers_for_group(group_pid)

      System.produce_messages(topic, @messages)

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
      topic = @topic_prefix <> "exclusive-multi"
      # In Exclusive mode, only one consumer should be allowed to subscribe
      # When we try to start multiple consumers, the consumer group should fail
      # because exclusive subscriptions only allow one consumer at a time
      result =
        Pulsar.start_consumer(
          topic: topic,
          subscription_name: @subscription_prefix <> "exclusive-multi",
          subscription_type: :Exclusive,
          callback_module: @consumer_callback,
          opts: [consumer_count: 2]
        )

      assert {:error, _reason} = result
    end

    test "Exclusive subscription with single consumer" do
      topic = @topic_prefix <> "exclusive-single"
      # Test that exclusive subscription works correctly with a single consumer
      {:ok, [group_pid]} =
        Pulsar.start_consumer(
          topic: topic,
          subscription_name: @subscription_prefix <> "exclusive-single",
          subscription_type: :Exclusive,
          callback_module: @consumer_callback,
          opts: [consumer_count: 1]
        )

      [consumer1_pid] = Pulsar.consumers_for_group(group_pid)

      System.produce_messages(topic, @messages)

      Utils.wait_for(fn ->
        consumer1_count = @consumer_callback.count_messages(consumer1_pid)
        Enum.count(@messages) == consumer1_count
      end)

      consumer1_messages = @consumer_callback.get_messages(consumer1_pid)
      consumer1_count = length(consumer1_messages)

      assert consumer1_count == Enum.count(@messages)
    end

    test "Consumer groups in a partitioned topic" do
      topic = @topic_prefix <> "partitioned"
      System.create_topic(topic, 3)

      {:ok, group_pids} =
        Pulsar.start_consumer(
          topic: topic,
          subscription_name: @subscription_prefix <> "partitioned",
          subscription_type: :Shared,
          callback_module: @consumer_callback,
          opts: [consumer_count: 2]
        )

      consumers =
        group_pids
        |> Enum.flat_map(&Pulsar.consumers_for_group(&1))

      System.produce_messages(topic, @messages)

      Utils.wait_for(fn ->
        consumers
        |> Enum.reduce(0, fn consumer_pid, acc ->
          @consumer_callback.count_messages(consumer_pid) + acc
        end)
        |> Kernel.==(Enum.count(@messages))
      end)

      consumed_messages =
        consumers
        |> Enum.reduce(0, fn consumer_pid, acc ->
          @consumer_callback.count_messages(consumer_pid) + acc
        end)

      # The number of consumer (groups) should be equal to the number of
      # partitions in the topic. The total number of consumers should be
      # equal to the number of topics times the number of consumers per
      # partition. Last but not least, all messages produced should be
      # consumed.
      assert length(group_pids) == 3
      assert Enum.count(consumers) == 6
      assert consumed_messages == Enum.count(@messages)
    end

    test "Consumer only receives new messages when initial position is set to latest" do
      topic = @topic_prefix <> "latest"
      new_message = {"key5", "Message 1 for key5"}

      System.produce_messages(topic, @messages)

      {:ok, [group_pid]} =
        Pulsar.start_consumer(
          topic: topic,
          subscription_name: @subscription_prefix <> "latest",
          subscription_type: :Shared,
          callback_module: @consumer_callback,
          opts: [initial_position: :latest]
        )

      [consumer_pid] = Pulsar.consumers_for_group(group_pid)

      System.produce_messages(topic, [new_message])

      Utils.wait_for(fn ->
        @consumer_callback.count_messages(consumer_pid) == 1
      end)

      [consumed_message] = @consumer_callback.get_messages(consumer_pid)

      # Only the new message should be consumed, not the initial ones
      assert {consumed_message.partition_key, consumed_message.payload} == new_message
    end

    test "Consumer receives all messages when initial position is set to earliest" do
      topic = @topic_prefix <> "earliest"
      new_message = {"key5", "Message 1 for key5"}

      System.produce_messages(topic, @messages)

      {:ok, [group_pid]} =
        Pulsar.start_consumer(
          topic: topic,
          subscription_name: @subscription_prefix <> "earliest",
          subscription_type: :Shared,
          callback_module: @consumer_callback,
          opts: [initial_position: :earliest]
        )

      [consumer_pid] = Pulsar.consumers_for_group(group_pid)

      System.produce_messages(topic, [new_message])

      expected_total = Enum.count(@messages) + 1

      Utils.wait_for(fn ->
        @consumer_callback.count_messages(consumer_pid) == expected_total
      end)

      consumed_messages = @consumer_callback.get_messages(consumer_pid)

      # All messages should be consumed, including the initial ones
      assert Enum.count(consumed_messages) == expected_total
    end
  end

  test "Non-durable subscription is deleted if it has no consumers" do
    topic = @topic_prefix <> "non-durable"
    subscription = @subscription_prefix <> "non-durable"

    {:ok, [group_pid]} =
      Pulsar.start_consumer(
        topic: topic,
        subscription_name: subscription,
        subscription_type: :Shared,
        callback_module: @consumer_callback,
        opts: [durable: false]
      )

    [consumer_pid] = Pulsar.consumers_for_group(group_pid)

    :ok = Pulsar.stop_consumer(consumer_pid)

    Utils.wait_for(fn ->
      not Process.alive?(consumer_pid)
    end)

    {:ok, subscriptions} = System.topic_subscriptions(topic)

    # Subscription should be removed from the list of available subscriptions
    # on the target topic
    assert subscription not in subscriptions
  end

  test "Durable subscription remains if it has no consumers" do
    topic = @topic_prefix <> "durable"
    subscription = @subscription_prefix <> "durable"

    {:ok, [group_pid]} =
      Pulsar.start_consumer(
        topic: topic,
        subscription_name: subscription,
        subscription_type: :Shared,
        callback_module: @consumer_callback,
        opts: [durable: true]
      )

    [consumer_pid] = Pulsar.consumers_for_group(group_pid)

    :ok = Pulsar.stop_consumer(consumer_pid)

    Utils.wait_for(fn ->
      not Process.alive?(consumer_pid)
    end)

    {:ok, subscriptions} = System.topic_subscriptions(topic)

    # Subscription should remain in the list of available subscriptions
    # on the target topic
    assert subscription in subscriptions
  end

  describe "Flow Control Configuration" do
    @tag telemetry_listen: [[:pulsar, :consumer, :flow_control, :stop]]
    test "consumer with one permit at a time" do
      topic = @topic_prefix <> "tiny-permits"

      {:ok, [group_pid]} =
        Pulsar.start_consumer(
          topic: topic,
          subscription_name: @subscription_prefix <> "tiny-permits",
          subscription_type: :Shared,
          callback_module: @consumer_callback,
          opts: [
            flow_initial: 1,
            flow_threshold: 0,
            flow_refill: 1
          ]
        )

      [consumer_pid] = Pulsar.consumers_for_group(group_pid)

      System.produce_messages(topic, @messages)

      Utils.wait_for(fn ->
        consumer_count = @consumer_callback.count_messages(consumer_pid)
        Enum.count(@messages) == consumer_count
      end)

      consumer_id = consumer_pid |> :sys.get_state() |> Map.get(:consumer_id)

      # We expect: 1 initial request + 6 refills (one per message) = 7 total events
      # Each event requests 1 permit, so requested_total should be 7
      stats = flow_control_stats()
      assert %{^consumer_id => %{event_count: 7, requested_total: 7}} = stats
    end

    @tag telemetry_listen: [[:pulsar, :consumer, :flow_control, :stop]]
    test "multiple consumers in same group with shared flow control settings" do
      topic = @topic_prefix <> "group-shared"

      {:ok, group_pids} =
        Pulsar.start_consumer(
          topic: topic,
          subscription_name: @subscription_prefix <> "group-shared",
          subscription_type: :Shared,
          callback_module: @consumer_callback,
          opts: [
            consumer_count: 2,
            flow_initial: 5,
            flow_threshold: 1,
            flow_refill: 4
          ]
        )

      [consumer1_pid, consumer2_pid] = Enum.flat_map(group_pids, &Pulsar.consumers_for_group(&1))

      System.produce_messages(topic, @messages)

      Utils.wait_for(fn ->
        consumer1_count = @consumer_callback.count_messages(consumer1_pid)
        consumer2_count = @consumer_callback.count_messages(consumer2_pid)
        Enum.count(@messages) == consumer1_count + consumer2_count
      end)

      consumer1_id = consumer1_pid |> :sys.get_state() |> Map.get(:consumer_id)
      consumer2_id = consumer2_pid |> :sys.get_state() |> Map.get(:consumer_id)

      # Each consumer gets initial request of 5 permits
      # Since messages (6 total) are distributed between 2 consumers (~3 each),
      # neither should hit threshold so we expect only 2 events total
      stats = flow_control_stats()

      assert %{
               ^consumer1_id => %{event_count: 1, requested_total: 5},
               ^consumer2_id => %{event_count: 1, requested_total: 5}
             } = stats
    end

    # Helper function to collect and aggregate flow control statistics from telemetry events
    # Returns a map with statistics grouped by consumer_id
    defp flow_control_stats do
      collect_flow_events([]) |> aggregate_stats()
    end

    defp collect_flow_events(acc) do
      receive do
        {:telemetry_event,
         %{
           event: [:pulsar, :consumer, :flow_control, :stop],
           metadata: metadata
         }} ->
          collect_flow_events([metadata | acc])
      after
        0 -> Enum.reverse(acc)
      end
    end

    defp aggregate_stats(events) do
      events
      |> Enum.group_by(& &1.consumer_id)
      |> Enum.map(fn {consumer_id, consumer_events} ->
        stats = %{
          consumer_id: consumer_id,
          event_count: length(consumer_events),
          requested_total: Enum.sum(Enum.map(consumer_events, & &1.permits_requested))
        }

        {consumer_id, stats}
      end)
      |> Map.new()
    end
  end
end
