defmodule Pulsar.Integration.ConsumerTest do
  use ExUnit.Case

  import TelemetryTest

  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  require Logger

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

  test "subscription types and settings" do
    topic = @topic_prefix <> "comprehensive-test"
    non_existent_topic = @topic_prefix <> "non-existent"

    # Start core subscription type consumers
    {:ok, shared_group} =
      Pulsar.start_consumer(
        topic,
        @subscription_prefix <> "shared-test",
        @consumer_callback,
        subscription_type: :Shared,
        consumer_count: 2
      )

    {:ok, key_shared_group} =
      Pulsar.start_consumer(
        topic,
        @subscription_prefix <> "key-shared-test",
        @consumer_callback,
        subscription_type: :Key_Shared,
        consumer_count: 2
      )

    {:ok, failover_group} =
      Pulsar.start_consumer(
        topic,
        @subscription_prefix <> "failover-test",
        @consumer_callback,
        subscription_type: :Failover,
        consumer_count: 2
      )

    {:ok, exclusive_group} =
      Pulsar.start_consumer(
        topic,
        @subscription_prefix <> "exclusive-test",
        @consumer_callback,
        subscription_type: :Exclusive
      )

    # Get core consumer PIDs
    [shared_consumer1, shared_consumer2] = Pulsar.get_consumers(shared_group)
    [key_shared_consumer1, key_shared_consumer2] = Pulsar.get_consumers(key_shared_group)
    [failover_consumer1, failover_consumer2] = Pulsar.get_consumers(failover_group)
    [exclusive_consumer] = Pulsar.get_consumers(exclusive_group)

    # Publish messages once for core subscription type tests
    System.produce_messages(topic, @messages)
    expected_count = Enum.count(@messages)

    # Wait for core consumers to receive messages
    Utils.wait_for(fn ->
      shared_total =
        @consumer_callback.count_messages(shared_consumer1) +
          @consumer_callback.count_messages(shared_consumer2)

      key_shared_total =
        @consumer_callback.count_messages(key_shared_consumer1) +
          @consumer_callback.count_messages(key_shared_consumer2)

      failover_total =
        @consumer_callback.count_messages(failover_consumer1) +
          @consumer_callback.count_messages(failover_consumer2)

      exclusive_total = @consumer_callback.count_messages(exclusive_consumer)

      shared_total == expected_count and
        key_shared_total == expected_count and
        failover_total == expected_count and
        exclusive_total == expected_count
    end)

    # Verify Shared subscription behavior
    shared_count1 = @consumer_callback.count_messages(shared_consumer1)
    shared_count2 = @consumer_callback.count_messages(shared_consumer2)
    assert shared_count1 + shared_count2 == expected_count
    assert shared_count1 > 0
    assert shared_count2 > 0

    # Verify Key_Shared subscription behavior (key partitioning)
    key_shared_messages1 = @consumer_callback.get_messages(key_shared_consumer1)
    key_shared_messages2 = @consumer_callback.get_messages(key_shared_consumer2)
    key_shared_count1 = length(key_shared_messages1)
    key_shared_count2 = length(key_shared_messages2)

    assert key_shared_count1 + key_shared_count2 == expected_count
    assert key_shared_count1 > 0
    assert key_shared_count2 > 0

    # Extract partition keys to verify no key overlap
    extract_keys = fn messages ->
      messages
      |> Enum.map(& &1.partition_key)
      |> Enum.filter(&(&1 != nil))
      |> MapSet.new()
    end

    key_shared_keys1 = extract_keys.(key_shared_messages1)
    key_shared_keys2 = extract_keys.(key_shared_messages2)
    key_overlap = MapSet.intersection(key_shared_keys1, key_shared_keys2)
    assert MapSet.size(key_overlap) == 0

    # Verify Failover subscription behavior (only one active consumer)
    failover_count1 = @consumer_callback.count_messages(failover_consumer1)
    failover_count2 = @consumer_callback.count_messages(failover_consumer2)
    assert failover_count1 + failover_count2 == expected_count

    assert (failover_count1 == expected_count and failover_count2 == 0) or
             (failover_count1 == 0 and failover_count2 == expected_count)

    # Verify Exclusive subscription behavior
    exclusive_count = @consumer_callback.count_messages(exclusive_consumer)
    assert exclusive_count == expected_count

    # Now test initial position behaviors (after messages are already published)
    {:ok, latest_group} =
      Pulsar.start_consumer(
        topic,
        @subscription_prefix <> "latest-test",
        @consumer_callback,
        subscription_type: :Shared,
        initial_position: :latest
      )

    {:ok, earliest_group} =
      Pulsar.start_consumer(
        topic,
        @subscription_prefix <> "earliest-test",
        @consumer_callback,
        subscription_type: :Shared,
        initial_position: :earliest
      )

    [latest_consumer] = Pulsar.get_consumers(latest_group)
    [earliest_consumer] = Pulsar.get_consumers(earliest_group)

    Utils.wait_for(fn ->
      earliest_count = @consumer_callback.count_messages(earliest_consumer)
      earliest_count == expected_count
    end)

    # Latest consumer should get no messages (no new messages published after it started)
    latest_count = @consumer_callback.count_messages(latest_consumer)
    assert latest_count == 0

    # Earliest consumer should get all existing messages
    earliest_count = @consumer_callback.count_messages(earliest_consumer)
    assert earliest_count == expected_count

    # Test consuming from specific message ID and timestamp - reuse existing topic and messages
    [message11, message12 | _] = @consumer_callback.get_messages(exclusive_consumer)
    publish_time = publish_time_from_message(message12)
    # Wednesday, January 1, 3000 1:00:00 AM
    future_timestamp = 32_503_683_600_000

    # Start message ID and timestamp consumers
    {:ok, message_id_group} =
      Pulsar.start_consumer(
        topic,
        @subscription_prefix <> "start-from-message-id",
        @consumer_callback,
        subscription_type: :Exclusive,
        start_message_id: message12.id
      )

    {:ok, timestamp_group1} =
      Pulsar.start_consumer(
        topic,
        @subscription_prefix <> "start-from-timestamp-1",
        @consumer_callback,
        subscription_type: :Exclusive,
        start_timestamp: publish_time
      )

    {:ok, timestamp_group2} =
      Pulsar.start_consumer(
        topic,
        @subscription_prefix <> "start-from-timestamp-2",
        @consumer_callback,
        subscription_type: :Exclusive,
        start_timestamp: 0
      )

    {:ok, timestamp_group3} =
      Pulsar.start_consumer(
        topic,
        @subscription_prefix <> "start-from-timestamp-3",
        @consumer_callback,
        subscription_type: :Exclusive,
        start_timestamp: future_timestamp
      )

    [message_id_consumer] = Pulsar.get_consumers(message_id_group)
    [timestamp_consumer1] = Pulsar.get_consumers(timestamp_group1)
    [timestamp_consumer2] = Pulsar.get_consumers(timestamp_group2)
    [timestamp_consumer3] = Pulsar.get_consumers(timestamp_group3)

    # Wait for all positioning-based consumers to receive their expected messages
    Utils.wait_for(fn ->
      message_id_count = @consumer_callback.count_messages(message_id_consumer)
      timestamp1_messages = @consumer_callback.get_messages(timestamp_consumer1)
      timestamp2_messages = @consumer_callback.get_messages(timestamp_consumer2)

      message_id_count == expected_count - 1 and
        timestamp1_messages != [] and
        timestamp2_messages != []
    end)

    # Verify message ID consumer starts from correct message
    [first_received_message | _] = @consumer_callback.get_messages(message_id_consumer)
    assert first_received_message.payload == message12.payload

    # Verify timestamp-based consumption works with ordered messages from exclusive consumer
    [timestamp_message1 | _] = @consumer_callback.get_messages(timestamp_consumer1)
    [timestamp_message2 | _] = @consumer_callback.get_messages(timestamp_consumer2)
    future_messages = @consumer_callback.get_messages(timestamp_consumer3)

    assert timestamp_message1.payload == message12.payload
    assert timestamp_message2.payload == message11.payload
    assert future_messages == []

    # Test durability behaviors (don't need messages, just subscription persistence)
    {:ok, durable_group} =
      Pulsar.start_consumer(
        topic,
        @subscription_prefix <> "durable-test",
        @consumer_callback,
        subscription_type: :Shared,
        durable: true
      )

    {:ok, non_durable_group} =
      Pulsar.start_consumer(
        topic,
        @subscription_prefix <> "non-durable-test",
        @consumer_callback,
        subscription_type: :Shared,
        durable: false
      )

    [durable_consumer] = Pulsar.get_consumers(durable_group)
    [non_durable_consumer] = Pulsar.get_consumers(non_durable_group)

    # Stop both consumers to test subscription persistence
    :ok = Pulsar.stop_consumer(durable_consumer)
    :ok = Pulsar.stop_consumer(non_durable_consumer)

    # Start failing consumers first so they can fail in background while we test successful scenarios
    {:ok, exclusive_multi_group} =
      Pulsar.start_consumer(
        topic,
        @subscription_prefix <> "exclusive-multi-fail",
        @consumer_callback,
        subscription_type: :Exclusive,
        consumer_count: 2
      )

    {:ok, no_force_create_group} =
      Pulsar.start_consumer(
        non_existent_topic,
        @subscription_prefix <> "no-force-create",
        @consumer_callback,
        subscription_type: :Shared,
        force_create_topic: false
      )

    # Wait for all failing/stopped consumers to complete their lifecycle
    Utils.wait_for(fn ->
      not Process.alive?(durable_consumer) and
        not Process.alive?(non_durable_consumer) and
        not Process.alive?(exclusive_multi_group) and
        not Process.alive?(no_force_create_group)
    end)

    # Verify durability behavior
    {:ok, subscriptions} = System.topic_subscriptions(topic)
    durable_subscription_name = @subscription_prefix <> "durable-test"
    non_durable_subscription_name = @subscription_prefix <> "non-durable-test"
    assert durable_subscription_name in subscriptions
    refute non_durable_subscription_name in subscriptions

    # Verify failure scenarios completed as expected
    assert Process.alive?(exclusive_multi_group) == false
    assert Process.alive?(no_force_create_group) == false
  end

  test "partitioned consumers" do
    topic = @topic_prefix <> "partitioned"
    System.create_topic(topic, 3)

    {:ok, partitioned_consumer_pid} =
      Pulsar.start_consumer(
        topic,
        @subscription_prefix <> "partitioned",
        @consumer_callback,
        subscription_type: :Shared,
        consumer_count: 2
      )

    # Get all consumer processes from the PartitionedConsumer
    consumers = Pulsar.get_consumers(partitioned_consumer_pid)

    System.produce_messages(topic, @messages)

    Utils.wait_for(fn ->
      consumers
      |> Enum.reduce(0, fn consumer_pid, acc ->
        @consumer_callback.count_messages(consumer_pid) + acc
      end)
      |> Kernel.==(Enum.count(@messages))
    end)

    consumed_messages =
      Enum.reduce(consumers, 0, fn consumer_pid, acc ->
        @consumer_callback.count_messages(consumer_pid) + acc
      end)

    # Get partition groups to verify structure
    partition_groups = Pulsar.PartitionedConsumer.get_partition_groups(partitioned_consumer_pid)

    # The number of partition groups should be equal to the number of
    # partitions in the topic. The total number of consumers should be
    # equal to the number of partitions times the number of consumers per
    # partition. Last but not least, all messages produced should be
    # consumed.
    assert length(partition_groups) == 3
    assert Enum.count(consumers) == 6
    assert consumed_messages == Enum.count(@messages)
  end

  @tag telemetry_listen: [[:pulsar, :consumer, :flow_control, :stop]]
  test "flow control" do
    topic = @topic_prefix <> "flow-control"

    {:ok, tiny_permits_group} =
      Pulsar.start_consumer(
        topic,
        @subscription_prefix <> "tiny-permits",
        @consumer_callback,
        subscription_type: :Shared,
        flow_initial: 1,
        flow_threshold: 0,
        flow_refill: 1
      )

    {:ok, group_shared_pid} =
      Pulsar.start_consumer(
        topic,
        @subscription_prefix <> "group-shared",
        @consumer_callback,
        subscription_type: :Shared,
        consumer_count: 2,
        flow_initial: 5,
        flow_threshold: 1,
        flow_refill: 4
      )

    [tiny_permits_consumer] = Pulsar.get_consumers(tiny_permits_group)

    [shared_consumer1, shared_consumer2] = Pulsar.get_consumers(group_shared_pid)

    # Publish messages to both topics
    System.produce_messages(topic, @messages)
    expected_count = Enum.count(@messages)

    # Wait for all consumers to process messages
    Utils.wait_for(fn ->
      tiny_permits_count = @consumer_callback.count_messages(tiny_permits_consumer)
      shared_count1 = @consumer_callback.count_messages(shared_consumer1)
      shared_count2 = @consumer_callback.count_messages(shared_consumer2)

      tiny_permits_count == expected_count and
        shared_count1 + shared_count2 == expected_count
    end)

    # Get consumer IDs for telemetry verification
    tiny_permits_id = tiny_permits_consumer |> :sys.get_state() |> Map.get(:consumer_id)
    shared_consumer1_id = shared_consumer1 |> :sys.get_state() |> Map.get(:consumer_id)
    shared_consumer2_id = shared_consumer2 |> :sys.get_state() |> Map.get(:consumer_id)

    # Collect and verify flow control statistics
    stats = Utils.collect_flow_stats()

    # Tiny permits consumer: 1 initial request + 6 refills (one per message) = 7 total events
    # Each event requests 1 permit, so requested_total should be 7
    assert %{^tiny_permits_id => %{event_count: 7, requested_total: 7}} = stats

    # Shared consumers: Each gets initial request of 5 permits
    # Since messages (6 total) are distributed between 2 consumers (~3 each),
    # neither should hit threshold so we expect only 1 event each (initial request)
    assert %{
             ^shared_consumer1_id => %{event_count: 1, requested_total: 5},
             ^shared_consumer2_id => %{event_count: 1, requested_total: 5}
           } = stats
  end

  defp publish_time_from_message(message) do
    %{
      metadata: %Binary.MessageMetadata{
        publish_time: publish_time
      }
    } = message

    publish_time
  end
end
