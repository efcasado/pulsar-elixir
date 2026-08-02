defmodule Pulsar.Integration.Consumer.SubscriptionOptionsTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils
  alias Pulsar.Topology

  @moduletag :integration
  @client :subscription_options_test_client
  @topic "persistent://public/default/subscription-options-test"
  @consumer_callback Pulsar.Test.Support.DummyConsumer
  @messages [
    {"key1", "Message 1 for key1"},
    {"key2", "Message 1 for key2"},
    {"key1", "Message 2 for key1"},
    {"key2", "Message 2 for key2"},
    {"key3", "Message 1 for key3"},
    {"key4", "Message 1 for key4"}
  ]

  setup_all do
    broker = System.broker()

    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    {:ok, _producer_pid} =
      Pulsar.Producer.start(
        @topic,
        client: @client,
        name: :subscription_options_producer
      )

    for {key, payload} <- @messages do
      Utils.wait_for(
        fn ->
          Pulsar.Producer.send(:subscription_options_producer, payload, partition_key: key, client: @client)
        end,
        until: &match?({:ok, _message_id}, &1)
      )
    end

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)

    {:ok, expected_count: Enum.count(@messages)}
  end

  setup [:telemetry_listen]

  test ":name names the group, and each consumer is named after it on the broker" do
    {:ok, group} =
      Pulsar.Consumer.start(
        "persistent://public/default/consumer-naming",
        "naming",
        @consumer_callback,
        client: @client,
        name: "naming-group",
        consumer_count: 2
      )

    on_exit(fn -> Pulsar.Consumer.stop("naming-group", client: @client) end)

    workers =
      Utils.wait_for(fn -> Topology.workers(group) end,
        until: fn workers -> length(workers) == 2 end
      )

    names =
      workers
      |> Enum.map(fn pid -> :sys.get_state(pid).consumer_name end)
      |> Enum.sort()

    assert names == ["naming-group-1", "naming-group-2"]
  end

  test "initial_position latest skips existing messages", %{expected_count: _expected_count} do
    {:ok, latest_group} =
      Pulsar.Consumer.start(
        @topic,
        "latest",
        @consumer_callback,
        subscription_options(:latest)
      )

    [consumer] = Utils.wait_for(fn -> Topology.workers(latest_group) end, until: &match?([_], &1))

    # Give it time to potentially receive messages (if bug)
    Process.sleep(1000)

    # Should get no messages since they were published before subscription
    count = @consumer_callback.count_messages(consumer)
    assert count == 0
  end

  test "initial_position earliest reads all messages", %{expected_count: expected_count} do
    {:ok, earliest_group} =
      Pulsar.Consumer.start(
        @topic,
        "earliest",
        @consumer_callback,
        subscription_options(:earliest)
      )

    [consumer] = Utils.wait_for(fn -> Topology.workers(earliest_group) end, until: &match?([_], &1))

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == expected_count
    end)

    count = @consumer_callback.count_messages(consumer)
    assert count == expected_count
  end

  test "consuming from specific message ID", %{expected_count: expected_count} do
    # First consumer to get messages and determine starting point
    {:ok, setup_group} =
      Pulsar.Consumer.start(
        @topic,
        "setup-message-id",
        @consumer_callback,
        subscription_options(:earliest)
      )

    [setup_consumer] = Utils.wait_for(fn -> Topology.workers(setup_group) end, until: &match?([_], &1))

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(setup_consumer) == expected_count
    end)

    [_message1, message2 | _] = @consumer_callback.get_messages(setup_consumer)

    # Start consumer from second message
    message_id = {message2.command.message_id.ledgerId, message2.command.message_id.entryId}

    {:ok, message_id_group} =
      Pulsar.Consumer.start(
        @topic,
        "from-message-id",
        @consumer_callback,
        subscription_options(:earliest, start_message_id: message_id)
      )

    [message_id_consumer] =
      Utils.wait_for(fn -> Topology.workers(message_id_group) end, until: &match?([_], &1))

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(message_id_consumer) == expected_count - 1
    end)

    [first_received | _] = @consumer_callback.get_messages(message_id_consumer)
    assert first_received.payload == message2.payload
  end

  test "consuming from timestamp", %{expected_count: expected_count} do
    # First consumer to get messages and determine timestamp
    {:ok, setup_group} =
      Pulsar.Consumer.start(
        @topic,
        "setup-timestamp",
        @consumer_callback,
        subscription_options(:earliest)
      )

    [setup_consumer] = Utils.wait_for(fn -> Topology.workers(setup_group) end, until: &match?([_], &1))

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(setup_consumer) == expected_count
    end)

    [message1, message2 | _] = @consumer_callback.get_messages(setup_consumer)
    publish_time = publish_time_from_message(message2)

    # Start from second message's timestamp
    {:ok, timestamp_group1} =
      Pulsar.Consumer.start(
        @topic,
        "from-timestamp-1",
        @consumer_callback,
        subscription_options(:earliest, start_timestamp: publish_time)
      )

    # Start from beginning (timestamp 0)
    {:ok, timestamp_group2} =
      Pulsar.Consumer.start(
        @topic,
        "from-timestamp-2",
        @consumer_callback,
        subscription_options(:earliest, start_timestamp: 0)
      )

    # Start from future (should get no messages)
    future_timestamp = 32_503_683_600_000

    {:ok, timestamp_group3} =
      Pulsar.Consumer.start(
        @topic,
        "from-timestamp-3",
        @consumer_callback,
        subscription_options(:earliest, start_timestamp: future_timestamp)
      )

    [timestamp_consumer1] =
      Utils.wait_for(fn -> Topology.workers(timestamp_group1) end, until: &match?([_], &1))

    [timestamp_consumer2] =
      Utils.wait_for(fn -> Topology.workers(timestamp_group2) end, until: &match?([_], &1))

    [timestamp_consumer3] =
      Utils.wait_for(fn -> Topology.workers(timestamp_group3) end, until: &match?([_], &1))

    Utils.wait_for(fn ->
      messages1 = @consumer_callback.get_messages(timestamp_consumer1)
      messages2 = @consumer_callback.get_messages(timestamp_consumer2)

      messages1 != [] and messages2 != []
    end)

    [first_message1 | _] = @consumer_callback.get_messages(timestamp_consumer1)
    [first_message2 | _] = @consumer_callback.get_messages(timestamp_consumer2)
    future_messages = @consumer_callback.get_messages(timestamp_consumer3)

    assert first_message1.payload == message2.payload
    assert first_message2.payload == message1.payload
    assert future_messages == []
  end

  test "durable subscription persists after consumer stops" do
    {:ok, durable_group} =
      Pulsar.Consumer.start(
        @topic,
        "durable",
        @consumer_callback,
        subscription_options(:earliest, durable: true)
      )

    [durable_consumer] = Utils.wait_for(fn -> Topology.workers(durable_group) end, until: &match?([_], &1))

    :ok = Pulsar.Consumer.stop(durable_consumer)

    Utils.wait_for(fn -> not Process.alive?(durable_consumer) end)

    {:ok, subscriptions} = System.topic_subscriptions(@topic)
    assert "durable" in subscriptions
  end

  test "non-durable subscription is removed after consumer stops" do
    {:ok, non_durable_group} =
      Pulsar.Consumer.start(
        @topic,
        "non-durable",
        @consumer_callback,
        subscription_options(:earliest, durable: false)
      )

    [non_durable_consumer] =
      Utils.wait_for(fn -> Topology.workers(non_durable_group) end, until: &match?([_], &1))

    :ok = Pulsar.Consumer.stop(non_durable_consumer)

    Utils.wait_for(fn -> not Process.alive?(non_durable_consumer) end)

    {:ok, subscriptions} = System.topic_subscriptions(@topic)
    refute "non-durable" in subscriptions
  end

  test "consumer fails when force_create_topic is false and topic does not exist" do
    non_existent_topic = "persistent://public/default/subscription-options-non-existent"

    {:ok, no_force_create_group} =
      Pulsar.Consumer.start(
        non_existent_topic,
        "no-force-create",
        @consumer_callback,
        subscription_options(:earliest, force_create_topic: false)
      )

    :ok = Utils.wait_for(fn -> Topology.status(no_force_create_group) == {:ready, :non_partitioned} end)
    :ok = Utils.wait_for(fn -> Topology.workers(no_force_create_group) == [] end)

    assert Process.alive?(no_force_create_group)
    assert no_force_create_group in Pulsar.Client.consumers(@client)
    assert {:error, :no_consumers_available} = Pulsar.Consumer.send_flow(no_force_create_group, 1)

    assert :ok = Pulsar.Consumer.stop(no_force_create_group, client: @client)
    :ok = Utils.wait_for(fn -> not Process.alive?(no_force_create_group) end)
    refute no_force_create_group in Pulsar.Client.consumers(@client)
  end

  test "read_compacted filters compacted messages", %{expected_count: expected_count} do
    :ok = System.compact_topic(@topic)

    Utils.wait_for(fn -> System.compacted_topic?(@topic) end)

    {:ok, compacted_group} =
      Pulsar.Consumer.start(
        @topic,
        "compacted-true",
        @consumer_callback,
        subscription_options(:earliest, read_compacted: true)
      )

    [compacted_consumer] =
      Utils.wait_for(fn -> Topology.workers(compacted_group) end, until: &match?([_], &1))

    {:ok, non_compacted_group} =
      Pulsar.Consumer.start(
        @topic,
        "compacted-false",
        @consumer_callback,
        subscription_options(:earliest, read_compacted: false)
      )

    [non_compacted_consumer] =
      Utils.wait_for(fn -> Topology.workers(non_compacted_group) end, until: &match?([_], &1))

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(compacted_consumer) == 4 and
        @consumer_callback.count_messages(non_compacted_consumer) == expected_count
    end)

    compacted_messages = @consumer_callback.get_messages(compacted_consumer)

    compacted_messages_map = Map.new(compacted_messages, &{&1.metadata.partition_key, &1.payload})

    assert Enum.count(compacted_messages) == 4

    assert compacted_messages_map["key1"] == "Message 2 for key1"
    assert compacted_messages_map["key2"] == "Message 2 for key2"
    assert compacted_messages_map["key3"] == "Message 1 for key3"
    assert compacted_messages_map["key4"] == "Message 1 for key4"

    non_compacted_count = @consumer_callback.count_messages(non_compacted_consumer)
    assert non_compacted_count == expected_count
  end

  defp subscription_options(initial_position, opts \\ []) do
    [
      client: @client,
      subscription_type: :Exclusive,
      initial_position: initial_position
    ] ++ opts
  end

  defp publish_time_from_message(%Pulsar.Message{metadata: metadata}) do
    metadata.publish_time
  end
end
