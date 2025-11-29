defmodule Pulsar.Integration.Consumer.SubscriptionOptionsTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

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

    {:ok, client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    {:ok, producer_pid} =
      Pulsar.start_producer(
        @topic,
        client: @client,
        name: :subscription_options_producer
      )

    for {key, payload} <- @messages do
      Pulsar.send(:subscription_options_producer, payload, partition_key: key, client: @client)
    end

    on_exit(fn ->
      if Process.alive?(producer_pid), do: Pulsar.stop_producer(producer_pid)
      if Process.alive?(client_pid), do: Supervisor.stop(client_pid, :normal)
    end)

    {:ok, expected_count: Enum.count(@messages)}
  end

  setup [:telemetry_listen]

  test "initial_position latest skips existing messages", %{expected_count: _expected_count} do
    {:ok, latest_group} =
      Pulsar.start_consumer(
        @topic,
        "latest",
        @consumer_callback,
        subscription_options(:latest)
      )

    [consumer] = Pulsar.get_consumers(latest_group)

    # Give it time to potentially receive messages (if bug)
    Process.sleep(1000)

    # Should get no messages since they were published before subscription
    count = @consumer_callback.count_messages(consumer)
    assert count == 0
  end

  test "initial_position earliest reads all messages", %{expected_count: expected_count} do
    {:ok, earliest_group} =
      Pulsar.start_consumer(
        @topic,
        "earliest",
        @consumer_callback,
        subscription_options(:earliest)
      )

    [consumer] = Pulsar.get_consumers(earliest_group)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == expected_count
    end)

    count = @consumer_callback.count_messages(consumer)
    assert count == expected_count
  end

  test "consuming from specific message ID", %{expected_count: expected_count} do
    # First consumer to get messages and determine starting point
    {:ok, setup_group} =
      Pulsar.start_consumer(
        @topic,
        "setup-message-id",
        @consumer_callback,
        subscription_options(:earliest)
      )

    [setup_consumer] = Pulsar.get_consumers(setup_group)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(setup_consumer) == expected_count
    end)

    [_message1, message2 | _] = @consumer_callback.get_messages(setup_consumer)

    # Start consumer from second message
    {:ok, message_id_group} =
      Pulsar.start_consumer(
        @topic,
        "from-message-id",
        @consumer_callback,
        subscription_options(:earliest, start_message_id: message2.id)
      )

    [message_id_consumer] = Pulsar.get_consumers(message_id_group)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(message_id_consumer) == expected_count - 1
    end)

    [first_received | _] = @consumer_callback.get_messages(message_id_consumer)
    assert first_received.payload == message2.payload
  end

  test "consuming from timestamp", %{expected_count: expected_count} do
    # First consumer to get messages and determine timestamp
    {:ok, setup_group} =
      Pulsar.start_consumer(
        @topic,
        "setup-timestamp",
        @consumer_callback,
        subscription_options(:earliest)
      )

    [setup_consumer] = Pulsar.get_consumers(setup_group, true)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(setup_consumer) == expected_count
    end)

    [message1, message2 | _] = @consumer_callback.get_messages(setup_consumer)
    publish_time = publish_time_from_message(message2)

    # Start from second message's timestamp
    {:ok, timestamp_group1} =
      Pulsar.start_consumer(
        @topic,
        "from-timestamp-1",
        @consumer_callback,
        subscription_options(:earliest, start_timestamp: publish_time)
      )

    # Start from beginning (timestamp 0)
    {:ok, timestamp_group2} =
      Pulsar.start_consumer(
        @topic,
        "from-timestamp-2",
        @consumer_callback,
        subscription_options(:earliest, start_timestamp: 0)
      )

    # Start from future (should get no messages)
    future_timestamp = 32_503_683_600_000

    {:ok, timestamp_group3} =
      Pulsar.start_consumer(
        @topic,
        "from-timestamp-3",
        @consumer_callback,
        subscription_options(:earliest, start_timestamp: future_timestamp)
      )

    [timestamp_consumer1] = Pulsar.get_consumers(timestamp_group1)
    [timestamp_consumer2] = Pulsar.get_consumers(timestamp_group2)
    [timestamp_consumer3] = Pulsar.get_consumers(timestamp_group3)

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
      Pulsar.start_consumer(
        @topic,
        "durable",
        @consumer_callback,
        subscription_options(:earliest, durable: true)
      )

    [durable_consumer] = Pulsar.get_consumers(durable_group)

    :ok = Pulsar.stop_consumer(durable_consumer)

    Utils.wait_for(fn -> not Process.alive?(durable_consumer) end)

    {:ok, subscriptions} = System.topic_subscriptions(@topic)
    assert "durable" in subscriptions
  end

  test "non-durable subscription is removed after consumer stops" do
    {:ok, non_durable_group} =
      Pulsar.start_consumer(
        @topic,
        "non-durable",
        @consumer_callback,
        subscription_options(:earliest, durable: false)
      )

    [non_durable_consumer] = Pulsar.get_consumers(non_durable_group)

    :ok = Pulsar.stop_consumer(non_durable_consumer)

    Utils.wait_for(fn -> not Process.alive?(non_durable_consumer) end)

    {:ok, subscriptions} = System.topic_subscriptions(@topic)
    refute "non-durable" in subscriptions
  end

  test "consumer fails when force_create_topic is false and topic does not exist" do
    non_existent_topic = "persistent://public/default/subscription-options-non-existent"

    {:ok, no_force_create_group} =
      Pulsar.start_consumer(
        non_existent_topic,
        "no-force-create",
        @consumer_callback,
        subscription_options(:earliest, force_create_topic: false)
      )

    Utils.wait_for(fn -> not Process.alive?(no_force_create_group) end)

    assert Process.alive?(no_force_create_group) == false
  end

  defp subscription_options(initial_position, opts \\ []) do
    [
      client: @client,
      subscription_type: :Exclusive,
      initial_position: initial_position
    ] ++ opts
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
