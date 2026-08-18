defmodule Pulsar.Integration.Consumer.SubscriptionOptionsTest do
  use Pulsar.Test.Case, async: true

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
    {:ok, producer} = Pulsar.Producer.start(@topic, client: @client, name: "subscription-options-seed")
    :ok = Pulsar.Producer.await_ready(producer)

    # "consuming from timestamp" seeks to one message's publish time, which can only name that
    # message if no two share a stamp. The producer stamps to the millisecond as it sends, so
    # these go a millisecond apart rather than together.
    for {key, payload} <- @messages do
      Process.sleep(2)
      {:ok, _message_id} = Pulsar.Producer.send(producer, payload, partition_key: key)
    end

    {:ok, expected_count: length(@messages)}
  end

  test ":name names the group, and each consumer is named after it on the broker" do
    {:ok, group} =
      Pulsar.Consumer.start(
        "persistent://public/default/consumer-naming",
        "naming",
        @consumer_callback,
        client: @client,
        name: "naming-group",
        consumer_count: 2,
        init_args: [forward_to: self()]
      )

    on_exit(fn -> Pulsar.Consumer.stop("naming-group", client: @client) end)

    :ok = Pulsar.Consumer.await_ready(group)
    workers = Topology.workers(group)

    names =
      workers
      |> Enum.map(fn pid -> :sys.get_state(pid).consumer_name end)
      |> Enum.sort()

    assert names == ["naming-group-1", "naming-group-2"]
  end

  test ":latest starts past what the topic already held" do
    {:ok, latest_group} =
      Pulsar.Consumer.start(
        @topic,
        "latest",
        @consumer_callback,
        subscription_options(:latest)
      )

    :ok = Pulsar.Consumer.await_ready(latest_group)
    [consumer] = Topology.workers(latest_group)

    refute_receive {:consumer, ^consumer, _message}, 1_000
  end

  test ":earliest starts at the beginning of the topic", %{expected_count: expected_count} do
    {:ok, earliest_group} =
      Pulsar.Consumer.start(
        @topic,
        "earliest",
        @consumer_callback,
        subscription_options(:earliest)
      )

    :ok = Pulsar.Consumer.await_ready(earliest_group)
    [consumer] = Topology.workers(earliest_group)

    for _message <- 1..expected_count, do: assert_receive({:consumer, ^consumer, _message})
  end

  test ":start_message_id starts at the message it names", %{expected_count: expected_count} do
    {:ok, setup_group} =
      Pulsar.Consumer.start(
        @topic,
        "setup-message-id",
        @consumer_callback,
        subscription_options(:earliest)
      )

    :ok = Pulsar.Consumer.await_ready(setup_group)
    [setup_consumer] = Topology.workers(setup_group)

    [_message1, message2 | _] = receive_messages(setup_consumer, expected_count)

    message_id = {message2.raw.command.message_id.ledgerId, message2.raw.command.message_id.entryId}

    {:ok, message_id_group} =
      Pulsar.Consumer.start(
        @topic,
        "from-message-id",
        @consumer_callback,
        subscription_options(:earliest, start_message_id: message_id)
      )

    :ok = Pulsar.Consumer.await_ready(message_id_group)
    [message_id_consumer] = Topology.workers(message_id_group)

    [first_received | _] = receive_messages(message_id_consumer, expected_count - 1)
    assert first_received.payload == message2.payload
  end

  test ":start_timestamp starts at the first message published at or after it", %{expected_count: expected_count} do
    {:ok, setup_group} =
      Pulsar.Consumer.start(
        @topic,
        "setup-timestamp",
        @consumer_callback,
        subscription_options(:earliest)
      )

    :ok = Pulsar.Consumer.await_ready(setup_group)
    [setup_consumer] = Topology.workers(setup_group)

    [message1, message2 | _] = receive_messages(setup_consumer, expected_count)
    publish_time = publish_time_from_message(message2)

    {:ok, timestamp_group1} =
      Pulsar.Consumer.start(
        @topic,
        "from-timestamp-1",
        @consumer_callback,
        subscription_options(:earliest, start_timestamp: publish_time)
      )

    {:ok, timestamp_group2} =
      Pulsar.Consumer.start(
        @topic,
        "from-timestamp-2",
        @consumer_callback,
        subscription_options(:earliest, start_timestamp: 0)
      )

    future_timestamp = 32_503_683_600_000

    {:ok, timestamp_group3} =
      Pulsar.Consumer.start(
        @topic,
        "from-timestamp-3",
        @consumer_callback,
        subscription_options(:earliest, start_timestamp: future_timestamp)
      )

    :ok = Pulsar.Consumer.await_ready(timestamp_group1)
    [timestamp_consumer1] = Topology.workers(timestamp_group1)

    :ok = Pulsar.Consumer.await_ready(timestamp_group2)
    [timestamp_consumer2] = Topology.workers(timestamp_group2)

    :ok = Pulsar.Consumer.await_ready(timestamp_group3)
    [timestamp_consumer3] = Topology.workers(timestamp_group3)

    assert_receive {:consumer, ^timestamp_consumer1, first_message1}
    assert_receive {:consumer, ^timestamp_consumer2, first_message2}

    assert first_message1.payload == message2.payload
    assert first_message2.payload == message1.payload
    refute_receive {:consumer, ^timestamp_consumer3, _message}
  end

  test "a durable subscription outlives the consumer that took it" do
    {:ok, durable_group} =
      Pulsar.Consumer.start(
        @topic,
        "durable",
        @consumer_callback,
        subscription_options(:earliest, durable: true)
      )

    :ok = Pulsar.Consumer.await_ready(durable_group)
    [durable_consumer] = Topology.workers(durable_group)

    ref = Process.monitor(durable_consumer)
    :ok = Pulsar.Consumer.stop(durable_group)

    assert_receive {:DOWN, ^ref, :process, ^durable_consumer, _reason}

    {:ok, subscriptions} = System.topic_subscriptions(@topic)
    assert "durable" in subscriptions
  end

  test "await_ready/2 waits for the subscription and the callback, and honours its timeout" do
    {:ok, consumer} =
      Pulsar.Consumer.start(
        @topic,
        "delayed-ready",
        @consumer_callback,
        subscription_options(:earliest, startup_delay_ms: 500)
      )

    assert :ok = Topology.await_ready(consumer, 1_000)
    assert Pulsar.Consumer.await_ready(consumer, timeout: 25) == {:error, :timeout}
    assert :ok = Pulsar.Consumer.await_ready(consumer)
    assert :ok = Pulsar.Consumer.stop(consumer, client: @client)
  end

  test "a non-durable subscription goes with the consumer that took it" do
    {:ok, non_durable_group} =
      Pulsar.Consumer.start(
        @topic,
        "non-durable",
        @consumer_callback,
        subscription_options(:earliest, durable: false)
      )

    :ok = Pulsar.Consumer.await_ready(non_durable_group)
    [non_durable_consumer] = Topology.workers(non_durable_group)

    ref = Process.monitor(non_durable_consumer)
    :ok = Pulsar.Consumer.stop(non_durable_group)

    assert_receive {:DOWN, ^ref, :process, ^non_durable_consumer, _reason}

    {:ok, subscriptions} = System.topic_subscriptions(@topic)
    refute "non-durable" in subscriptions
  end

  test "force_create_topic: false leaves the consumer running with no workers" do
    non_existent_topic = "persistent://public/default/subscription-options-non-existent"

    {:ok, no_force_create_group} =
      Pulsar.Consumer.start(
        non_existent_topic,
        "no-force-create",
        @consumer_callback,
        subscription_options(:earliest, force_create_topic: false)
      )

    :ok = Topology.await_ready(no_force_create_group, 1_000)

    # The topic does not exist and this consumer will not create one, so its worker gives up
    # rather than ever becoming ready.
    assert Pulsar.Consumer.await_ready(no_force_create_group, timeout: 2_000) == {:error, :timeout}
    assert Topology.workers(no_force_create_group) == []

    assert Process.alive?(no_force_create_group)
    assert no_force_create_group in Pulsar.Client.consumers(@client)
    assert {:error, :no_consumers_available} = Pulsar.Consumer.send_flow(no_force_create_group, 1)

    ref = Process.monitor(no_force_create_group)
    assert :ok = Pulsar.Consumer.stop(no_force_create_group, client: @client)
    assert_receive {:DOWN, ^ref, :process, ^no_force_create_group, _reason}
    refute no_force_create_group in Pulsar.Client.consumers(@client)
  end

  test "read_compacted reads the last message per key, not every message", %{expected_count: expected_count} do
    :ok = System.compact_topic(@topic)

    Utils.wait_for(fn -> System.compacted_topic?(@topic) end)

    {:ok, compacted_group} =
      Pulsar.Consumer.start(
        @topic,
        "compacted-true",
        @consumer_callback,
        subscription_options(:earliest, read_compacted: true)
      )

    :ok = Pulsar.Consumer.await_ready(compacted_group)
    [compacted_consumer] = Topology.workers(compacted_group)

    {:ok, non_compacted_group} =
      Pulsar.Consumer.start(
        @topic,
        "compacted-false",
        @consumer_callback,
        subscription_options(:earliest, read_compacted: false)
      )

    :ok = Pulsar.Consumer.await_ready(non_compacted_group)
    [non_compacted_consumer] = Topology.workers(non_compacted_group)

    compacted_messages = receive_messages(compacted_consumer, 4)
    compacted_messages_map = Map.new(compacted_messages, &{Pulsar.Message.key(&1), &1.payload})

    assert compacted_messages_map["key1"] == "Message 2 for key1"
    assert compacted_messages_map["key2"] == "Message 2 for key2"
    assert compacted_messages_map["key3"] == "Message 1 for key3"
    assert compacted_messages_map["key4"] == "Message 1 for key4"

    # Every message, including the ones compaction superseded.
    receive_messages(non_compacted_consumer, expected_count)
  end

  defp receive_messages(consumer, count) do
    for _message <- 1..count do
      assert_receive {:consumer, ^consumer, message}
      message
    end
  end

  defp subscription_options(initial_position, opts \\ []) do
    [
      client: @client,
      subscription_type: :exclusive,
      initial_position: initial_position,
      init_args: [forward_to: self()]
    ] ++ opts
  end

  defp publish_time_from_message(%Pulsar.Message{} = message) do
    Pulsar.Message.publish_time(message)
  end
end
