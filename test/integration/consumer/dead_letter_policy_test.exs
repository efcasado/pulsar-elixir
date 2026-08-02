defmodule Pulsar.Integration.Consumer.DeadLetterPolicyTest do
  use ExUnit.Case, async: true

  alias Pulsar.Protocol.Binary.Pulsar.Proto
  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils
  alias Pulsar.Topology

  @moduletag :integration
  @client :dead_letter_policy_test_client
  @topic "persistent://public/default/dlq-test-topic"
  @messages Enum.map(1..3, &"Message #{&1}")

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
        name: :test_producer
      )

    Enum.each(@messages, &({:ok, _message_id} = Pulsar.Producer.send(:test_producer, &1, client: @client)))

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)
  end

  test "an invalid message reaches the DLQ carrying its payload and is acknowledged" do
    topic = "persistent://public/default/dlq-invalid-topic"
    subscription = "invalid"
    dlq_topic = topic <> "-" <> subscription <> "-DLQ"

    {:ok, consumer_group} =
      Pulsar.Consumer.start(topic, subscription, DummyConsumer,
        client: @client,
        redelivery_interval: 100,
        dead_letter_policy: [max_redelivery: 1, topic: dlq_topic]
      )

    [consumer] = Utils.wait_for(fn -> Topology.workers(consumer_group) end, until: &match?([_], &1))

    {:ok, dlq_group} =
      Pulsar.Consumer.start(dlq_topic, "dlq-consumer", DummyConsumer,
        client: @client,
        initial_position: :earliest
      )

    [dlq_consumer] = Utils.wait_for(fn -> Topology.workers(dlq_group) end, until: &match?([_], &1))

    command = %Proto.CommandMessage{
      consumer_id: 1,
      message_id: %Proto.MessageIdData{ledgerId: 1, entryId: 1},
      redelivery_count: 5
    }

    send(consumer, {:broker_message, {:invalid, command, "corrupt-payload", :checksum_mismatch}})

    Utils.wait_for(fn -> DummyConsumer.count_messages(dlq_consumer) > 0 end)

    assert [dlq_message] = DummyConsumer.get_messages(dlq_consumer)
    assert dlq_message.payload == "corrupt-payload"

    # The acknowledgement carries a validation error the broker has to accept; had
    # it not, the connection would have gone and taken the consumer with it.
    assert Process.alive?(consumer)
    assert Pulsar.Consumer.topic(consumer) == topic
  end

  test "dead letter policy with max_redelivery sends messages to DLQ after threshold" do
    topic = @topic
    subscription = "failing"
    dlq_topic = topic <> "-" <> subscription <> "-DLQ"
    max_redelivery = 3

    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        topic,
        subscription,
        DummyConsumer,
        init_args: [fail_all: true],
        client: @client,
        initial_position: :earliest,
        subscription_type: :Shared,
        redelivery_interval: 100,
        dead_letter_policy: [
          max_redelivery: max_redelivery,
          topic: dlq_topic
        ]
      )

    [failing_consumer] =
      Utils.wait_for(fn -> Topology.workers(consumer_group) end, until: &match?([_], &1))

    {:ok, dlq_consumer_group} =
      Pulsar.Consumer.start(
        dlq_topic,
        "dlq-consumer",
        DummyConsumer,
        client: @client,
        subscription_type: :Shared,
        initial_position: :earliest
      )

    [dlq_consumer] =
      Utils.wait_for(fn -> Topology.workers(dlq_consumer_group) end, until: &match?([_], &1))

    Utils.wait_for(fn ->
      DummyConsumer.count_messages(dlq_consumer) == length(@messages)
    end)

    failing_consumer_count = DummyConsumer.count_messages(failing_consumer)

    assert failing_consumer_count == length(@messages) * (max_redelivery + 1)

    Utils.wait_for(fn ->
      DummyConsumer.count_messages(dlq_consumer) == length(@messages)
    end)

    dlq_messages = DummyConsumer.get_messages(dlq_consumer)
    assert length(dlq_messages) == length(@messages)

    dlq_payloads = Enum.map(dlq_messages, & &1.payload)
    assert dlq_payloads == @messages
  end

  test "no dead letter policy means no DLQ" do
    topic = @topic
    subscription = "no-dlq"
    expected_dlq_topic = "#{topic}-#{subscription}-DLQ"

    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        topic,
        subscription,
        DummyConsumer,
        init_args: [fail_all: true],
        client: @client,
        initial_position: :earliest,
        subscription_type: :Shared,
        redelivery_interval: 100
      )

    [failing_consumer] =
      Utils.wait_for(fn -> Topology.workers(consumer_group) end, until: &match?([_], &1))

    Utils.wait_for(fn ->
      DummyConsumer.count_messages(failing_consumer) >= length(@messages) * 2
    end)

    failing_consumer_count = DummyConsumer.count_messages(failing_consumer)
    assert failing_consumer_count >= length(@messages) * 2

    {:ok, topics} = System.list_topics()
    refute expected_dlq_topic in topics
  end

  test "dead letter policy with default DLQ topic name" do
    topic = @topic
    subscription = "default-name"
    expected_dlq_topic = "#{topic}-#{subscription}-DLQ"

    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        topic,
        subscription,
        DummyConsumer,
        init_args: [fail_all: true],
        client: @client,
        initial_position: :earliest,
        subscription_type: :Shared,
        redelivery_interval: 100,
        dead_letter_policy: [
          max_redelivery: 2
        ]
      )

    [_failing_consumer] =
      Utils.wait_for(fn -> Topology.workers(consumer_group) end, until: &match?([_], &1))

    {:ok, dlq_consumer_group} =
      Pulsar.Consumer.start(
        expected_dlq_topic,
        "dlq-default-monitor",
        DummyConsumer,
        client: @client,
        subscription_type: :Shared,
        initial_position: :earliest
      )

    [dlq_consumer] =
      Utils.wait_for(fn -> Topology.workers(dlq_consumer_group) end, until: &match?([_], &1))

    Utils.wait_for(fn ->
      DummyConsumer.count_messages(dlq_consumer) == length(@messages)
    end)

    dlq_messages = DummyConsumer.get_messages(dlq_consumer)
    assert length(dlq_messages) == length(@messages)
  end
end
