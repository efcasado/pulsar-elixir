defmodule Pulsar.Integration.Consumer.DeadLetterPolicyTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  require Logger

  @moduletag :integration
  @client :dead_letter_policy_test_client
  @topic "persistent://public/default/dlq-test-topic"
  @messages Enum.map(1..3, &"Message #{&1}")

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
        name: :test_producer
      )

    Enum.each(@messages, &({:ok, _message_id} = Pulsar.send(:test_producer, &1, client: @client)))

    on_exit(fn ->
      if Process.alive?(producer_pid), do: Pulsar.stop_producer(producer_pid)
      if Process.alive?(client_pid), do: Supervisor.stop(client_pid, :normal)
    end)
  end

  test "dead letter policy with max_redelivery sends messages to DLQ after threshold" do
    topic = @topic
    subscription = "failing"
    dlq_topic = topic <> "-" <> subscription <> "-DLQ"
    max_redelivery = 3

    {:ok, consumer_group} =
      Pulsar.start_consumer(
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

    [failing_consumer] = Pulsar.get_consumers(consumer_group)

    {:ok, dlq_consumer_group} =
      Pulsar.start_consumer(
        dlq_topic,
        "dlq-consumer",
        DummyConsumer,
        client: @client,
        subscription_type: :Shared,
        initial_position: :earliest
      )

    [dlq_consumer] = Pulsar.get_consumers(dlq_consumer_group)

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

  test "dead letter policy with nil max_redelivery does not send to DLQ" do
    topic = @topic
    subscription = "no-dlq"
    expected_dlq_topic = "#{topic}-#{subscription}-DLQ"

    {:ok, consumer_group} =
      Pulsar.start_consumer(
        topic,
        subscription,
        DummyConsumer,
        init_args: [fail_all: true],
        client: @client,
        initial_position: :earliest,
        subscription_type: :Shared,
        redelivery_interval: 100,
        dead_letter_policy: [
          max_redelivery: nil
        ]
      )

    [failing_consumer] = Pulsar.get_consumers(consumer_group)

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
      Pulsar.start_consumer(
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

    [_failing_consumer] = Pulsar.get_consumers(consumer_group)

    {:ok, dlq_consumer_group} =
      Pulsar.start_consumer(
        expected_dlq_topic,
        "dlq-default-monitor",
        DummyConsumer,
        client: @client,
        subscription_type: :Shared,
        initial_position: :earliest
      )

    [dlq_consumer] = Pulsar.get_consumers(dlq_consumer_group)

    Utils.wait_for(fn ->
      DummyConsumer.count_messages(dlq_consumer) == length(@messages)
    end)

    dlq_messages = DummyConsumer.get_messages(dlq_consumer)
    assert length(dlq_messages) == length(@messages)
  end
end
