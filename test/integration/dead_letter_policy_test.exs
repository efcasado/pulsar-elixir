defmodule Pulsar.Integration.DeadLetterPolicyTest do
  use ExUnit.Case

  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  require Logger

  @moduletag :integration
  @topic "persistent://public/default/dlq-test-topic"
  @subscription_prefix "dlq-test-subscription-"
  @messages Enum.map(1..3, &"Message #{&1}")

  setup_all do
    broker = System.broker()

    config = [
      host: broker.service_url,
      producers: [
        test_producer: [
          topic: @topic
        ]
      ]
    ]

    {:ok, app_pid} = Pulsar.start(config)

    Enum.each(@messages, &({:ok, _message_id} = Pulsar.send(:test_producer, &1)))

    on_exit(fn ->
      Process.exit(app_pid, :shutdown)
      Utils.wait_for(fn -> not Process.alive?(app_pid) end)
    end)
  end

  test "dead letter policy with max_redelivery sends messages to DLQ after threshold" do
    topic = @topic
    subscription = @subscription_prefix <> "failing"
    dlq_topic = topic <> "-" <> subscription <> "-DLQ"
    max_redelivery = 3

    {:ok, consumer_group} =
      Pulsar.start_consumer(
        topic,
        subscription,
        DummyConsumer,
        init_args: [fail_all: true],
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
        @subscription_prefix <> "dlq-consumer",
        DummyConsumer,
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
    subscription = @subscription_prefix <> "no-dlq"
    expected_dlq_topic = "#{topic}-#{subscription}-DLQ"

    {:ok, consumer_group} =
      Pulsar.start_consumer(
        topic,
        subscription,
        DummyConsumer,
        init_args: [fail_all: true],
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
    subscription = @subscription_prefix <> "default-name"
    expected_dlq_topic = "#{topic}-#{subscription}-DLQ"

    {:ok, consumer_group} =
      Pulsar.start_consumer(
        topic,
        subscription,
        DummyConsumer,
        init_args: [fail_all: true],
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
        @subscription_prefix <> "dlq-default-monitor",
        DummyConsumer,
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
