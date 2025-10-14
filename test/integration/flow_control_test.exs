defmodule Pulsar.Integration.FlowControlTest do
  use ExUnit.Case
  require Logger
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @topic "persistent://public/default/flow-control-test-topic"
  @subscription "flow-control-test-subscription"
  @consumer_callback Pulsar.Test.Support.DummyConsumer

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

  describe "Flow Control Configuration" do
    test "consumer with one permit at a time" do
      {:ok, group_pid} =
        Pulsar.start_consumer(
          topic: @topic,
          subscription_name: @subscription <> "-tiny-permits",
          subscription_type: :Shared,
          callback_module: @consumer_callback,
          opts: [
            initial_permits: 1,
            refill_threshold: 0,
            refill_amount: 1
          ]
        )

      [consumer_pid] = Pulsar.consumers_for_group(group_pid)

      messages =
        for i <- 1..5 do
          {"key#{i}", "Message #{i}"}
        end

      System.produce_messages(@topic, messages)

      Utils.wait_for(fn ->
        consumer_count = @consumer_callback.count_messages(consumer_pid)
        Enum.count(messages) == consumer_count
      end)

      message_count = @consumer_callback.count_messages(consumer_pid)
      assert message_count == 5
    end

    test "multiple consumers with different flow control settings" do
      {:ok, group_pid1} =
        Pulsar.start_consumer(
          topic: @topic,
          subscription_name: @subscription <> "-multi-1",
          subscription_type: :Shared,
          callback_module: @consumer_callback,
          opts: [
            consumer_count: 1,
            initial_permits: 100,
            refill_threshold: 50,
            refill_amount: 50
          ]
        )

      {:ok, group_pid2} =
        Pulsar.start_consumer(
          topic: @topic,
          subscription_name: @subscription <> "-multi-2",
          subscription_type: :Shared,
          callback_module: @consumer_callback,
          opts: [
            consumer_count: 1,
            initial_permits: 1000,
            refill_threshold: 500,
            refill_amount: 500
          ]
        )

      [consumer_pid1] = Pulsar.consumers_for_group(group_pid1)
      [consumer_pid2] = Pulsar.consumers_for_group(group_pid2)

      messages =
        for i <- 1..10 do
          {"key#{rem(i, 3)}", "Message #{i}"}
        end

      System.produce_messages(@topic, messages)

      # Both consumers should receive some messages (since they have different subscriptions)
      Utils.wait_for(fn ->
        consumer1_count = @consumer_callback.count_messages(consumer_pid1)
        consumer2_count = @consumer_callback.count_messages(consumer_pid2)
        consumer1_count == 10 and consumer2_count == 10
      end)

      consumer1_count = @consumer_callback.count_messages(consumer_pid1)
      consumer2_count = @consumer_callback.count_messages(consumer_pid2)

      assert consumer1_count == 10
      assert consumer2_count == 10
    end

    test "multiple consumers in same group with shared flow control settings" do
      {:ok, group_pid} =
        Pulsar.start_consumer(
          topic: @topic,
          subscription_name: @subscription <> "-group-shared",
          subscription_type: :Shared,
          callback_module: @consumer_callback,
          opts: [
            consumer_count: 2,
            initial_permits: 5,
            refill_threshold: 1,
            refill_amount: 4
          ]
        )

      [consumer1_pid, consumer2_pid] = Pulsar.consumers_for_group(group_pid)

      messages =
        for i <- 1..15 do
          {"key#{rem(i, 5)}", "Message #{i}"}
        end

      System.produce_messages(@topic, messages)

      Utils.wait_for(fn ->
        consumer1_count = @consumer_callback.count_messages(consumer1_pid)
        consumer2_count = @consumer_callback.count_messages(consumer2_pid)
        Enum.count(messages) == consumer1_count + consumer2_count
      end)

      consumer1_count = @consumer_callback.count_messages(consumer1_pid)
      consumer2_count = @consumer_callback.count_messages(consumer2_pid)
      total_messages = consumer1_count + consumer2_count

      # Both consumers should receive messages
      assert total_messages == 15
      assert consumer1_count > 0
      assert consumer2_count > 0
    end
  end
end
