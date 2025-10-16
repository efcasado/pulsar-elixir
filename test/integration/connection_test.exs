defmodule Pulsar.Integration.ConnectionTest do
  use ExUnit.Case
  require Logger
  alias Pulsar.Test.Support.Utils
  alias Pulsar.Test.Support.System

  @moduletag :integration
  @topic "persistent://public/default/integration-test-topic"
  @subscription "integration-test-subscription"
  @consumer_callback Pulsar.Test.Support.DummyConsumer

  setup_all do
    :ok = System.create_topic(@topic)
  end

  setup do
    broker = System.broker()

    config = [
      host: broker.service_url
    ]

    {:ok, app_pid} = Pulsar.start(config)

    original_trap_exit = Process.flag(:trap_exit, true)

    on_exit(fn ->
      Process.exit(app_pid, :shutdown)
      Utils.wait_for(fn -> not Process.alive?(app_pid) end)
      Process.flag(:trap_exit, original_trap_exit)
    end)
  end

  describe "Connection Reliability" do
    test "consumer recovers from broker crash" do
      {:ok, [group_pid]} =
        Pulsar.start_consumer(
          topic: @topic,
          subscription_name: @subscription <> "-crash",
          subscription_type: :Shared,
          callback_module: @consumer_callback
        )

      [consumer_pid_before_crash] = Pulsar.consumers_for_group(group_pid)

      Utils.wait_for(fn -> System.broker_for_consumer(consumer_pid_before_crash) != nil end)

      broker = System.broker_for_consumer(consumer_pid_before_crash)

      {:ok, broker_pid} = Pulsar.lookup_broker(broker.service_url)
      Process.exit(broker_pid, :kill)

      Utils.wait_for(fn -> not Process.alive?(consumer_pid_before_crash) end)

      [consumer_pid_after_crash] = Pulsar.consumers_for_group(group_pid)

      # consumer crashed due to broker link
      assert not Process.alive?(consumer_pid_before_crash)
      # consumer group supervisor is still alive
      assert Process.alive?(group_pid)
      # a new consumer started
      assert Process.alive?(consumer_pid_after_crash)
      # the old and new consumers are not the same
      assert consumer_pid_before_crash != consumer_pid_after_crash
    end

    test "consumer recovers from broker-initiated topic unload" do
      {:ok, [group_pid]} =
        Pulsar.start_consumer(
          topic: @topic,
          subscription_name: @subscription <> "-unload",
          subscription_type: :Shared,
          callback_module: Pulsar.Test.Support.DummyConsumer
        )

      [consumer_pid_before_unload] = Pulsar.consumers_for_group(group_pid)

      :ok = System.unload_topic(@topic)

      Utils.wait_for(fn -> not Process.alive?(consumer_pid_before_unload) end)

      [consumer_pid_after_unload] = Pulsar.consumers_for_group(group_pid)

      # original consumer crashed due to topic unload
      assert not Process.alive?(consumer_pid_before_unload)
      # consumer group supervisor is still alive
      assert Process.alive?(group_pid)
      # a new consumer started
      assert Process.alive?(consumer_pid_after_unload)
      # the old and new consumers are not the same
      assert consumer_pid_before_unload != consumer_pid_after_unload
    end
  end
end
