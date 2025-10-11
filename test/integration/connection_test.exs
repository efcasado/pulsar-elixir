defmodule Pulsar.Integration.ConnectionTest do
  use ExUnit.Case
  require Logger
  alias Pulsar.TestHelper

  @moduletag :integration
  @test_topic "persistent://public/default/integration-test-topic"
  @test_subscription "integration-test-subscription"

  setup_all do
    TestHelper.start_pulsar()

    on_exit(fn ->
      TestHelper.stop_pulsar()
    end)

    :ok
  end

  setup do
    pulsar_url = TestHelper.random_broker_url()
    {:ok, _broker_pid} = Pulsar.start_broker(pulsar_url)

    original_trap_exit = Process.flag(:trap_exit, true)

    on_exit(fn ->
      Pulsar.stop_broker(pulsar_url)
      Process.flag(:trap_exit, original_trap_exit)
    end)
  end

  describe "Connection Reliability" do
    test "consumer recovers from broker crash" do
      {:ok, group_pid} =
        Pulsar.start_consumer(
          @test_topic,
          @test_subscription <> "-crash",
          :Shared,
          Pulsar.DummyConsumer
        )

      [consumer_pid_before_crash] = Pulsar.ConsumerGroup.list_consumers(group_pid)

			broker = TestHelper.broker_for_consumer(consumer_pid_before_crash)

      {:ok, broker_pid} = Pulsar.lookup_broker(broker.service_url)
      Process.exit(broker_pid, :kill)

			wait_until(fn -> not Process.alive?(consumer_pid_before_crash) end)

			[consumer_pid_after_crash] = Pulsar.ConsumerGroup.list_consumers(group_pid)
			
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
      {:ok, group_pid} =
        Pulsar.start_consumer(
          @test_topic,
          @test_subscription <> "-unload",
          :Shared,
          Pulsar.DummyConsumer
        )

      [consumer_pid_before_unload] = Pulsar.ConsumerGroup.list_consumers(group_pid)

      :ok = TestHelper.unload_topic(@test_topic)

			wait_until(fn -> not Process.alive?(consumer_pid_before_unload) end)

			[consumer_pid_after_unload] = Pulsar.ConsumerGroup.list_consumers(group_pid)
			
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

	defp wait_until(_fun, attempts \\ 100, interval_ms \\ 100)
	defp wait_until(_fun, 0, _interval_ms) do
		:error
	end
	defp wait_until(fun, attempts, interval_ms) do
		case fun.() do
			true ->
				:ok
			false ->
				Process.sleep(interval_ms)
				wait_until(fun, attempts - 1, interval_ms)
		end
	end
end
