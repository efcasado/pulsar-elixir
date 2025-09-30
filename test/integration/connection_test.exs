defmodule Pulsar.Integration.ConnectionTest do
  use ExUnit.Case
  require Logger
  alias Pulsar.TestHelper

  @moduletag :integration
  @pulsar_url "pulsar://localhost:6650"
  @test_topic "persistent://public/default/integration-test-topic"
  @test_subscription "integration-test-subscription"

  setup_all do
    # Start Pulsar using test helper
    TestHelper.start_pulsar()

    # Cleanup function
    on_exit(fn ->
      TestHelper.stop_pulsar()
    end)

    :ok
  end

  setup do
    # Trap exit signals to handle process crashes in tests
    original_trap_exit = Process.flag(:trap_exit, true)

    # Reset trap_exit flag after test
    on_exit(fn ->
      Process.flag(:trap_exit, original_trap_exit)
    end)

    :ok
  end

  describe "Connection Reliability" do
    test "broker crash recovery" do
      # Start broker and consumer
      {:ok, broker_pid} = Pulsar.start_broker(@pulsar_url)

      {:ok, consumer_pid} =
        Pulsar.start_consumer(
          @test_topic,
          @test_subscription <> "-crash",
          :Shared,
          Pulsar.DummyConsumer
        )

      # Wait for consumer to connect and check it's registered
      Process.sleep(2000)
      consumers_before = Pulsar.Broker.get_consumers(@pulsar_url)
      initial_consumer_count = map_size(consumers_before)
      assert initial_consumer_count == 1

      # Crash the broker
      Process.exit(broker_pid, :kill)
      Process.sleep(3000)

      # Verify original processes crashed
      assert not Process.alive?(broker_pid)
      assert not Process.alive?(consumer_pid)

      # Verify broker restarted automatically
      {:ok, new_broker_pid} = Pulsar.lookup_broker(@pulsar_url)
      assert Process.alive?(new_broker_pid)
      assert new_broker_pid != broker_pid

      # Verify consumer restarted and re-registered with new broker
      # Give some time for the consumer to restart and reconnect
      Process.sleep(5000)
      consumers_after = Pulsar.Broker.get_consumers(@pulsar_url)

      # Debug: show what consumers we actually have
      Logger.info("Consumers after crash: #{inspect(consumers_after)}")
      Logger.info("Number of consumers: #{map_size(consumers_after)}")

      assert map_size(consumers_after) >= 1,
             "Expected at least 1 consumer to be re-registered after crash"
    end
  end
end
