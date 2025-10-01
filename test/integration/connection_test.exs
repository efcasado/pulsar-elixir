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
    # Start a broker for each test
    {:ok, broker_pid} = Pulsar.start_broker(@pulsar_url)

    # Trap exit signals to handle process crashes in tests
    original_trap_exit = Process.flag(:trap_exit, true)

    # Cleanup after test
    on_exit(fn ->
      # Stop broker using Pulsar API - this will also stop linked consumers
      Pulsar.stop_broker(@pulsar_url)
      Process.flag(:trap_exit, original_trap_exit)
    end)

    {:ok, broker_pid: broker_pid}
  end

  describe "Connection Reliability" do
    test "broker crash recovery", %{broker_pid: broker_pid} do
      # Start a consumer group (this returns the group supervisor PID)
      {:ok, group_pid} =
        Pulsar.start_consumer(
          @test_topic,
          @test_subscription <> "-crash",
          :Shared,
          Pulsar.DummyConsumer
        )

      # Get the individual consumer PID from the group
      [individual_consumer_pid] = Pulsar.ConsumerGroup.list_consumers(group_pid)

      # Wait for consumer to connect and check it's registered
      Process.sleep(2000)
      consumers_before = Pulsar.Broker.get_consumers(@pulsar_url)
      initial_consumer_count = map_size(consumers_before)
      assert initial_consumer_count == 1

      # Crash the broker
      Process.exit(broker_pid, :kill)
      Process.sleep(3000)

      # Verify original processes behavior:
      assert not Process.alive?(broker_pid), "Broker should have crashed"

      assert not Process.alive?(individual_consumer_pid),
             "Individual consumer should have crashed due to broker link"

      assert Process.alive?(group_pid), "Consumer group supervisor should still be alive"

      # Verify broker restarted automatically
      {:ok, new_broker_pid} = Pulsar.lookup_broker(@pulsar_url)
      assert Process.alive?(new_broker_pid)
      assert new_broker_pid != broker_pid

      # Verify individual consumer restarted (group supervisor restarts it)
      # Give some time for the consumer to restart and reconnect
      Process.sleep(5000)

      # Get the new individual consumer PID
      [new_individual_consumer_pid] = Pulsar.ConsumerGroup.list_consumers(group_pid)

      assert Process.alive?(new_individual_consumer_pid),
             "New individual consumer should be alive"

      assert new_individual_consumer_pid != individual_consumer_pid,
             "Should be a new consumer process"

      # Verify consumer re-registered with new broker
      consumers_after = Pulsar.Broker.get_consumers(@pulsar_url)

      # Debug: show what consumers we actually have
      Logger.info("Consumers after crash: #{inspect(consumers_after)}")
      Logger.info("Number of consumers: #{map_size(consumers_after)}")

      assert map_size(consumers_after) >= 1,
             "Expected at least 1 consumer to be re-registered after crash"
    end
  end
end
