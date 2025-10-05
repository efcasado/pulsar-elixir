defmodule Pulsar.Integration.ConnectionTest do
  use ExUnit.Case
  require Logger
  alias Pulsar.TestHelper

  @moduletag :integration
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
    # Use a random broker for each test since Pulsar is leaderless
    pulsar_url = TestHelper.random_broker_url()
    {:ok, broker_pid} = Pulsar.start_broker(pulsar_url)

    # Trap exit signals to handle process crashes in tests
    original_trap_exit = Process.flag(:trap_exit, true)

    # Cleanup after test
    on_exit(fn ->
      # Stop broker using Pulsar API - this will also stop linked consumers
      Pulsar.stop_broker(pulsar_url)
      Process.flag(:trap_exit, original_trap_exit)
    end)

    {:ok, broker_pid: broker_pid, pulsar_url: pulsar_url}
  end

  describe "Connection Reliability" do
    test "broker crash recovery", %{broker_pid: broker_pid, pulsar_url: pulsar_url} do
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
      consumers_before = Pulsar.Broker.get_consumers(pulsar_url)
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
      {:ok, new_broker_pid} = Pulsar.lookup_broker(pulsar_url)
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
      consumers_after = Pulsar.Broker.get_consumers(pulsar_url)

      # Debug: show what consumers we actually have
      Logger.info("Consumers after crash: #{inspect(consumers_after)}")
      Logger.info("Number of consumers: #{map_size(consumers_after)}")

      assert map_size(consumers_after) >= 1,
             "Expected at least 1 consumer to be re-registered after crash"
    end

    test "broker-initiated topic unload recovery", %{pulsar_url: pulsar_url} do
      # Start a consumer group
      {:ok, group_pid} =
        Pulsar.start_consumer(
          @test_topic,
          @test_subscription <> "-unload",
          :Shared,
          Pulsar.DummyConsumer
        )

      # Get the individual consumer PID from the group
      [initial_consumer_pid] = Pulsar.ConsumerGroup.list_consumers(group_pid)

      # Wait for consumer to connect and verify it's registered
      Process.sleep(2000)
      consumers_before = Pulsar.Broker.get_consumers(pulsar_url)
      assert map_size(consumers_before) == 1
      [{consumer_id, registered_pid}] = Map.to_list(consumers_before)
      assert registered_pid == initial_consumer_pid

      Logger.info("Consumer #{consumer_id} registered with broker before unload")

      # Unload the topic using pulsar-admin - this should trigger CommandCloseConsumer
      assert :ok = TestHelper.unload_topic(@test_topic)

      # Wait for the consumer to be closed and restarted
      Process.sleep(3000)

      # Verify the original consumer was closed (should have exited)
      assert not Process.alive?(initial_consumer_pid),
             "Original consumer should have exited due to topic unload"

      # Verify the consumer group supervisor is still alive
      assert Process.alive?(group_pid),
             "Consumer group supervisor should still be alive"

      # Verify a new consumer was started by the supervisor
      [new_consumer_pid] = Pulsar.ConsumerGroup.list_consumers(group_pid)

      assert Process.alive?(new_consumer_pid),
             "New consumer should be alive after topic unload recovery"

      assert new_consumer_pid != initial_consumer_pid,
             "Should be a new consumer process after restart"

      # Verify exactly one consumer is registered with the broker (the restarted one)
      consumers_after = Pulsar.Broker.get_consumers(pulsar_url)

      assert map_size(consumers_after) == 1,
             "Should have exactly 1 consumer registered after topic unload recovery"

      [{new_consumer_id, new_registered_pid}] = Map.to_list(consumers_after)

      assert new_registered_pid == new_consumer_pid,
             "The registered consumer should be the new restarted consumer"

      Logger.info("Consumer #{new_consumer_id} successfully re-registered after topic unload")
    end
  end
end
