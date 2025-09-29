defmodule Pulsar.Integration.ConsumerTest do
  use ExUnit.Case
  require Logger
  alias Pulsar.TestHelper

  @moduletag :integration
  @pulsar_url "pulsar://localhost:6650"
  @test_topic "persistent://public/default/integration-test-topic"
  @test_subscription "integration-test-subscription"

  # Test callback to collect received messages
  defmodule TestCallback do
    def start_link do
      Agent.start_link(fn -> [] end, name: __MODULE__)
    end

    def handle_message(message) do
      Agent.update(__MODULE__, fn messages ->
        [message | messages]
      end)

      :ok
    end

    def get_messages do
      Agent.get(__MODULE__, &Enum.reverse/1)
    end

    def clear_messages do
      Agent.update(__MODULE__, fn _ -> [] end)
    end

    def count_messages do
      Agent.get(__MODULE__, &length/1)
    end
  end

  setup_all do
    # Start Pulsar using test helper
    TestHelper.start_pulsar()

    # Start test callback agent
    {:ok, _pid} = TestCallback.start_link()

    # Cleanup function
    on_exit(fn ->
      TestHelper.stop_pulsar()
    end)

    :ok
  end

  setup do
    # Clear messages before each test
    TestCallback.clear_messages()

    # Trap exit signals to handle process crashes in tests
    original_trap_exit = Process.flag(:trap_exit, true)

    # Reset trap_exit flag after test
    on_exit(fn ->
      Process.flag(:trap_exit, original_trap_exit)
    end)

    :ok
  end

  describe "Consumer Integration" do
    test "produce and consume messages" do
      # Start a broker using the idempotent Pulsar API
      {:ok, broker_pid} = Pulsar.start_broker(@pulsar_url)

      # Start consumer using the Pulsar API (no bootstrap broker needed)
      {:ok, consumer_pid} =
        Pulsar.start_consumer(
          @test_topic,
          @test_subscription <> "-e2e",
          :Shared,
          TestCallback
        )

      # Give consumer time to subscribe
      Process.sleep(3000)

      # Generate and produce test messages using test helper
      test_messages = TestHelper.generate_test_messages(3)
      TestHelper.produce_messages(@test_topic, test_messages)

      # Give consumer time to process messages
      Process.sleep(3000)

      # Verify messages were received
      received_messages = TestCallback.get_messages()
      message_count = TestCallback.count_messages()

      Logger.info("Received #{message_count} messages")

      # Assert we received at least some messages (may not be all due to timing)
      assert message_count > 0, "Expected to receive at least 1 message, got #{message_count}"

      # Check that we received some of our test messages
      received_payloads =
        Enum.map(received_messages, fn msg ->
          Map.get(msg, :payload, "") |> to_string()
        end)

      # At least one of our test messages should be received
      assert Enum.any?(test_messages, fn test_msg ->
               Enum.any?(received_payloads, fn payload ->
                 String.contains?(payload, String.slice(test_msg, 0, 10))
               end)
             end),
             "Expected to find at least one test message in received messages"

      Logger.info("Successfully produced and consumed messages!")

      # Cleanup
      Process.exit(consumer_pid, :normal)

      Process.exit(broker_pid, :normal)
    end

    test "broker crash recovery" do
      # Start broker and consumer
      {:ok, broker_pid} = Pulsar.start_broker(@pulsar_url)

      {:ok, consumer_pid} =
        Pulsar.start_consumer(@test_topic, @test_subscription <> "-crash", :Shared, TestCallback)

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
