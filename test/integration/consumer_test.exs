defmodule Pulsar.Integration.ConsumerTest do
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
          Pulsar.DummyConsumer
        )

      # Give consumer time to subscribe
      Process.sleep(3000)

      # Generate and produce test messages using test helper
      test_messages = TestHelper.generate_test_messages(3)
      TestHelper.produce_messages(@test_topic, test_messages)

      # Give consumer time to process messages
      Process.sleep(3000)

      # Verify messages were received using the new API
      received_messages = Pulsar.DummyConsumer.get_messages(consumer_pid)
      message_count = Pulsar.DummyConsumer.count_messages(consumer_pid)

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
  end
end
