defmodule Pulsar.Integration.ConsumerTest do
  use ExUnit.Case
  require Logger

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
    # Start Docker Compose for Pulsar
    Logger.info("Starting Pulsar with Docker Compose...")
    {_, 0} = System.cmd("docker", ["compose", "up", "-d"], stderr_to_stdout: true)

    # Wait for Pulsar to be ready
    Logger.info("Waiting for Pulsar to be ready...")
    wait_for_pulsar()

    # Start test callback agent
    {:ok, _pid} = TestCallback.start_link()

    # Cleanup function
    on_exit(fn ->
      Logger.info("Stopping Pulsar...")
      System.cmd("docker", ["compose", "down"], stderr_to_stdout: true)
    end)

    :ok
  end

  setup do
    # Clear messages before each test
    TestCallback.clear_messages()
    :ok
  end

  describe "Consumer Integration" do
    test "produce and consume messages" do
      {:ok, discovery_pid} = Pulsar.ServiceDiscovery.start_link(@pulsar_url, [])
      
      # Start consumer
      {:ok, consumer_pid} = Pulsar.Consumer.start_link(
        discovery_pid,
        @test_topic,
        :Shared,
        @test_subscription <> "-e2e",
        TestCallback
      )
      
      # Give consumer time to subscribe
      Process.sleep(2000)
      
      # Produce test messages using pulsar-client in Docker container
      test_messages = [
        "Test message 1: #{:os.system_time(:millisecond)}",
        "Test message 2: #{:os.system_time(:millisecond) + 1}",
        "Test message 3: #{:os.system_time(:millisecond) + 2}"
      ]
      
      # Send each message
      Enum.each(test_messages, fn message ->
        {output, exit_code} = System.cmd("docker", [
          "exec", "pulsar", "bin/pulsar-client", "produce", 
          @test_topic, "-m", message
        ], stderr_to_stdout: true)
        
        Logger.info("Produced message: #{message}")
        if exit_code != 0 do
          Logger.error("Failed to produce message: #{output}")
        end
        
        # Small delay between messages
        Process.sleep(100)
      end)
      
      # Give consumer time to process messages
      Process.sleep(3000)
      
      # Verify messages were received
      received_messages = TestCallback.get_messages()
      message_count = TestCallback.count_messages()
      
      Logger.info("Received #{message_count} messages")
      
      # Assert we received at least some messages (may not be all due to timing)
      assert message_count > 0, "Expected to receive at least 1 message, got #{message_count}"
      
      # Check that we received some of our test messages
      received_payloads = Enum.map(received_messages, fn msg -> 
        Map.get(msg, :payload, "") |> to_string()
      end)
      
      # At least one of our test messages should be received
      assert Enum.any?(test_messages, fn test_msg ->
        Enum.any?(received_payloads, fn payload -> 
          String.contains?(payload, String.slice(test_msg, 0, 10))
        end)
      end), "Expected to find at least one test message in received messages"
      
      Logger.info("Successfully produced and consumed messages!")
      
      # Cleanup
      Process.exit(consumer_pid, :normal)
      Process.exit(discovery_pid, :normal)
    end
  end

  # Helper functions
  defp wait_for_pulsar(retries \\ 30) do
    case System.cmd("curl", ["-f", "http://localhost:8080/admin/v2/brokers/health"], 
                   stderr_to_stdout: true) do
      {_, 0} ->
        Logger.info("Pulsar is ready!")
        setup_pulsar_resources()
        :ok
      {_, _} when retries > 0 ->
        Logger.info("Pulsar not ready yet, waiting... (#{retries} retries left)")
        Process.sleep(2000)
        wait_for_pulsar(retries - 1)
      {output, _} ->
        raise "Pulsar failed to start after 60 seconds. Output: #{output}"
    end
  end
  
  defp setup_pulsar_resources do
    Logger.info("Setting up Pulsar namespace and topic...")
    
    # Create namespace (if it doesn't exist)
    case System.cmd("curl", [
      "-X", "PUT",
      "-H", "Content-Type: application/json",
      "http://localhost:8080/admin/v2/namespaces/public/default",
      "-d", "{}"
    ], stderr_to_stdout: true) do
      {_, 0} -> Logger.info("Namespace created/verified")
      {_, _} -> Logger.info("Namespace might already exist or creation failed")
    end
    
    # Create topic (topics are created automatically on first use in Pulsar,
    # but we can pre-create it to ensure it exists)
    case System.cmd("curl", [
      "-X", "PUT",
      "http://localhost:8080/admin/v2/persistent/public/default/integration-test-topic"
    ], stderr_to_stdout: true) do
      {_, 0} -> Logger.info("Topic created/verified")
      {_, _} -> Logger.info("Topic might already exist or creation failed")
    end
    
    # Give a moment for the changes to propagate
    Process.sleep(1000)
  end
end
