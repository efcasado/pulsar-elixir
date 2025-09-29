ExUnit.start()

defmodule Pulsar.TestHelper do
  @moduledoc """
  Shared test utilities for Pulsar integration tests.
  """
  require Logger

  @pulsar_health_url "http://localhost:8080/admin/v2/brokers/health"
  @pulsar_namespace_url "http://localhost:8080/admin/v2/namespaces/public/default"
  @pulsar_topic_url "http://localhost:8080/admin/v2/persistent/public/default/integration-test-topic"

  @doc """
  Starts Pulsar with Docker Compose and waits for it to be ready.
  """
  def start_pulsar do
    Logger.info("Starting Pulsar with Docker Compose...")
    {_, 0} = System.cmd("docker", ["compose", "up", "-d"], stderr_to_stdout: true)

    Logger.info("Waiting for Pulsar to be ready...")
    wait_for_pulsar()
  end

  @doc """
  Stops Pulsar Docker Compose services.
  """
  def stop_pulsar do
    Logger.info("Stopping Pulsar...")
    System.cmd("docker", ["compose", "down"], stderr_to_stdout: true)
  end

  @doc """
  Produces a message to a topic using the pulsar-client in the Docker container.
  """
  def produce_message(topic, message) do
    {output, exit_code} =
      System.cmd(
        "docker",
        [
          "exec",
          "pulsar",
          "bin/pulsar-client",
          "produce",
          topic,
          "-m",
          message
        ],
        stderr_to_stdout: true
      )

    if exit_code != 0 do
      Logger.error("Failed to produce message: #{output}")
      {:error, output}
    else
      Logger.debug("Produced message: #{message}")
      :ok
    end
  end

  @doc """
  Produces multiple messages to a topic with optional delay between messages.
  """
  def produce_messages(topic, messages, delay_ms \\ 100) do
    Enum.each(messages, fn message ->
      produce_message(topic, message)
      if delay_ms > 0, do: Process.sleep(delay_ms)
    end)
  end

  @doc """
  Generates test messages with timestamps for uniqueness.
  """
  def generate_test_messages(count \\ 3, prefix \\ "Test message") do
    base_time = :os.system_time(:millisecond)

    Enum.map(1..count, fn i ->
      "#{prefix} #{i}: #{base_time + i - 1}"
    end)
  end

  # Private helper functions
  defp wait_for_pulsar(retries \\ 30) do
    case System.cmd("curl", ["-f", @pulsar_health_url], stderr_to_stdout: true) do
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
    case System.cmd(
           "curl",
           [
             "-X",
             "PUT",
             "-H",
             "Content-Type: application/json",
             @pulsar_namespace_url,
             "-d",
             "{}"
           ],
           stderr_to_stdout: true
         ) do
      {_, 0} -> Logger.info("Namespace created/verified")
      {_, _} -> Logger.info("Namespace might already exist or creation failed")
    end

    # Create topic (topics are created automatically on first use in Pulsar,
    # but we can pre-create it to ensure it exists)
    case System.cmd(
           "curl",
           [
             "-X",
             "PUT",
             @pulsar_topic_url
           ],
           stderr_to_stdout: true
         ) do
      {_, 0} -> Logger.info("Topic created/verified")
      {_, _} -> Logger.info("Topic might already exist or creation failed")
    end

    # Give a moment for the changes to propagate
    Process.sleep(1000)
  end
end
