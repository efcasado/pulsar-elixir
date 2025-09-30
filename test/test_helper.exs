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
  def produce_message(topic, message, key \\ nil) do
    base_args = [
      "exec",
      "pulsar",
      "bin/pulsar-client",
      "produce",
      topic,
      "-m",
      message
    ]

    args =
      if key do
        base_args ++ ["-k", key]
      else
        base_args
      end

    {output, exit_code} = System.cmd("docker", args, stderr_to_stdout: true)

    if exit_code != 0 do
      Logger.error("Failed to produce message: #{output}")
      {:error, output}
    else
      Logger.debug("Produced message: #{message}#{if key, do: " (key: #{key})", else: ""}")
      :ok
    end
  end

  @doc """
  Produces multiple messages to a topic with optional delay between messages.

  Messages can be:
  - Simple strings: ["message1", "message2"]
  - Tuples with keys: [{"key1", "message1"}, {"key2", "message2"}]
  """
  def produce_messages(topic, messages, delay_ms \\ 100) do
    Enum.each(messages, fn
      {key, message} ->
        produce_message(topic, message, key)
        if delay_ms > 0, do: Process.sleep(delay_ms)

      message when is_binary(message) ->
        produce_message(topic, message)
        if delay_ms > 0, do: Process.sleep(delay_ms)
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

# Shared test callback module for all integration tests
defmodule Pulsar.DummyConsumer do
  @moduledoc """
  A test consumer callback that collects messages and provides test utilities.

  This module can be used across different test files to avoid code duplication.
  """

  @behaviour Pulsar.Consumer.Callback

  def init(_opts) do
    {:ok, %{messages: [], count: 0}}
  end

  def handle_message(message, state) do
    new_state = %{
      state
      | messages: [message | state.messages],
        count: state.count + 1
    }

    {:ok, new_state}
  end

  # Custom GenServer calls for testing
  def handle_call(:get_messages, _from, state) do
    {:reply, Enum.reverse(state.messages), state}
  end

  def handle_call(:count_messages, _from, state) do
    {:reply, state.count, state}
  end

  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  def handle_cast(:clear_messages, _state) do
    {:noreply, %{messages: [], count: 0}}
  end

  # Test helper functions that delegate to GenServer calls
  def get_messages(consumer_pid) do
    GenServer.call(consumer_pid, :get_messages)
  end

  def clear_messages(consumer_pid) do
    GenServer.cast(consumer_pid, :clear_messages)
  end

  def count_messages(consumer_pid) do
    GenServer.call(consumer_pid, :count_messages)
  end

  def get_state(consumer_pid) do
    GenServer.call(consumer_pid, :get_state)
  end
end
