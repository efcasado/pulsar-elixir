ExUnit.start()

defmodule Pulsar.TestHelper do
  @moduledoc """
  Shared test utilities for Pulsar integration tests.
  """
  require Logger

  # Broker configurations - we'll randomly pick one for admin operations since Pulsar is leaderless
  @brokers [
    %{
			container: "broker1",
			host: "broker1",
      web_port: 8080,
      service_port: 6650,
      service_url: "pulsar://broker1:6650",
      health_url: "http://broker1:8080/admin/v2/brokers/health"
    },
    %{
			container: "broker2",
			host: "broker2",
      web_port: 8081,
      service_port: 6651,
      service_url: "pulsar://broker2:6651",
      health_url: "http://broker2:8081/admin/v2/brokers/health"
    }
  ]
	
  @doc """
  Starts Pulsar with Docker Compose and waits for it to be ready.
  """
  def start_pulsar do
    Logger.info("Starting Pulsar with Docker Compose...")
    {output, exit_code} = System.cmd("docker", ["compose", "up", "-d"], stderr_to_stdout: true)

    if exit_code != 0 do
      Logger.error("Docker compose failed to start: #{output}")
      # Try to stop any containers that might have started
      System.cmd("docker", ["compose", "down"], stderr_to_stdout: true)
      raise "Failed to start Docker Compose. Exit code: #{exit_code}. Output: #{output}"
    end

    Logger.info("Waiting for Pulsar to be ready...")
    wait_for_pulsar()
  end

  @doc """
  Returns a random broker service URL for connecting to Pulsar.
  Since Pulsar is leaderless, any broker can handle client connections.
  """
  def random_broker_url do
    broker = Enum.random(@brokers)
    broker.service_url
  end

  @doc """
  Creates a namespace using a specific broker.
  """
  def create_namespace(broker, namespace) do
    Logger.info("Creating namespace '#{namespace}' using broker on port #{broker.web_port}...")

    namespace_url = "http://#{broker.host}:#{broker.web_port}/admin/v2/namespaces/#{namespace}"

    case System.cmd(
           "curl",
           [
             "-X",
             "PUT",
             "-H",
             "Content-Type: application/json",
             namespace_url,
             "-d",
             "{}"
           ],
           stderr_to_stdout: true
         ) do
      {_, 0} ->
        Logger.info("Namespace '#{namespace}' created/verified")
        :ok

      {error, _} ->
        Logger.warning("Namespace '#{namespace}' creation failed: #{error}")
        {:error, error}
    end
  end

  @doc """
  Creates a topic using a specific broker.
  """
  def create_topic(broker, topic) do
    Logger.info("Creating topic '#{topic}' using broker on port #{broker.web_port}...")

    topic_url = "http://#{broker.host}:#{broker.web_port}/admin/v2/persistent/#{topic}"

    case System.cmd(
           "curl",
           [
             "-X",
             "PUT",
             topic_url
           ],
           stderr_to_stdout: true
         ) do
      {_, 0} ->
        Logger.info("Topic '#{topic}' created/verified")
        :ok

      {error, _} ->
        Logger.warning("Topic '#{topic}' creation failed: #{error}")
        {:error, error}
    end
  end

  @doc """
  Sets up Pulsar resources (namespaces and topics) for testing.

  ## Options
  - `:namespaces` - List of namespace names to create (default: ["public/default"])
  - `:topics` - List of topic names to create (default: ["public/default/integration-test-topic"])

  ## Examples

      setup_pulsar_resources(broker)
      setup_pulsar_resources(broker, namespaces: ["public/test1", "public/test2"])
      setup_pulsar_resources(broker, topics: ["public/default/topic1", "public/default/topic2"])
  """
  def setup_pulsar_resources(broker, opts \\ []) do
    namespaces = Keyword.get(opts, :namespaces, ["public/default"])
    topics = Keyword.get(opts, :topics, ["public/default/integration-test-topic"])

    Logger.info("Setting up Pulsar resources using broker on port #{broker.web_port}...")

    # Create all requested namespaces
    Enum.each(namespaces, fn namespace ->
      create_namespace(broker, namespace)
    end)

    # Create all requested topics
    Enum.each(topics, fn topic ->
      create_topic(broker, topic)
    end)

    # Give a moment for the changes to propagate
    Process.sleep(1000)
  end

  @doc """
  Sets up Pulsar resources using a random broker.
  This is a convenience function that selects a random broker and sets up resources.

  ## Examples

      setup_test_resources()
      setup_test_resources(topics: ["public/default/topic1", "public/default/topic2"])
  """
  def setup_test_resources(opts \\ []) do
    broker = Enum.random(@brokers)
    setup_pulsar_resources(broker, opts)
    broker
  end

  @doc """
  Creates multiple test topics in the default namespace.

  ## Examples

      create_test_topics(["topic1", "topic2", "topic3"])
  """
  def create_test_topics(topic_names) when is_list(topic_names) do
    broker = Enum.random(@brokers)
    topics = Enum.map(topic_names, &"public/default/#{&1}")
    setup_pulsar_resources(broker, topics: topics)
    broker
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
    # Randomly select a broker container for message production
    # Both broker-1 and broker-2 containers can handle message production
    broker = Enum.random(@brokers)

    base_args = [
      "exec",
      broker.container,
      "bin/pulsar-client",
			"--url",
			broker.service_url,
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
      Logger.error("Failed to produce message using #{broker.container}: #{output}")
      {:error, output}
    else
      Logger.debug(
        "Produced message using #{broker.container}: #{message}#{if key, do: " (key: #{key})", else: ""}"
      )

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

  @doc """
  Unloads a topic using pulsar-admin, forcing consumers to reconnect.
  This simulates broker-initiated topic unloading scenarios.
  """
  def unload_topic(topic) do
    # Randomly select a broker container for admin operations
    broker = Enum.random(@brokers)

    {output, exit_code} =
      System.cmd(
        "docker",
        [
          "exec",
          broker.container,
          "bin/pulsar-admin",
					"--admin-url",
					"http://#{broker.host}:#{broker.web_port}",
          "topics",
          "unload",
          topic
        ],
        stderr_to_stdout: true
      )

    if exit_code != 0 do
      Logger.error("Failed to unload topic using #{broker.container}: #{output}")
      {:error, output}
    else
      Logger.info("Successfully unloaded topic using #{broker.container}: #{topic}")
      :ok
    end
  end

  # Private helper functions
  defp wait_for_pulsar(retries \\ 30) do
    # Try each broker until one responds or we run out of retries
    case try_brokers_health_check(@brokers) do
      {:ok, working_broker} ->
        Logger.info("Pulsar broker on port #{working_broker.web_port} is ready!")
        setup_pulsar_resources(working_broker)
        :ok

      {:error, _} when retries > 0 ->
        Logger.info("Pulsar brokers not ready yet, waiting... (#{retries} retries left)")
        Process.sleep(2000)
        wait_for_pulsar(retries - 1)

      {:error, last_error} ->
        raise "Pulsar failed to start after 60 seconds. Last error: #{last_error}"
    end
  end

  defp try_brokers_health_check(brokers) do
    # Shuffle brokers to randomly select which one to try first
    shuffled_brokers = Enum.shuffle(brokers)

    Enum.reduce_while(shuffled_brokers, {:error, "No brokers available"}, fn broker, _acc ->
      case System.cmd("curl", ["-f", broker.health_url], stderr_to_stdout: true) do
        {_, 0} -> {:halt, {:ok, broker}}
        {error, _} -> {:cont, {:error, error}}
      end
    end)
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
