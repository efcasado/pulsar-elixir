defmodule Pulsar.Test.Support.System do
  @moduledoc false
  alias Pulsar.Test.Support.Utils

  require Logger

  @brokers [
    %{
      container: "broker1",
      host: "broker1",
      web_port: 8080,
      service_port: 6650,
      admin_url: "http://broker1:8080",
      service_url: "pulsar://broker1:6650",
      health_url: "http://broker1:8080/admin/v2/brokers/health"
    },
    %{
      container: "broker2",
      host: "broker2",
      web_port: 8081,
      service_port: 6651,
      admin_url: "http://broker2:8081",
      service_url: "pulsar://broker2:6651",
      health_url: "http://broker2:8081/admin/v2/brokers/health"
    }
  ]

  def broker do
    Enum.random(@brokers)
  end

  def brokers do
    @brokers
  end

  def kill_broker(%{container: container} = _broker) do
    {_out, 0} = System.cmd("docker", ["kill", container], stderr_to_stdout: true)
    :ok
  end

  def broker_for_consumer(consumer) when is_pid(consumer) do
    Enum.find(
      @brokers,
      nil,
      fn broker ->
        broker.service_url
        |> Pulsar.Broker.get_consumers()
        |> Enum.any?(fn {_id, pid} -> pid == consumer end)
      end
    )
  end

  def broker_for_producer(producer) when is_pid(producer) do
    Enum.find(
      @brokers,
      nil,
      fn broker ->
        broker.service_url
        |> Pulsar.Broker.get_producers()
        |> Enum.any?(fn {_id, pid} -> pid == producer end)
      end
    )
  end

  def consumers_on(broker_url) when is_binary(broker_url) do
    Pulsar.Broker.get_consumers(broker_url)
  end

  def start_pulsar do
    Logger.info("Starting Pulsar ...")
    {_output, 0} = System.cmd("docker", ["compose", "up", "-d"], stderr_to_stdout: true)

    :ok = Utils.wait_for(&brokers_up?/0, _attempts = 100, _delay = 100)
  end

  def stop_pulsar do
    Logger.info("Stopping Pulsar ...")
    {_output, 0} = System.cmd("docker", ["compose", "down"], stderr_to_stdout: true)
    :ok
  end

  def create_namespace(namespace) do
    broker = broker()

    command = [
      "bin/pulsar-admin",
      "--admin-url",
      broker.admin_url,
      "namespaces",
      "create",
      namespace
    ]

    {_, 0} = docker_exec(command)
    :ok
  end

  def create_topic(topic, partitions \\ 0)

  def create_topic(topic, 0) do
    broker = broker()
    command = ["bin/pulsar-admin", "--admin-url", broker.admin_url, "topics", "create", topic]

    {_, 0} = docker_exec(command)
    :ok
  end

  def create_topic(topic, n) do
    broker = broker()

    command = [
      "bin/pulsar-admin",
      "--admin-url",
      broker.admin_url,
      "topics",
      "create-partitioned-topic",
      topic,
      "--partitions",
      "#{n}"
    ]

    {_, 0} = docker_exec(command)
    :ok
  end

  def unload_topic(topic) do
    broker = broker()
    command = ["bin/pulsar-admin", "--admin-url", broker.admin_url, "topics", "unload", topic]

    {_, 0} = docker_exec(command)
    :ok
  end

  def produce_messages(topic, messages, broker \\ broker()) do
    base_cmd = [
      "bin/pulsar-client",
      "--url",
      broker.service_url,
      "produce",
      topic
    ]

    Enum.each(messages, fn
      {key, message} ->
        {_, 0} = docker_exec(base_cmd ++ ["-m", message, "-k", key])

      message when is_binary(message) ->
        {_, 0} = docker_exec(base_cmd ++ ["-m", message])
    end)

    :ok
  end

  def topic_subscriptions(topic, broker \\ broker()) do
    command = [
      "bin/pulsar-admin",
      "--admin-url",
      broker.admin_url,
      "topics",
      "subscriptions",
      topic
    ]

    case docker_exec(broker.container, command) do
      {raw_subscriptions, 0} ->
        subscriptions = String.split(raw_subscriptions)

        {:ok, subscriptions}

      {error_output, exit_code} ->
        {:error, %{exit_code: exit_code, message: error_output}}
    end
  end

  def list_topics(namespace \\ "public/default", broker \\ broker()) do
    command = [
      "bin/pulsar-admin",
      "--admin-url",
      broker.admin_url,
      "topics",
      "list",
      namespace
    ]

    case docker_exec(broker.container, command) do
      {raw_topics, 0} ->
        topics =
          raw_topics
          |> String.split("\n", trim: true)
          |> Enum.map(&String.trim/1)
          |> Enum.reject(&(&1 == ""))

        {:ok, topics}

      {error_output, exit_code} ->
        {:error, %{exit_code: exit_code, message: error_output}}
    end
  end

  defp docker_exec(command) do
    broker = broker()

    docker_exec(broker.container, command)
  end

  defp docker_exec(container, command) do
    System.cmd("docker", ["exec", container | command], stderr_to_stdout: true)
  end

  defp brokers_up? do
    Enum.all?(@brokers, &broker_up?(&1))
  end

  defp broker_up?(broker) do
    {"ok", 0} == System.cmd("curl", ["-s", broker.health_url])
  end
end
