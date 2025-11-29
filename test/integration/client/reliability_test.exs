defmodule Pulsar.Integration.Client.ReliabilityTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  require Logger

  @moduletag :integration
  @client :reliability_test_client
  @topic "persistent://public/default/reliability-test-topic"
  @consumer_callback DummyConsumer

  setup_all do
    broker = System.broker()

    :ok = System.create_topic(@topic)

    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)
  end

  test "producer recovers from broker crash" do
    {:ok, group_pid} = Pulsar.start_producer(@topic, producer_options())

    [producer_pid_before_crash] = Pulsar.get_producers(group_pid)

    :ok = Utils.wait_for(fn -> System.broker_for_producer(producer_pid_before_crash, @client) != nil end)

    broker = System.broker_for_producer(producer_pid_before_crash, @client)

    {:ok, broker_pid} = Pulsar.lookup_broker(broker.service_url, client: @client)
    Process.exit(broker_pid, :kill)

    Utils.wait_for(fn -> not Process.alive?(producer_pid_before_crash) end)

    [producer_pid_after_crash] = Pulsar.get_producers(group_pid, client: @client)

    Utils.wait_for(fn -> Process.alive?(producer_pid_before_crash) end)

    # producer crashed due to broker link
    refute Process.alive?(producer_pid_before_crash)
    # producer group supervisor is still alive
    assert Process.alive?(group_pid)
    # a new producer started
    assert Process.alive?(producer_pid_after_crash)
    # the old and new producers are not the same
    assert producer_pid_before_crash != producer_pid_after_crash
  end

  test "consumer recovers from broker crash" do
    {:ok, group_pid} =
      Pulsar.start_consumer(
        @topic,
        "broker-crash",
        @consumer_callback,
        subscription_options()
      )

    [consumer_pid_before_crash] = Pulsar.get_consumers(group_pid, client: @client)

    Utils.wait_for(fn -> System.broker_for_consumer(consumer_pid_before_crash, @client) != nil end)

    broker = System.broker_for_consumer(consumer_pid_before_crash, @client)

    {:ok, broker_pid} = Pulsar.lookup_broker(broker.service_url, client: @client)
    Process.exit(broker_pid, :kill)

    Utils.wait_for(fn -> not Process.alive?(consumer_pid_before_crash) end)

    [consumer_pid_after_crash] = Pulsar.get_consumers(group_pid, client: @client)

    # consumer crashed due to broker link
    refute Process.alive?(consumer_pid_before_crash)
    # consumer group supervisor is still alive
    assert Process.alive?(group_pid)
    # a new consumer started
    assert Process.alive?(consumer_pid_after_crash)
    # the old and new consumers are not the same
    assert consumer_pid_before_crash != consumer_pid_after_crash
  end

  test "consumer recovers from broker-initiated topic unload" do
    {:ok, group_pid} =
      Pulsar.start_consumer(
        @topic,
        "topic-unload",
        DummyConsumer,
        subscription_options()
      )

    [consumer_pid_before_unload] = Pulsar.get_consumers(group_pid, client: @client)

    :ok = System.unload_topic(@topic)

    Utils.wait_for(fn -> not Process.alive?(consumer_pid_before_unload) end)

    [consumer_pid_after_unload] = Pulsar.get_consumers(group_pid, client: @client)

    # original consumer crashed due to topic unload
    refute Process.alive?(consumer_pid_before_unload)
    # consumer group supervisor is still alive
    assert Process.alive?(group_pid)
    # a new consumer started
    assert Process.alive?(consumer_pid_after_unload)
    # the old and new consumers are not the same
    assert consumer_pid_before_unload != consumer_pid_after_unload
  end

  defp subscription_options do
    [
      client: @client
    ]
  end

  defp producer_options do
    [
      client: @client
    ]
  end
end
