defmodule Pulsar.Integration.ProducerTest do
  use ExUnit.Case
  import TelemetryTest

  require Logger

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @topic "persistent://public/default/producer-test-topic"

  setup do
    broker = System.broker()

    config = [
      host: broker.service_url
    ]

    {:ok, app_pid} = Pulsar.start(config)

    on_exit(fn ->
      Process.exit(app_pid, :shutdown)
      Utils.wait_for(fn -> not Process.alive?(app_pid) end)
    end)
  end

  setup [:telemetry_listen]

  describe "Producer Lifecycle" do
    @tag telemetry_listen: [[:pulsar, :producer, :opened, :stop]]
    test "create producer successfully" do
      assert {:ok, group_pid} = Pulsar.start_producer(@topic)
      assert Process.alive?(group_pid)

      [producer] = Pulsar.get_producers(group_pid)

      # Wait for producer to complete registration (producer_name assigned by broker)
      :ok =
        Utils.wait_for(fn ->
          state = :sys.get_state(producer)
          state.producer_name != nil
        end)

      stats = Utils.collect_producer_opened_stats()
      assert %{success_count: 1, failure_count: 0, total_count: 1} = stats
    end
  end

  describe "Connection Reliability" do
    test "producer recovers from broker crash" do
      {:ok, group_pid} = Pulsar.start_producer(@topic)

      [producer_pid_before_crash] = Pulsar.get_producers(group_pid)

      # Wait for producer to connect to a broker
      :ok = Utils.wait_for(fn -> System.broker_for_producer(producer_pid_before_crash) != nil end)

      broker = System.broker_for_producer(producer_pid_before_crash)
      {:ok, broker_pid} = Pulsar.lookup_broker(broker.service_url)

      # Kill the broker
      Process.exit(broker_pid, :kill)

      # Wait for original producer to crash due to broker link
      Utils.wait_for(fn -> not Process.alive?(producer_pid_before_crash) end)

      # Wait for supervisor to restart the producer and reconnect to a broker
      [producer_pid_after_crash] = Pulsar.get_producers(group_pid)

      :ok =
        Utils.wait_for(fn ->
          Process.alive?(producer_pid_after_crash) and
            System.broker_for_producer(producer_pid_after_crash) != nil
        end)

      # Original producer crashed
      assert not Process.alive?(producer_pid_before_crash)
      # Producer group supervisor is still alive
      assert Process.alive?(group_pid)
      # A new producer started
      assert Process.alive?(producer_pid_after_crash)
      # The old and new producers are not the same
      assert producer_pid_before_crash != producer_pid_after_crash
    end
  end
end
