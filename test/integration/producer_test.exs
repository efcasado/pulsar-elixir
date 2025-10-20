defmodule Pulsar.Integration.ProducerTest do
  use ExUnit.Case, async: true
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
    @tag telemetry_listen: [
           [:pulsar, :producer, :opened, :stop],
           [:pulsar, :producer, :closed, :stop]
         ]
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

      assert :ok = Pulsar.stop_producer(group_pid)

      # Wait for producer process to fully terminate
      Utils.wait_for(fn -> not Process.alive?(producer) end)

      close_stats = Utils.collect_producer_closed_stats()
      assert %{success_count: 1, failure_count: 0, total_count: 1} = close_stats
    end

    @tag telemetry_listen: [
           [:pulsar, :producer, :opened, :stop],
           [:pulsar, :producer, :message, :published]
         ]
    test "send message successfully" do
      # Start producer
      assert {:ok, group_pid} = Pulsar.start_producer(@topic)
      assert Process.alive?(group_pid)

      [producer] = Pulsar.get_producers(group_pid)

      # Wait for producer to complete registration
      :ok =
        Utils.wait_for(fn ->
          state = :sys.get_state(producer)
          state.producer_name != nil
        end)

      # Send a message using the producer group name (default pattern)
      producer_group_name = "#{@topic}-producer"
      message_payload = "Hello, Pulsar!"

      Logger.debug("Sending test message")
      assert {:ok, message_id_data} = Pulsar.send(producer_group_name, message_payload)

      # Verify message_id is returned
      assert message_id_data.ledgerId != nil
      assert message_id_data.entryId != nil

      # Verify telemetry event was emitted
      publish_stats = Utils.collect_message_published_stats()
      assert %{total_count: 1} = publish_stats

      # Cleanup
      assert :ok = Pulsar.stop_producer(group_pid)
      Utils.wait_for(fn -> not Process.alive?(producer) end)
    end

    test "send returns error when producer not found" do
      assert {:error, :producer_not_found} = Pulsar.send("non-existent-producer-group", "message")
    end
  end

  describe "Producer-Consumer End-to-End interaction" do
    @subscription "producer-test-subscription"

    test "produce and consume message successfully" do
      # Start producer
      assert {:ok, group_pid} = Pulsar.start_producer(@topic)
      assert Process.alive?(group_pid)

      [producer] = Pulsar.get_producers(group_pid)

      # Wait for producer to complete registration
      :ok =
        Utils.wait_for(fn ->
          state = :sys.get_state(producer)
          state.producer_name != nil
        end)

      # Start consumer
      assert {:ok, [consumer_pid]} =
               Pulsar.start_consumer(
                 topic: @topic,
                 subscription_name: @subscription,
                 subscription_type: :Exclusive,
                 callback_module: Pulsar.Test.Support.DummyConsumer
               )

      # Wait for consumer to be ready and subscribed
      [consumer] = Pulsar.consumers_for_group(consumer_pid)
      Utils.wait_for(fn -> Process.alive?(consumer) end)

      # Wait for consumer to complete subscription and be ready to receive messages
      # This is indicated by flow_outstanding_permits being greater than 0
      Utils.wait_for(fn ->
        state = :sys.get_state(consumer)
        state.flow_outstanding_permits > 0
      end)

      Logger.debug("Producer and Consumer ready. Sending message.")

      # Send a message
      producer_group_name = "#{@topic}-producer"
      message_payload = "Hello from producer to consumer!"
      assert {:ok, message_id_data} = Pulsar.send(producer_group_name, message_payload)
      Logger.debug("Message sent #{inspect(message_id_data)}")

      # Wait for consumer to receive the message
      Utils.wait_for(fn ->
        Pulsar.Test.Support.DummyConsumer.count_messages(consumer) > 0
      end)

      # Verify message was received
      messages = Pulsar.Test.Support.DummyConsumer.get_messages(consumer)
      assert length(messages) == 1
      assert hd(messages).payload == message_payload

      # Cleanup
      assert :ok = Pulsar.stop_consumer(consumer_pid)
      assert :ok = Pulsar.stop_producer(group_pid)
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
      # A new producer started
      assert Process.alive?(producer_pid_after_crash)
      # The old and new producers are not the same
      assert producer_pid_before_crash != producer_pid_after_crash
    end
  end
end
