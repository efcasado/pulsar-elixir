defmodule Pulsar.Integration.ProducerConsumerTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  require Logger

  @moduletag :integration

  @topic "persistent://public/default/producer-consumer-topic"
  @subscription "producer-test-subscription"

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

  describe "Producer-Consumer End-to-End interaction" do
    test "produce and consume message successfully" do
      # Start producer
      assert {:ok, group_pid} = Pulsar.start_producer(@topic)
      assert Process.alive?(group_pid)

      [producer] = Pulsar.get_producers(group_pid)

      # Wait for producer to complete registration
      :ok = Utils.wait_for(fn -> :sys.get_state(producer).ready end)

      # Start consumer
      assert {:ok, consumer_pid} = Pulsar.start_consumer(@topic, @subscription, DummyConsumer)

      # Wait for consumer to be ready and subscribed
      [consumer] = Pulsar.get_consumers(consumer_pid)
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
        DummyConsumer.count_messages(consumer) > 0
      end)

      # Verify message was received
      messages = DummyConsumer.get_messages(consumer)
      assert length(messages) == 1
      assert hd(messages).payload == message_payload

      # Cleanup
      assert :ok = Pulsar.stop_consumer(consumer_pid)
      assert :ok = Pulsar.stop_producer(group_pid)
    end

    test "produce and consume compressed message successfully" do
      {:ok, _} =
        Pulsar.start_producer(
          @topic <> "-compression",
          name: "p-none",
          compression: :NONE
        )

      {:ok, _} =
        Pulsar.start_producer(
          @topic <> "-compression",
          name: "p-lz4",
          compression: :LZ4
        )

      {:ok, _} =
        Pulsar.start_producer(
          @topic <> "-compression",
          name: "p-zlib",
          compression: :ZLIB
        )

      {:ok, _} =
        Pulsar.start_producer(
          @topic <> "-compression",
          name: "p-zstd",
          compression: :ZSTD
        )

      {:ok, _} =
        Pulsar.start_producer(
          @topic <> "-compression",
          name: "p-snappy",
          compression: :SNAPPY
        )

      {:ok, consumer_pid} =
        Pulsar.start_consumer(
          @topic,
          @subscription,
          DummyConsumer
        )

      [consumer] = Pulsar.get_consumers(consumer_pid)
      Utils.wait_for(fn -> Process.alive?(consumer) end)

      {:ok, _} = Pulsar.send("p-none", "Hello, world!")
      {:ok, _} = Pulsar.send("p-lz4", "Hello, world!")
      {:ok, _} = Pulsar.send("p-zstd", "Hello, world!")
      {:ok, _} = Pulsar.send("p-zlib", "Hello, world!")
      {:ok, _} = Pulsar.send("p-snappy", "Hello, world!")

      Utils.wait_for(fn ->
        DummyConsumer.count_messages(consumer) == 5
      end)

      all_decoded? =
        consumer
        |> DummyConsumer.get_messages()
        |> Enum.all?(fn message -> message == "Hello, world!" end)

      assert all_decoded?
    end
  end
end
