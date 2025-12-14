defmodule Pulsar.Integration.Producer.BatchTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  require Logger

  @moduletag :integration
  @client :producer_batch_test_client
  @topic "persistent://public/default/producer-batch-test"

  setup_all do
    broker = System.broker()

    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)
  end

  describe "batch producer" do
    test "sends batch when batch_size reached" do
      topic = @topic <> "-size-threshold"
      :ok = System.create_topic(topic)

      {:ok, consumer_group_pid} =
        Pulsar.start_consumer(
          topic,
          "batch-size-test-sub",
          DummyConsumer,
          client: @client,
          initial_position: :earliest,
          init_args: [notify_pid: self()]
        )

      [consumer_pid] = wait_for_consumer_ready(1)

      {:ok, producer_pid} =
        Pulsar.start_producer(topic,
          client: @client,
          name: "batch-size-producer",
          batch_enabled: true,
          batch_size: 3,
          # Long interval to ensure size triggers flush
          flush_interval: 30_000
        )

      [producer] = Pulsar.get_producers(producer_pid)

      Utils.wait_for(fn ->
        state = :sys.get_state(producer)
        state.producer_name != nil
      end)

      tasks =
        Enum.map(["msg1", "msg2", "msg3"], fn msg ->
          Task.async(fn -> Pulsar.send(producer_pid, msg) end)
        end)

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &match?({:ok, _}, &1))

      # Send one more to start second batch (will flush on timer or next batch)
      Task.async(fn -> Pulsar.send(producer_pid, "In second batch") end)

      Utils.wait_for(fn -> DummyConsumer.count_messages(consumer_pid) == 3 end)

      messages = DummyConsumer.get_messages(consumer_pid)
      payloads = Enum.map(messages, & &1.payload)

      assert "msg1" in payloads
      assert "msg2" in payloads
      assert "msg3" in payloads

      Pulsar.stop_producer(producer_pid)
      Pulsar.stop_consumer(consumer_group_pid)
    end

    test "sends batch when flush_interval expires" do
      topic = @topic <> "-timer-flush"
      :ok = System.create_topic(topic)

      # Start consumer first
      {:ok, consumer_group_pid} =
        Pulsar.start_consumer(
          topic,
          "batch-timer-test-sub",
          DummyConsumer,
          client: @client,
          initial_position: :earliest,
          init_args: [notify_pid: self()]
        )

      # Wait for consumer ready
      assert_receive {:consumer_ready, consumer_pid}, 5000

      # Start producer with batching enabled - small flush interval
      {:ok, producer_pid} =
        Pulsar.start_producer(topic,
          client: @client,
          name: "batch-timer-producer",
          batch_enabled: true,
          # High batch size to ensure timer triggers
          batch_size: 100,
          # 100ms flush interval
          flush_interval: 100
        )

      # Wait for producer to be ready
      Utils.wait_for(fn ->
        case Pulsar.get_producers(producer_pid) do
          [p | _] ->
            state = :sys.get_state(p)
            state.ready == true

          _ ->
            false
        end
      end)

      # Send 2 messages (below batch_size, should flush on timer)
      assert {:ok, _} = Pulsar.send(producer_pid, "timer-msg1")
      assert {:ok, _} = Pulsar.send(producer_pid, "timer-msg2")

      # Wait for timer flush and consumption
      Utils.wait_for(
        fn ->
          DummyConsumer.count_messages(consumer_pid) >= 2
        end,
        50
      )

      messages = DummyConsumer.get_messages(consumer_pid)
      payloads = Enum.map(messages, & &1.payload)

      assert "timer-msg1" in payloads
      assert "timer-msg2" in payloads

      Pulsar.stop_producer(producer_pid)
      Pulsar.stop_consumer(consumer_group_pid)
    end
  end

  def wait_for_consumer_ready(count, timeout \\ 5000) do
    Enum.map(1..count, fn _ ->
      receive do
        {:consumer_ready, pid} -> pid
      after
        timeout -> flunk("Timeout waiting for consumer to be ready")
      end
    end)
  end
end
