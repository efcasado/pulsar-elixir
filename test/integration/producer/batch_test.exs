defmodule Pulsar.Integration.Producer.BatchTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :producer_batch_test_client
  @topic "persistent://public/default/producer-batch-test"

  setup_all do
    broker = System.broker()
    {:ok, _} = Pulsar.Client.start_link(name: @client, host: broker.service_url)
    on_exit(fn -> Pulsar.Client.stop(@client) end)
  end

  setup [:telemetry_listen]

  describe "batch producer" do
    @tag telemetry_listen: [[:pulsar, :producer, :batch, :published]]
    test "sends multiple batches when messages exceed batch_size" do
      {consumer_pid, consumer_group, producer_pid} =
        setup_producer_consumer("multi-batch", batch_size: 3, flush_interval: 30_000)

      messages = Enum.map(1..12, &"msg-#{&1}")
      send_messages(producer_pid, messages)

      assert_messages_received(consumer_pid, messages)

      # Should have 4 batch events (3+3+3+3=12 messages)
      events = Utils.collect_events([:pulsar, :producer, :batch, :published])
      assert length(events) == 4
      assert Enum.all?(events, fn %{count: c} -> c == 3 end)

      cleanup(producer_pid, consumer_group)
    end

    @tag telemetry_listen: [[:pulsar, :producer, :batch, :published]]
    test "flushes single message batch on timer" do
      {consumer_pid, consumer_group, producer_pid} =
        setup_producer_consumer("single-msg", batch_size: 100, flush_interval: 100)

      assert {:ok, _} = Pulsar.send(producer_pid, "single-msg")

      assert_messages_received(consumer_pid, ["single-msg"])
      assert_batch_telemetry(count: 1)

      cleanup(producer_pid, consumer_group)
    end

    @tag telemetry_listen: [[:pulsar, :producer, :batch, :published]]
    test "empty batch flush is no-op" do
      {_consumer_pid, consumer_group, producer_pid} =
        setup_producer_consumer("empty-batch", batch_size: 10, flush_interval: 50)

      # Wait for a few timer cycles without sending anything
      Process.sleep(200)

      [producer] = Pulsar.get_producers(producer_pid)
      state = :sys.get_state(producer)
      assert state.ready == true
      assert state.batch == []

      assert [] = Utils.collect_events([:pulsar, :producer, :batch, :published])

      cleanup(producer_pid, consumer_group)
    end
  end

  # Helpers

  defp setup_producer_consumer(suffix, opts) do
    topic = @topic <> "-" <> suffix
    :ok = System.create_topic(topic)

    {:ok, consumer_group} =
      Pulsar.start_consumer(topic, "batch-#{suffix}-sub", DummyConsumer,
        client: @client,
        initial_position: :earliest,
        init_args: [notify_pid: self()]
      )

    consumer_pid = wait_for_consumer_ready()

    {:ok, producer_pid} =
      Pulsar.start_producer(
        topic,
        [client: @client, name: "batch-#{suffix}-producer", batch_enabled: true] ++ opts
      )

    wait_for_producer_ready(producer_pid)

    {consumer_pid, consumer_group, producer_pid}
  end

  defp wait_for_consumer_ready(timeout \\ 5000) do
    receive do
      {:consumer_ready, pid} -> pid
    after
      timeout -> flunk("Timeout waiting for consumer")
    end
  end

  defp wait_for_producer_ready(producer_pid) do
    Utils.wait_for(fn ->
      case Pulsar.get_producers(producer_pid) do
        [p | _] -> :sys.get_state(p).ready == true
        _ -> false
      end
    end)
  end

  defp send_messages(producer_pid, messages) do
    tasks = Enum.map(messages, fn msg -> Task.async(fn -> Pulsar.send(producer_pid, msg) end) end)
    results = Task.await_many(tasks, 10_000)
    assert Enum.all?(results, &match?({:ok, _}, &1))
  end

  defp assert_messages_received(consumer_pid, expected_payloads) do
    Utils.wait_for(fn -> DummyConsumer.count_messages(consumer_pid) >= length(expected_payloads) end)
    payloads = consumer_pid |> DummyConsumer.get_messages() |> Enum.map(& &1.payload)
    Enum.each(expected_payloads, fn expected -> assert expected in payloads end)
  end

  defp assert_batch_telemetry(count: expected_count) do
    events = Utils.collect_events([:pulsar, :producer, :batch, :published])
    assert [%{count: ^expected_count}] = events
  end

  defp cleanup(producer_pid, consumer_group) do
    Pulsar.stop_producer(producer_pid)
    Pulsar.stop_consumer(consumer_group)
  end
end
