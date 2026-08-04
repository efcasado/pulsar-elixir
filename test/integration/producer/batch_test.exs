defmodule Pulsar.Integration.Producer.BatchTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils
  alias Pulsar.Topology

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
      {consumer_pid, producer_pid} =
        setup_producer_consumer("multi-batch", batch_size: 3, flush_interval: 30_000)

      [producer] = Utils.wait_for(fn -> Topology.workers(producer_pid) end, until: &match?([_], &1))
      producer_name = :sys.get_state(producer).producer_name

      messages = Enum.map(1..12, &"msg-#{&1}")
      send_messages(producer_pid, messages)

      assert_messages_received(consumer_pid, messages)

      # Should have 4 batch events (3+3+3+3=12 messages)
      events = Utils.collect_events([:pulsar, :producer, :batch, :published], producer_names: [producer_name])
      assert length(events) == 4
      assert Enum.all?(events, fn %{count: c} -> c == 3 end)
    end

    # A batched message carries its key, properties and event time per entry rather than on the
    # message that carried them, so this is the shape Pulsar.Message's accessors have to resolve.
    @tag telemetry_listen: [[:pulsar, :producer, :batch, :published]]
    test "each message in a batch keeps its own key, properties and event time" do
      {consumer_pid, producer_pid} =
        setup_producer_consumer("per-entry-metadata", batch_size: 3, flush_interval: 30_000)

      [producer] = Utils.wait_for(fn -> Topology.workers(producer_pid) end, until: &match?([_], &1))
      producer_name = :sys.get_state(producer).producer_name

      event_time = DateTime.utc_now()
      event_time_ms = DateTime.to_unix(event_time, :millisecond)

      sends =
        Enum.map(1..3, fn i ->
          Task.async(fn ->
            Pulsar.Producer.send(producer_pid, "entry-#{i}",
              partition_key: "key-#{i}",
              properties: %{"entry" => "#{i}"},
              event_time: event_time
            )
          end)
        end)

      assert Enum.all?(Task.await_many(sends, 10_000), &match?({:ok, _}, &1))

      expected_payloads = Enum.map(1..3, &"entry-#{&1}")
      assert_messages_received(consumer_pid, expected_payloads)

      # The assertions below only mean anything if these arrived as one batch.
      assert_batch_telemetry(count: 3, producer_name: producer_name)

      by_payload = Map.new(DummyConsumer.get_messages(consumer_pid), &{&1.payload, &1})

      for i <- 1..3 do
        message = Map.fetch!(by_payload, "entry-#{i}")

        assert Pulsar.Message.key(message) == "key-#{i}"
        assert Pulsar.Message.properties(message) == %{"entry" => "#{i}"}
        assert Pulsar.Message.event_time(message) == event_time_ms
      end
    end

    # A batch spends one sequence id per message it carries, so the next batch has to start
    # past the whole range. Starting at the previous batch's first id repeats the ids consumers
    # see and understates the high-water mark the broker reports back on reconnect.
    @tag telemetry_listen: [[:pulsar, :producer, :batch, :published]]
    test "consecutive batches claim sequence id ranges that do not overlap" do
      {consumer_pid, producer_pid} =
        setup_producer_consumer("sequence-ids", batch_size: 3, flush_interval: 30_000)

      [producer] = Utils.wait_for(fn -> Topology.workers(producer_pid) end, until: &match?([_], &1))

      messages = Enum.map(1..9, &"seq-#{&1}")
      send_messages(producer_pid, messages)

      assert_messages_received(consumer_pid, messages)

      assert :sys.get_state(producer).sequence_id == 9

      sequence_ids =
        consumer_pid
        |> DummyConsumer.get_messages()
        |> Enum.map(& &1.raw.single_metadata.sequence_id)
        |> Enum.sort()

      assert sequence_ids == Enum.to_list(1..9)
    end

    @tag telemetry_listen: [[:pulsar, :producer, :batch, :published]]
    test "flushes single message batch on timer" do
      {consumer_pid, producer_pid} =
        setup_producer_consumer("single-msg", batch_size: 100, flush_interval: 100)

      [producer] = Utils.wait_for(fn -> Topology.workers(producer_pid) end, until: &match?([_], &1))
      producer_name = :sys.get_state(producer).producer_name

      assert {:ok, _} = Pulsar.Producer.send(producer_pid, "single-msg")

      assert_messages_received(consumer_pid, ["single-msg"])
      assert_batch_telemetry(count: 1, producer_name: producer_name)
    end

    @tag telemetry_listen: [[:pulsar, :producer, :batch, :published]]
    test "empty batch flush is no-op" do
      {_consumer_pid, producer_pid} =
        setup_producer_consumer("empty-batch", batch_size: 10, flush_interval: 50)

      # Wait for a few timer cycles without sending anything
      Process.sleep(200)

      [producer] = Utils.wait_for(fn -> Topology.workers(producer_pid) end, until: &match?([_], &1))
      state = :sys.get_state(producer)
      assert state.ready == true
      assert state.batch == []

      assert [] = Utils.collect_events([:pulsar, :producer, :batch, :published], producer_names: [state.producer_name])
    end

    test "refuses a batch the broker would reject, and keeps the connection" do
      {_consumer_pid, producer_pid} =
        setup_producer_consumer("oversized-batch", batch_size: 2, flush_interval: 30_000)

      half_of_an_oversized_batch = String.duplicate("x", 3 * 1024 * 1024)

      assert [{:error, :message_too_large}, {:error, :message_too_large}] =
               send_concurrently(producer_pid, [half_of_an_oversized_batch, half_of_an_oversized_batch])

      # Reaching the broker with this would have closed the connection.
      assert [{:ok, _}, {:ok, _}] = send_concurrently(producer_pid, ["a", "b"])
    end
  end

  # Helpers

  defp setup_producer_consumer(suffix, opts) do
    topic = @topic <> "-" <> suffix
    :ok = System.create_topic(topic)

    {:ok, _consumer_group} =
      Pulsar.Consumer.start(topic, "batch-#{suffix}-sub", DummyConsumer,
        client: @client,
        initial_position: :earliest,
        init_args: [notify_pid: self()]
      )

    [consumer_pid] = Utils.wait_for_consumer_ready(1)

    {:ok, producer_pid} =
      Pulsar.Producer.start(
        topic,
        [client: @client, name: "#{suffix}-producer", batch_enabled: true] ++ opts
      )

    Utils.wait_for(fn -> Topology.workers(producer_pid) end,
      until: fn
        [producer] -> :sys.get_state(producer).ready
        _workers -> false
      end
    )

    {consumer_pid, producer_pid}
  end

  defp send_messages(producer_pid, messages) do
    results = send_concurrently(producer_pid, messages)
    assert Enum.all?(results, &match?({:ok, _}, &1))
  end

  # A batch only flushes once :batch_size messages are in, so its callers have to be waiting
  # at the same time.
  defp send_concurrently(producer_pid, messages) do
    messages
    |> Enum.map(fn msg -> Task.async(fn -> Pulsar.Producer.send(producer_pid, msg) end) end)
    |> Task.await_many(10_000)
  end

  defp assert_messages_received(consumer_pid, expected_payloads) do
    Utils.wait_for(fn -> DummyConsumer.count_messages(consumer_pid) >= length(expected_payloads) end)
    payloads = consumer_pid |> DummyConsumer.get_messages() |> Enum.map(& &1.payload)
    Enum.each(expected_payloads, fn expected -> assert expected in payloads end)
  end

  defp assert_batch_telemetry(count: expected_count, producer_name: producer_name) do
    events = Utils.collect_events([:pulsar, :producer, :batch, :published], producer_names: [producer_name])
    assert [%{count: ^expected_count}] = events
  end
end
