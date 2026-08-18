defmodule Pulsar.Integration.Producer.BatchTest do
  use Pulsar.Test.Case, async: true

  alias Pulsar.Test.Support.DummyConsumer

  @topic "persistent://public/default/producer-batch-test"
  @batch_published [:pulsar, :producer, :batch, :published]

  describe "batch producer" do
    @tag telemetry_listen: [@batch_published]
    test "sends multiple batches when messages exceed batch_size" do
      {consumer_pid, producer_pid} =
        setup_producer_consumer("multi-batch", batch_size: 3, flush_interval: 30_000)

      [producer] = Topology.workers(producer_pid)
      producer_name = :sys.get_state(producer).producer_name

      messages = Enum.map(1..12, &"msg-#{&1}")
      send_messages(producer_pid, messages)

      assert_messages_received(consumer_pid, messages)

      # Twelve messages at three to a batch, and nothing left over.
      for _batch <- 1..4, do: assert_batch_published(producer_name, 3)
      refute_batch_published(producer_name)
    end

    # A batched message carries its key, properties and event time per entry rather than on the
    # message that carried them, so this is the shape Pulsar.Message's accessors have to resolve.
    @tag telemetry_listen: [@batch_published]
    test "each message in a batch keeps its own key, properties and event time" do
      {consumer_pid, producer_pid} =
        setup_producer_consumer("per-entry-metadata", batch_size: 3, flush_interval: 30_000)

      [producer] = Topology.workers(producer_pid)
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

    @tag telemetry_listen: [@batch_published]
    test "key-based batching gives every key an entry of its own, not just the first" do
      {consumer_pid, producer_pid} =
        setup_producer_consumer("key-based",
          batch_size: 4,
          flush_interval: 30_000,
          batch_builder: :key_based
        )

      [producer] = Topology.workers(producer_pid)
      producer_name = :sys.get_state(producer).producer_name

      keys = ["tenant-1", "tenant-2", "tenant-1", "tenant-2"]

      sends =
        keys
        |> Enum.with_index()
        |> Enum.map(fn {key, i} ->
          Task.async(fn -> Pulsar.Producer.send(producer_pid, "keyed-#{i}", partition_key: key) end)
        end)

      assert Enum.all?(Task.await_many(sends, 10_000), &match?({:ok, _}, &1))

      expected_payloads = Enum.map(0..3, &"keyed-#{&1}")
      assert_messages_received(consumer_pid, expected_payloads)

      # One entry per key, rather than the single entry the default builder would have sent.
      assert_batch_published(producer_name, 2)
      assert_batch_published(producer_name, 2)
      refute_batch_published(producer_name)

      # Every message dispatches on its own key, not on whichever one led the batch.
      for message <- DummyConsumer.get_messages(consumer_pid) do
        assert message.raw.metadata.partition_key == Pulsar.Message.key(message)
      end
    end

    @tag telemetry_listen: [@batch_published]
    test "the entry a batch arrives as carries a key for Key_Shared to dispatch on" do
      {consumer_pid, producer_pid} =
        setup_producer_consumer("entry-key", batch_size: 2, flush_interval: 30_000)

      [producer] = Topology.workers(producer_pid)
      producer_name = :sys.get_state(producer).producer_name

      sends =
        Enum.map(1..2, fn i ->
          Task.async(fn -> Pulsar.Producer.send(producer_pid, "keyed-#{i}", partition_key: "tenant-1") end)
        end)

      assert Enum.all?(Task.await_many(sends, 10_000), &match?({:ok, _}, &1))

      assert_messages_received(consumer_pid, ["keyed-1", "keyed-2"])
      assert_batch_telemetry(count: 2, producer_name: producer_name)

      for message <- DummyConsumer.get_messages(consumer_pid) do
        assert message.raw.metadata.partition_key == "tenant-1"
      end
    end

    # A batch spends one sequence id per message it carries, so the next batch has to start
    # past the whole range. Starting at the previous batch's first id repeats the ids consumers
    # see and understates the high-water mark the broker reports back on reconnect.
    @tag telemetry_listen: [@batch_published]
    test "consecutive batches claim sequence id ranges that do not overlap" do
      {consumer_pid, producer_pid} =
        setup_producer_consumer("sequence-ids", batch_size: 3, flush_interval: 30_000)

      [producer] = Topology.workers(producer_pid)

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

    @tag telemetry_listen: [@batch_published]
    test "flushes single message batch on timer" do
      {consumer_pid, producer_pid} =
        setup_producer_consumer("single-msg", batch_size: 100, flush_interval: 100)

      [producer] = Topology.workers(producer_pid)
      producer_name = :sys.get_state(producer).producer_name

      assert {:ok, _} = Pulsar.Producer.send(producer_pid, "single-msg")

      assert_messages_received(consumer_pid, ["single-msg"])
      assert_batch_telemetry(count: 1, producer_name: producer_name)
    end

    @tag telemetry_listen: [@batch_published]
    test "empty batch flush is no-op" do
      {_consumer_pid, producer_pid} =
        setup_producer_consumer("empty-batch", batch_size: 10, flush_interval: 50)

      # Wait for a few timer cycles without sending anything
      Process.sleep(200)

      [producer] = Topology.workers(producer_pid)
      state = :sys.get_state(producer)
      assert state.ready == true
      assert state.batch == []

      refute_batch_published(state.producer_name)
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

    test "a delayed message keeps its delay, and flushes the batch it could not join" do
      {consumer_pid, producer_pid} =
        setup_producer_consumer("delayed", batch_size: 10, flush_interval: 30_000)

      deliver_at = DateTime.shift(DateTime.utc_now(), second: 1)
      deliver_at_ms = DateTime.to_unix(deliver_at, :millisecond)

      [producer] = Topology.workers(producer_pid)

      # Neither fills the batch, so both wait for a flush that nothing has scheduled yet.
      pending =
        Enum.map(["batched-1", "batched-2"], fn payload ->
          Task.async(fn -> Pulsar.Producer.send(producer_pid, payload) end)
        end)

      # Both are queued before the delayed one arrives, so it is what flushes them.
      Utils.wait_for(fn -> :sys.get_state(producer).batched == 2 end)

      assert {:ok, _message_id} = Pulsar.Producer.send(producer_pid, "delayed-1", deliver_at_time: deliver_at)

      assert Enum.all?(Task.await_many(pending, 10_000), &match?({:ok, _}, &1))

      assert_messages_received(consumer_pid, ["batched-1", "batched-2", "delayed-1"])
      messages = DummyConsumer.get_messages(consumer_pid)
      delayed = Enum.find(messages, &(&1.payload == "delayed-1"))
      batched = Enum.find(messages, &(&1.payload == "batched-1"))

      assert delayed.raw.metadata.deliver_at_time == deliver_at_ms

      # It arrived as an entry of its own: a batched message carries an index into its entry.
      assert delayed.message_id.batch_index == -1
      assert batched.message_id.batch_index >= 0
    end
  end

  # Helpers

  defp setup_producer_consumer(suffix, opts) do
    topic = @topic <> "-" <> suffix
    :ok = System.create_topic(topic)

    {:ok, consumer_group} =
      Pulsar.Consumer.start(topic, "batch-#{suffix}-sub", DummyConsumer,
        client: @client,
        initial_position: :earliest
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [consumer_pid] = Topology.workers(consumer_group)

    {:ok, producer_pid} =
      Pulsar.Producer.start(
        topic,
        [client: @client, name: "#{suffix}-producer", batch_enabled: true] ++ opts
      )

    :ok = Pulsar.Producer.await_ready(producer_pid)

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
    assert_batch_published(producer_name, expected_count)
    refute_batch_published(producer_name)
  end

  # Every test listening for this event is sent every producer's, so the name is what picks
  # this producer's out of the mailbox.
  defp assert_batch_published(producer_name, count) do
    assert_receive {:telemetry_event,
                    %{
                      event: @batch_published,
                      measurements: %{count: ^count},
                      metadata: %{producer_name: ^producer_name}
                    }}
  end

  defp refute_batch_published(producer_name) do
    refute_receive {:telemetry_event, %{event: @batch_published, metadata: %{producer_name: ^producer_name}}}
  end
end
