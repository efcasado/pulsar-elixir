defmodule Pulsar.Integration.Producer.DeduplicationTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils
  alias Pulsar.Topology

  @moduletag :integration
  @client :producer_dedup_test_client
  @topic "persistent://public/default/producer-dedup-test"

  # The broker's "not persisted" message id, which reaches us as an unsigned 64-bit -1.
  @deduplicated 18_446_744_073_709_551_615

  setup_all do
    broker = System.broker()
    {:ok, _} = Pulsar.Client.start_link(name: @client, host: broker.service_url)
    on_exit(fn -> Pulsar.Client.stop(@client) end)
  end

  setup [:telemetry_listen]

  describe "a topic with deduplication enabled" do
    test "a producer restarting under the same name resumes past the sequence id it reached" do
      {consumer_pid, topic} = setup_topic("resume")

      producer = start_producer(topic, "resume-producer")
      first = Enum.map(1..5, &"first-#{&1}")
      Enum.each(first, fn payload -> assert {:ok, _} = Pulsar.Producer.send(producer, payload) end)

      assert worker_state(producer).sequence_id == 5
      assert :ok = Pulsar.Producer.stop(producer, client: @client)

      producer = start_producer(topic, "resume-producer")
      assert worker_state(producer).sequence_id == 5

      second = Enum.map(1..5, &"second-#{&1}")

      for payload <- second do
        assert {:ok, %MessageIdData{} = message_id} = Pulsar.Producer.send(producer, payload)
        refute message_id.ledgerId == @deduplicated
      end

      assert_messages_received(consumer_pid, first ++ second)
    end

    # Resuming the counter stops a producer colliding with itself, but two sharing a name, or a
    # topic whose mark rolled back, still reissue an id the broker has seen. Rewinding the
    # counter is how that is reached from a single producer.
    test "a send the broker discards as a duplicate answers with no message id" do
      {consumer_pid, topic} = setup_topic("reissued")

      producer = start_producer(topic, "reissued-producer")
      assert {:ok, _} = Pulsar.Producer.send(producer, "stored")

      [worker] = Topology.workers(producer)
      :sys.replace_state(worker, &%{&1 | sequence_id: 0})

      # Discarded on its sequence id, despite this payload never having been published.
      assert {:ok, :deduplicated} = Pulsar.Producer.send(producer, "discarded")

      # Published after the discarded one and on the same topic, so its arrival is the point at
      # which "discarded" would have arrived too had the broker stored it.
      assert {:ok, _} = Pulsar.Producer.send(producer, "barrier")
      assert_messages_received(consumer_pid, ["stored", "barrier"])

      assert ["stored", "barrier"] == consumer_pid |> DummyConsumer.get_messages() |> Enum.map(& &1.payload)
    end

    test "a chunked message is not deduplicated, though an unchunked one at the same id is" do
      {_consumer_pid, topic} = setup_topic("chunked")

      producer = start_producer(topic, "chunked-producer", chunking_enabled: true, max_message_size: 32)

      # Three unchunked sends take ids 1..3, so that is the mark the broker holds.
      for payload <- ["one", "two", "three"], do: assert({:ok, _} = Pulsar.Producer.send(producer, payload))

      [worker] = Topology.workers(producer)
      :sys.replace_state(worker, &%{&1 | sequence_id: 1})

      # Four chunks over ids 2..5, the first two of them at or below the mark.
      assert {:ok, %{num_chunks: 4}} = Pulsar.Producer.send(producer, String.duplicate("x", 128))
      assert worker_state(producer).pending_frames == %{}

      # The same id, unchunked, is discarded -- so it is is_chunk that exempts them.
      :sys.replace_state(worker, &%{&1 | sequence_id: 1})
      assert {:ok, :deduplicated} = Pulsar.Producer.send(producer, "unchunked")
    end

    @tag telemetry_listen: [[:pulsar, :producer, :message, :deduplicated]]
    test "a discarded batch answers every caller and reports the messages it carried" do
      {_consumer_pid, topic} = setup_topic("batch")

      producer = start_producer(topic, "batch-producer", batch_enabled: true, batch_size: 3, flush_interval: 30_000)
      producer_name = worker_state(producer).producer_name

      assert [ok: _, ok: _, ok: _] = send_concurrently(producer, ["a", "b", "c"])

      [worker] = Topology.workers(producer)
      :sys.replace_state(worker, &%{&1 | sequence_id: 0})

      assert [ok: :deduplicated, ok: :deduplicated, ok: :deduplicated] =
               send_concurrently(producer, ["d", "e", "f"])

      assert [%{count: 3}] =
               Utils.collect_events([:pulsar, :producer, :message, :deduplicated], producer_names: [producer_name])
    end
  end

  # Helpers

  defp setup_topic(suffix) do
    topic = @topic <> "-" <> suffix
    :ok = System.create_topic(topic)
    :ok = System.enable_deduplication(topic)

    {:ok, _consumer_group} =
      Pulsar.Consumer.start(topic, "dedup-#{suffix}-sub", DummyConsumer,
        client: @client,
        initial_position: :earliest,
        init_args: [notify_pid: self()]
      )

    [consumer_pid] = Utils.wait_for_consumer_ready(1)

    {consumer_pid, topic}
  end

  defp start_producer(topic, name, opts \\ []) do
    {:ok, producer} = Pulsar.Producer.start(topic, [client: @client, name: name] ++ opts)

    Utils.wait_for(fn -> Topology.workers(producer) end,
      until: fn
        [worker] -> :sys.get_state(worker).ready
        _workers -> false
      end
    )

    producer
  end

  defp worker_state(producer) do
    [worker] = Topology.workers(producer)
    :sys.get_state(worker)
  end

  defp send_concurrently(producer, payloads) do
    payloads
    |> Enum.map(fn payload -> Task.async(fn -> Pulsar.Producer.send(producer, payload) end) end)
    |> Task.await_many(10_000)
  end

  defp assert_messages_received(consumer_pid, expected_payloads) do
    Utils.wait_for(fn -> DummyConsumer.count_messages(consumer_pid) >= length(expected_payloads) end)
    payloads = consumer_pid |> DummyConsumer.get_messages() |> Enum.map(& &1.payload)
    Enum.each(expected_payloads, fn expected -> assert expected in payloads end)
  end
end
