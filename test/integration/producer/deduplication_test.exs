defmodule Pulsar.Integration.Producer.DeduplicationTest do
  use ExUnit.Case, async: true

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
        assert {:ok, message_id} = Pulsar.Producer.send(producer, payload)

        # A deduplicated send is still answered, with this in place of a message id.
        refute message_id.ledgerId == @deduplicated
      end

      assert_messages_received(consumer_pid, first ++ second)
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

  defp start_producer(topic, name) do
    {:ok, producer} = Pulsar.Producer.start(topic, client: @client, name: name)

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

  defp assert_messages_received(consumer_pid, expected_payloads) do
    Utils.wait_for(fn -> DummyConsumer.count_messages(consumer_pid) >= length(expected_payloads) end)
    payloads = consumer_pid |> DummyConsumer.get_messages() |> Enum.map(& &1.payload)
    Enum.each(expected_payloads, fn expected -> assert expected in payloads end)
  end
end
