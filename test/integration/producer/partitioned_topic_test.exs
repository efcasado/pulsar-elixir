defmodule Pulsar.Integration.Producer.PartitionedTopicTest do
  use Pulsar.Test.Case, async: true

  alias Pulsar.Test.Support.DummyConsumer

  @topic "persistent://public/default/partitioned-producer-test"
  @discovery_interval_ms 200
  @reconciliation [:pulsar, :topology, :reconciliation, :stop]

  setup_all do
    System.create_topic(@topic, 3)

    :ok
  end

  test "starts a worker for every partition of the topic" do
    {:ok, producer_pid} =
      Pulsar.Producer.start(@topic,
        client: @client,
        name: "partitioned-producer-test-1"
      )

    assert Pulsar.Client.producers(@client) == [producer_pid]

    :ok = Pulsar.Producer.await_ready(producer_pid)

    assert Enum.count(Topology.workers(producer_pid)) == 3

    :ok = Pulsar.Producer.stop(producer_pid)
  end

  test "routes messages sharing a partition key to one partition" do
    test_id = :erlang.unique_integer([:positive])
    subscription = "partitioned-test-#{test_id}"
    producer_name = "partitioned-producer-test-#{test_id}"

    {:ok, producer_pid} =
      Pulsar.Producer.start(@topic,
        client: @client,
        name: producer_name
      )

    :ok = Pulsar.Producer.await_ready(producer_pid)

    {:ok, consumer_pid} =
      Pulsar.Consumer.start(
        @topic,
        subscription,
        DummyConsumer,
        client: @client,
        initial_position: :latest,
        init_args: [forward_to: self()]
      )

    :ok = Pulsar.Consumer.await_ready(consumer_pid)

    partition_key = "same-partition-key-#{test_id}"
    messages = ["e2e-msg-1-#{test_id}", "e2e-msg-2-#{test_id}", "e2e-msg-3-#{test_id}"]

    for msg <- messages do
      {:ok, _} = Pulsar.Producer.send(producer_pid, msg, partition_key: partition_key)
    end

    our_messages = receive_messages(messages)

    assert [^partition_key] =
             our_messages
             |> Enum.map(fn msg -> Pulsar.Message.key(msg) end)
             |> Enum.uniq()

    assert [_single_partition] =
             our_messages
             |> Enum.map(fn msg -> msg.raw.command.message_id.partition end)
             |> Enum.uniq()

    :ok = Pulsar.Producer.stop(producer_pid)
    :ok = Pulsar.Consumer.stop(consumer_pid)
  end

  test "spreads keyless messages across every partition" do
    test_id = :erlang.unique_integer([:positive])
    subscription = "partitioned-random-test-#{test_id}"
    producer_name = "partitioned-producer-random-test-#{test_id}"

    {:ok, producer_pid} =
      Pulsar.Producer.start(@topic,
        client: @client,
        name: producer_name
      )

    :ok = Pulsar.Producer.await_ready(producer_pid)

    {:ok, consumer_pid} =
      Pulsar.Consumer.start(
        @topic,
        subscription,
        DummyConsumer,
        client: @client,
        initial_position: :latest,
        init_args: [forward_to: self()]
      )

    :ok = Pulsar.Consumer.await_ready(consumer_pid)

    messages =
      for i <- 1..30 do
        msg = "random-msg-#{i}-#{test_id}"
        {:ok, _} = Pulsar.Producer.send(producer_pid, msg)
        msg
      end

    partitions =
      messages
      |> receive_messages()
      |> Enum.map(fn msg -> msg.raw.command.message_id.partition end)

    # With 30 messages and 3 partitions, random distribution should hit all partitions
    assert partitions |> Enum.uniq() |> Enum.count() == 3
    partition_counts = Enum.frequencies(partitions)
    assert Enum.all?(partition_counts, fn {_partition, count} -> count >= 1 end)

    :ok = Pulsar.Producer.stop(producer_pid)
    :ok = Pulsar.Consumer.stop(consumer_pid)
  end

  # Every partition's worker forwards here, so the messages arrive interleaved and are picked
  # out by payload rather than by which worker held them.
  defp receive_messages(payloads) do
    for _payload <- payloads do
      assert_receive {:consumer, _pid, message}
      message
    end
  end

  @tag telemetry_listen: [@reconciliation]
  test "discovers partitions added to the topic" do
    test_id = :erlang.unique_integer([:positive])
    topic = "persistent://public/default/partition-discovery-producer-#{test_id}"

    System.create_topic(topic, 3)

    {:ok, producer_pid} =
      Pulsar.Producer.start(topic,
        client: @client,
        name: "partition-discovery-producer-#{test_id}",
        partition_discovery_interval_ms: @discovery_interval_ms
      )

    assert_receive {:telemetry_event, %{event: @reconciliation, metadata: %{topic: ^topic, partition_count: 3}}}

    initial_workers = Topology.workers(producer_pid)

    System.update_partitions(topic, 6)

    # The poller picks the new partitions up and starts a worker for each, leaving the workers
    # already running where they were.
    assert_receive {:telemetry_event,
                    %{
                      event: @reconciliation,
                      metadata: %{topic: ^topic, partition_count: 6, added_partitions: [3, 4, 5]}
                    }},
                   10_000

    assert Enum.all?(initial_workers, &(&1 in Topology.workers(producer_pid)))

    :ok = Pulsar.Producer.stop(producer_pid)
  end
end
