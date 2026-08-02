defmodule Pulsar.Integration.Consumer.PartitionedTopicTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :partition_topic_test_client
  @topic "persistent://public/default/partition-topic-test"
  @consumer_callback Pulsar.Test.Support.DummyConsumer
  @discovery_interval_ms 200
  @messages [
    {"key1", "Message 1 for key1"},
    {"key2", "Message 1 for key2"},
    {"key1", "Message 2 for key1"},
    {"key2", "Message 2 for key2"},
    {"key3", "Message 1 for key3"},
    {"key4", "Message 1 for key4"}
  ]

  setup_all do
    broker = System.broker()

    System.create_topic(@topic, 3)

    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    System.produce_messages(@topic, @messages)

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)

    {:ok, expected_count: Enum.count(@messages)}
  end

  test "partitioned consumers", %{expected_count: expected_count} do
    {:ok, partitioned_consumer_pid} =
      Pulsar.Consumer.start(
        @topic,
        "partitioned-consumers",
        @consumer_callback,
        subscription_options(2)
      )

    assert Pulsar.Client.consumers(@client) == [partitioned_consumer_pid]
    assert :ok = wait_for_worker_count(partitioned_consumer_pid, 6)

    consumers = Pulsar.Topology.workers(partitioned_consumer_pid)

    Utils.wait_for(fn ->
      consumers
      |> Enum.reduce(0, fn consumer_pid, acc ->
        @consumer_callback.count_messages(consumer_pid) + acc
      end)
      |> Kernel.==(expected_count)
    end)

    consumed_messages =
      Enum.reduce(consumers, 0, fn consumer_pid, acc ->
        @consumer_callback.count_messages(consumer_pid) + acc
      end)

    assert Enum.count(consumers) == 6
    assert consumed_messages == expected_count

    :ok = Pulsar.Consumer.stop(partitioned_consumer_pid)
  end

  test "discovers partitions added to the topic" do
    test_id = :erlang.unique_integer([:positive])
    topic = "persistent://public/default/partition-discovery-consumer-#{test_id}"

    System.create_topic(topic, 3)

    opts = Keyword.put(subscription_options(1), :partition_discovery_interval_ms, @discovery_interval_ms)

    {:ok, partitioned_consumer_pid} =
      Pulsar.Consumer.start(topic, "partition-discovery-#{test_id}", @consumer_callback, opts)

    assert wait_for_worker_count(partitioned_consumer_pid, 3) == :ok
    initial_workers = Pulsar.Topology.workers(partitioned_consumer_pid)

    System.update_partitions(topic, 6)

    # The discovery poller should pick up the new partitions and start a
    # consumer group for each one, without restarting the existing groups.
    assert wait_for_worker_count(partitioned_consumer_pid, 6) == :ok
    current_workers = Pulsar.Topology.workers(partitioned_consumer_pid)
    assert Enum.all?(initial_workers, &(&1 in current_workers))

    :ok = Pulsar.Consumer.stop(partitioned_consumer_pid)
  end

  test "topic/1 answers for a partitioned consumer and its workers without stopping them" do
    {:ok, partitioned} =
      Pulsar.Consumer.start(@topic, "topic-at-every-level", @consumer_callback, subscription_options(1))

    assert :ok = Utils.wait_for(fn -> Pulsar.Consumer.topic(partitioned) == @topic end)

    workers = Pulsar.Topology.workers(partitioned)

    assert Pulsar.Consumer.topic(partitioned) == @topic
    assert length(workers) == 3
    assert Enum.all?(workers, &(Pulsar.Consumer.topic(&1) =~ "#{@topic}-partition-"))

    assert Process.alive?(partitioned)
    assert Enum.all?(workers, &Process.alive?/1)

    :ok = Pulsar.Consumer.stop(partitioned)
  end

  defp subscription_options(count) do
    [
      client: @client,
      initial_position: :earliest,
      consumer_count: count,
      flow_initial: 1,
      flow_threshold: 0,
      flow_refill: 1
    ]
  end

  defp wait_for_worker_count(partitioned_consumer_pid, expected) do
    Utils.wait_for(fn ->
      case Pulsar.Topology.workers(partitioned_consumer_pid) do
        workers when is_list(workers) -> length(workers) == expected
        _not_ready -> false
      end
    end)
  end
end
