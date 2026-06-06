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
      Pulsar.start_consumer(
        @topic,
        "partitioned-consumers",
        @consumer_callback,
        subscription_options(2)
      )

    consumers = Pulsar.get_consumers(partitioned_consumer_pid)

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

    partition_groups = Pulsar.PartitionedConsumer.get_partition_groups(partitioned_consumer_pid)

    # The number of partition groups should be equal to the number of
    # partitions in the topic. The total number of consumers should be
    # equal to the number of partitions times the number of consumers per
    # partition. Last but not least, all messages produced should be
    # consumed.
    assert length(partition_groups) == 3
    assert Enum.count(consumers) == 6
    assert consumed_messages == expected_count
  end

  test "discovers partitions added to the topic" do
    test_id = :erlang.unique_integer([:positive])
    topic = "persistent://public/default/partition-discovery-consumer-#{test_id}"

    System.create_topic(topic, 3)

    opts = Keyword.put(subscription_options(1), :partition_discovery_interval_ms, @discovery_interval_ms)

    {:ok, partitioned_consumer_pid} =
      Pulsar.start_consumer(topic, "partition-discovery-#{test_id}", @consumer_callback, opts)

    assert wait_for_partition_count(partitioned_consumer_pid, 3) == :ok

    System.update_partitions(topic, 6)

    # The discovery poller should pick up the new partitions and start a
    # consumer group for each one, without restarting the existing groups.
    assert wait_for_partition_count(partitioned_consumer_pid, 6) == :ok

    :ok = Pulsar.stop_consumer(partitioned_consumer_pid)
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

  defp wait_for_partition_count(partitioned_consumer_pid, expected) do
    Utils.wait_for(fn ->
      length(Pulsar.PartitionedConsumer.get_partition_groups(partitioned_consumer_pid)) == expected
    end)
  end
end
