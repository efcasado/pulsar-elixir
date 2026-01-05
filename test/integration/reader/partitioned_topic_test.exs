defmodule Pulsar.Integration.Reader.PartitionedTopicTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.System

  @moduletag :integration
  @client :reader_partitioned_test_client
  @topic "persistent://public/default/reader-partitioned-test"
  @partitions 3
  @num_messages 100

  setup_all do
    broker = System.broker()

    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    :ok = System.create_topic(@topic, @partitions)

    {:ok, _producer_pid} =
      Pulsar.start_producer(
        @topic,
        client: @client,
        name: :reader_partitioned_test_producer
      )

    for i <- 1..@num_messages do
      Pulsar.send(:reader_partitioned_test_producer, "Message #{i}", client: @client)
    end

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)

    :ok
  end

  test "reads messages from all partitions" do
    {:ok, stream} = Pulsar.Reader.stream(@topic, client: @client)
    result = Enum.take(stream, @num_messages)

    assert length(result) == @num_messages

    payloads = Enum.map(result, & &1.payload)

    for i <- 1..@num_messages do
      assert "Message #{i}" in payloads
    end

    partitions =
      result
      |> Enum.map(& &1.message_id_to_ack.partition)
      |> Enum.uniq()
      |> Enum.sort()

    assert partitions == [0, 1, 2]
  end
end
