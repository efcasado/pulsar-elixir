defmodule Pulsar.Integration.Reader.PartitionedTopicTest do
  use Pulsar.Test.Case, async: true

  @topic "persistent://public/default/reader-partitioned-test"
  @partitions 3
  @num_messages 100

  setup_all do
    :ok = System.create_topic(@topic, @partitions)
    Utils.seed_topic(@topic, Enum.map(1..@num_messages, &"Message #{&1}"), client: @client)

    :ok
  end

  test "reads messages from all partitions" do
    result =
      @topic
      |> Pulsar.Reader.stream(client: @client)
      |> Enum.take(@num_messages)

    assert length(result) == @num_messages

    payloads = Enum.map(result, & &1.payload)

    for i <- 1..@num_messages do
      assert "Message #{i}" in payloads
    end

    partitions =
      result
      |> Enum.map(& &1.message_id.partition)
      |> Enum.uniq()
      |> Enum.sort()

    assert partitions == [0, 1, 2]
  end
end
