defmodule Pulsar.Integration.Reader.CommonTest do
  use Pulsar.Test.Case, async: true

  @topic "persistent://public/default/reader-common-test"
  @num_messages 100

  setup_all do
    Utils.seed_topic(@topic, Enum.map(1..@num_messages, &"Message #{&1}"), client: @client)

    :ok
  end

  # Without a :timeout the stream waits on the topic rather than ending, so taking a few
  # messages from it returning at all is the laziness: nothing here reads the other 95.
  test "is lazy, and composes with the Stream functions" do
    payloads =
      @topic
      |> Pulsar.Reader.stream(client: @client)
      |> Stream.map(& &1.payload)
      |> Stream.filter(&String.ends_with?(&1, "0"))
      |> Enum.take(5)

    assert payloads == Enum.map([10, 20, 30, 40, 50], &"Message #{&1}")
  end

  test "reads the topic from the start again on each enumeration" do
    stream = Pulsar.Reader.stream(@topic, client: @client)

    assert Enum.map(Enum.take(stream, 3), & &1.payload) == ["Message 1", "Message 2", "Message 3"]
    assert Enum.map(Enum.take(stream, 3), & &1.payload) == ["Message 1", "Message 2", "Message 3"]
  end

  test "ends once the topic is drained, given a timeout to end on" do
    payloads =
      @topic
      |> Pulsar.Reader.stream(client: @client, timeout: 2_000)
      |> Enum.map(& &1.payload)

    assert payloads == Enum.map(1..@num_messages, &"Message #{&1}")
  end

  test "closes the consumer it opened once the stream is done with it" do
    assert @topic
           |> Pulsar.Reader.stream(client: @client)
           |> Enum.take(@num_messages)
           |> Enum.count() == @num_messages

    assert Pulsar.Client.consumers(@client) == []
  end

  # Each reader needs a subscription of its own. This cannot reproduce the cross-node collision
  # a per-VM counter causes, only the guarantee that readers do not interfere.
  test "two readers on one topic each read all of it" do
    # A longer idle timeout than the tests above: two readers sharing the topic take turns, so
    # 100ms of quiet is not evidence that the topic is drained.
    read = fn -> @topic |> Pulsar.Reader.stream(client: @client, timeout: 2_000) |> Enum.count() end

    [first, second] = [read, read] |> Enum.map(&Task.async/1) |> Task.await_many(30_000)

    assert first == @num_messages
    assert second == @num_messages
  end

  test "reading from :latest yields nothing published before it" do
    unique_topic = "persistent://public/default/reader-empty-#{:erlang.unique_integer([:positive])}"

    {:ok, _producer} =
      Pulsar.Producer.start(unique_topic,
        client: @client,
        name: :"empty_producer_#{:erlang.unique_integer([:positive])}"
      )

    result =
      unique_topic
      |> Pulsar.Reader.stream(
        client: @client,
        start_position: :latest,
        timeout: 500
      )
      |> Enum.take(1)

    assert result == []
  end
end
