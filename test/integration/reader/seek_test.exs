defmodule Pulsar.Integration.Reader.SeekTest do
  use Pulsar.Test.Case, async: true

  @topic "persistent://public/default/reader-seek-test"
  @num_messages 10

  setup_all do
    {:ok, producer} = Pulsar.Producer.start(@topic, client: @client, name: "seek-seed")
    :ok = Pulsar.Producer.await_ready(producer)

    # Seeking to a timestamp can only name one message if no two share one. The producer stamps
    # a message as it sends, to the millisecond, so a message per millisecond is what it takes.
    message_ids =
      Enum.map(1..@num_messages, fn position ->
        Process.sleep(2)
        {:ok, message_id} = Pulsar.Producer.send(producer, "Message #{position}")

        message_id
      end)

    # The stamp itself only comes back off the topic, on the message that carries it.
    publish_times =
      @topic
      |> Pulsar.Reader.stream(client: @client)
      |> Enum.take(@num_messages)
      |> Enum.map(&Pulsar.Message.publish_time/1)

    seeds =
      [message_ids, publish_times]
      |> Enum.zip_with(fn [message_id, publish_time] ->
        %{message_id: message_id, publish_time: publish_time}
      end)
      |> Enum.with_index(1)
      |> Map.new(fn {seed, position} -> {position, seed} end)

    assert publish_times == Enum.uniq(publish_times), "messages have to be stamped apart"

    {:ok, seeds: seeds}
  end

  test "reads from the message id it is given", %{seeds: seeds} do
    %{message_id: %{ledgerId: ledger_id, entryId: entry_id}} = seeds[5]

    messages =
      @topic
      |> Pulsar.Reader.stream(
        client: @client,
        start_message_id: {ledger_id, entry_id}
      )
      |> Enum.take(6)

    assert length(messages) == 6
    payloads = Enum.map(messages, & &1.payload)
    assert payloads == ["Message 5", "Message 6", "Message 7", "Message 8", "Message 9", "Message 10"]
  end

  test "reads from the timestamp it is given", %{seeds: seeds} do
    %{publish_time: publish_time} = seeds[5]

    messages =
      @topic
      |> Pulsar.Reader.stream(
        client: @client,
        start_timestamp: publish_time
      )
      |> Enum.take(6)

    assert length(messages) == 6
    payloads = Enum.map(messages, & &1.payload)
    assert payloads == ["Message 5", "Message 6", "Message 7", "Message 8", "Message 9", "Message 10"]
  end
end
