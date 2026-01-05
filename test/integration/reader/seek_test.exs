defmodule Pulsar.Integration.Reader.SeekTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.System

  @moduletag :integration
  @client :reader_seek_test_client
  @topic "persistent://public/default/reader-seek-test"
  @num_messages 10

  setup_all do
    broker = System.broker()

    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    {:ok, _producer_pid} =
      Pulsar.start_producer(
        @topic,
        client: @client,
        name: :reader_seek_test_producer
      )

    message_ids =
      for i <- 1..@num_messages do
        payload = "Message #{i}"
        {:ok, message_id} = Pulsar.send(:reader_seek_test_producer, payload, client: @client)
        {i, message_id}
      end

    # Read all messages to get their publish times from Pulsar
    {:ok, stream} = Pulsar.Reader.stream(@topic, client: @client, timeout: 100)
    messages = Enum.to_list(stream)

    # Build lookup by payload -> {message_id, publish_time}
    message_info =
      messages
      |> Enum.with_index(1)
      |> Map.new(fn {msg, i} ->
        {i, %{message_id: Map.new(message_ids)[i], publish_time: msg.metadata.publish_time}}
      end)

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)

    {:ok, message_info: message_info}
  end

  test "read from specific message_id", %{message_info: message_info} do
    %{message_id: %{ledgerId: ledger_id, entryId: entry_id}} = message_info[5]

    {:ok, stream} =
      Pulsar.Reader.stream(@topic,
        client: @client,
        start_message_id: {ledger_id, entry_id},
        timeout: 100
      )

    messages = Enum.to_list(stream)

    assert length(messages) == 6
    payloads = Enum.map(messages, & &1.payload)
    assert payloads == ["Message 5", "Message 6", "Message 7", "Message 8", "Message 9", "Message 10"]
  end

  test "read from timestamp", %{message_info: message_info} do
    %{publish_time: publish_time} = message_info[5]

    {:ok, stream} =
      Pulsar.Reader.stream(@topic,
        client: @client,
        start_timestamp: publish_time,
        timeout: 100
      )

    messages = Enum.to_list(stream)

    assert length(messages) == 6
    payloads = Enum.map(messages, & &1.payload)
    assert payloads == ["Message 5", "Message 6", "Message 7", "Message 8", "Message 9", "Message 10"]
  end
end
