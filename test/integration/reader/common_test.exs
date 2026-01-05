defmodule Pulsar.Integration.Reader.CommonTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.System

  @moduletag :integration
  @client :reader_common_test_client
  @topic "persistent://public/default/reader-common-test"
  @num_messages 100

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
        name: :reader_common_test_producer
      )

    for i <- 1..@num_messages do
      payload = "Message #{i}"
      Pulsar.send(:reader_common_test_producer, payload, client: @client)
    end

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)

    :ok
  end

  test "basic streaming with Enum.take" do
    {:ok, stream} = Pulsar.Reader.stream(@topic, client: @client)
    result = Enum.take(stream, 5)

    assert length(result) == 5
  end

  test "stream pipeline with lazy evaluation" do
    {:ok, stream} = Pulsar.Reader.stream(@topic, client: @client)

    result =
      stream
      |> Stream.map(fn msg -> String.replace(msg.payload, "Message ", "") end)
      |> Stream.map(&String.to_integer/1)
      |> Stream.filter(fn n -> rem(n, 2) == 0 end)
      |> Enum.take(5)

    assert result == [2, 4, 6, 8, 10]
  end

  test "each enumeration restarts from beginning" do
    {:ok, stream1} = Pulsar.Reader.stream(@topic, client: @client)
    {:ok, stream2} = Pulsar.Reader.stream(@topic, client: @client)

    first_batch = Enum.take(stream1, 3)
    second_batch = Enum.take(stream2, 3)

    assert Enum.map(first_batch, & &1.payload) == ["Message 1", "Message 2", "Message 3"]
    assert Enum.map(second_batch, & &1.payload) == ["Message 1", "Message 2", "Message 3"]
  end

  test "Stream.take vs Enum.take in pipeline" do
    {:ok, stream} = Pulsar.Reader.stream(@topic, client: @client)

    result =
      stream
      |> Stream.take(10)
      |> Stream.map(fn msg -> msg.payload end)
      |> Enum.take(5)

    assert result == ["Message 1", "Message 2", "Message 3", "Message 4", "Message 5"]
  end

  test "process all messages with Enum.each" do
    counter = :counters.new(1, [:atomics])

    {:ok, stream} = Pulsar.Reader.stream(@topic, client: @client, timeout: 100)
    Enum.each(stream, fn _msg -> :counters.add(counter, 1, 1) end)

    assert :counters.get(counter, 1) == @num_messages
  end

  test "consume in chunks using Stream.chunk_every" do
    {:ok, stream} = Pulsar.Reader.stream(@topic, client: @client, timeout: 100)

    chunks =
      stream
      |> Stream.map(& &1.payload)
      |> Stream.chunk_every(3)
      |> Enum.to_list()

    assert length(chunks) == 34
    assert hd(chunks) == ["Message 1", "Message 2", "Message 3"]
    assert List.last(chunks) == ["Message 100"]
  end

  test "empty stream when no messages available from latest" do
    unique_topic = "persistent://public/default/reader-empty-#{:erlang.unique_integer([:positive])}"

    {:ok, _producer} =
      Pulsar.start_producer(unique_topic,
        client: @client,
        name: :"empty_producer_#{:erlang.unique_integer([:positive])}"
      )

    {:ok, stream} =
      Pulsar.Reader.stream(unique_topic,
        client: @client,
        start_position: :latest,
        timeout: 500
      )

    result = Enum.take(stream, 1)

    assert result == []
  end
end
