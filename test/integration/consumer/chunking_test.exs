defmodule Pulsar.Integration.Consumer.ChunkingTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :chunking_test_client
  @topic "persistent://public/default/chunking-test"
  @consumer_callback Pulsar.Test.Support.DummyConsumer

  setup_all do
    broker = System.broker()

    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)

    :ok
  end

  test "receives and reassembles a simple chunked message" do
    large_message = "This is a test message that will be chunked."

    {:ok, producer} =
      Pulsar.start_producer(
        @topic,
        client: @client,
        name: :chunking_simple_producer,
        chunking_enabled: true,
        max_message_size: 32
      )

    {:ok, consumer_group} =
      Pulsar.start_consumer(
        @topic,
        "chunking-simple",
        @consumer_callback,
        client: @client
      )

    [consumer] = Pulsar.get_consumers(consumer_group)

    Utils.wait_for(fn ->
      state = :sys.get_state(consumer)
      state.callback_state != nil
    end)

    assert byte_size(large_message) == 44

    {:ok, _msg_id} = Pulsar.send(producer, large_message)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == 1
    end)

    messages = @consumer_callback.get_messages(consumer)
    assert length(messages) == 1
    [received_msg] = messages
    assert received_msg.payload == large_message
    assert received_msg.chunk_metadata.chunked == true
    assert received_msg.chunk_metadata.complete == true
    assert received_msg.chunk_metadata.num_chunks == 2
  end

  test "handles interleaved chunks from multiple chunked messages" do
    p1_large_message = "This is a test message that will be chunked from producer 1."
    p2_large_message = "This is a test message that will be chunked from producer 2."

    {:ok, producer1} =
      Pulsar.start_producer(
        @topic,
        client: @client,
        name: :chunking_interleaved_producer1,
        chunking_enabled: true,
        max_message_size: 8
      )

    {:ok, producer2} =
      Pulsar.start_producer(
        @topic,
        client: @client,
        name: :chunking_interleaved_producer2,
        chunking_enabled: true,
        max_message_size: 8
      )

    {:ok, consumer_group} =
      Pulsar.start_consumer(
        @topic,
        "chunking-interleaved",
        @consumer_callback,
        client: @client
      )

    [consumer] = Pulsar.get_consumers(consumer_group)

    Utils.wait_for(fn ->
      state = :sys.get_state(consumer)
      state.callback_state != nil
    end)

    task1 = Task.async(fn -> Pulsar.send(producer1, p1_large_message) end)
    task2 = Task.async(fn -> Pulsar.send(producer2, p2_large_message) end)

    Task.await(task1)
    Task.await(task2)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == 2
    end)

    messages = @consumer_callback.get_messages(consumer)
    assert length(messages) == 2
    payloads = messages |> Enum.map(& &1.payload) |> Enum.sort()
    assert p1_large_message in payloads
    assert p2_large_message in payloads
  end

  test "handles mix of chunked and non-chunked messages" do
    small_message = "Small message"
    large_message = "This is a test message that will be chunked."

    {:ok, producer} =
      Pulsar.start_producer(
        @topic,
        client: @client,
        name: :chunking_mixed_producer,
        chunking_enabled: true,
        max_message_size: 32
      )

    {:ok, consumer_group} =
      Pulsar.start_consumer(
        @topic,
        "chunking-mixed",
        @consumer_callback,
        client: @client
      )

    [consumer] = Pulsar.get_consumers(consumer_group)

    Utils.wait_for(fn ->
      state = :sys.get_state(consumer)
      state.callback_state != nil
    end)

    {:ok, _} = Pulsar.send(producer, small_message)
    {:ok, _} = Pulsar.send(producer, large_message)
    {:ok, _} = Pulsar.send(producer, small_message)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == 3
    end)

    messages = @consumer_callback.get_messages(consumer)
    assert length(messages) == 3

    payloads = Enum.map(messages, & &1.payload)
    assert Enum.count(payloads, &(&1 == small_message)) == 2
    assert large_message in payloads
  end

  test "producer with chunking disabled cannot send 5MB messages" do
    very_large_message = String.duplicate("x", 6_291_456)

    {:ok, producer} =
      Pulsar.start_producer(
        @topic,
        client: @client,
        name: :chunking_disabled_producer,
        chunking_enabled: false
      )

    assert byte_size(very_large_message) == 6_291_456

    assert {:error, _reason} = Pulsar.send(producer, very_large_message)
  end

  test "producer with chunking enabled can send and receive messages larger than 5MB" do
    very_large_message = String.duplicate("x", 6_291_456)

    {:ok, producer} =
      Pulsar.start_producer(
        @topic,
        client: @client,
        name: :chunking_enabled_5mb_producer,
        chunking_enabled: true
      )

    {:ok, consumer_group} =
      Pulsar.start_consumer(
        @topic,
        "chunking-5mb",
        @consumer_callback,
        client: @client
      )

    [consumer] = Pulsar.get_consumers(consumer_group)

    Utils.wait_for(fn ->
      state = :sys.get_state(consumer)
      state.callback_state != nil
    end)

    assert byte_size(very_large_message) == 6_291_456

    {:ok, chunked_msg_id} = Pulsar.send(producer, very_large_message)
    assert is_map(chunked_msg_id)
    assert chunked_msg_id.uuid
    assert chunked_msg_id.num_chunks == 2

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == 1
    end)

    messages = @consumer_callback.get_messages(consumer)
    assert length(messages) == 1
    [received_msg] = messages
    assert received_msg.payload == very_large_message
    assert byte_size(received_msg.payload) == 6_291_456

    assert received_msg.chunk_metadata.chunked == true
    assert received_msg.chunk_metadata.complete == true
    assert received_msg.chunk_metadata.num_chunks == 2
  end
end
