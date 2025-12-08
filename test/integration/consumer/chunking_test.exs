defmodule Pulsar.Integration.Consumer.ChunkingTest do
  use ExUnit.Case, async: true

  import TelemetryTest

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

  setup [:telemetry_listen]

  @tag telemetry_listen: [
         [:pulsar, :consumer, :chunk, :received],
         [:pulsar, :consumer, :chunk, :complete],
         [:pulsar, :producer, :chunk, :start],
         [:pulsar, :producer, :chunk, :complete]
       ]
  test "receives and reassembles a simple chunked message" do
    {:ok, producer} =
      Pulsar.start_producer(
        @topic,
        client: @client,
        name: :chunking_simple_producer,
        chunking_enabled: true,
        # Small chunk size to force chunking
        max_message_size: 1024
      )

    {:ok, consumer_group} =
      Pulsar.start_consumer(
        @topic,
        "chunking-simple",
        @consumer_callback,
        client: @client,
        initial_position: :latest,
        consumer_count: 1
      )

    [consumer] = Pulsar.get_consumers(consumer_group)

    Utils.wait_for(fn ->
      state = :sys.get_state(consumer)
      state.callback_state != nil
    end)

    # Send a large message that will be chunked (2.5KB > 1KB chunk size)
    large_message = String.duplicate("This is a test message that will be chunked. ", 60)
    assert byte_size(large_message) > 1024

    {:ok, _msg_id} = Pulsar.send(producer, large_message)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == 1
    end)

    messages = @consumer_callback.get_messages(consumer)
    assert length(messages) == 1
    [received_msg] = messages
    assert received_msg.payload == large_message

    state = :sys.get_state(consumer)
    assert state.chunked_message_contexts == %{}
  end

  test "sets expire_time_of_incomplete_chunked_message" do
    {:ok, consumer_group} =
      Pulsar.start_consumer(
        @topic,
        "chunking-expiration",
        @consumer_callback,
        client: @client,
        initial_position: :latest,
        consumer_count: 1,
        expire_time_of_incomplete_chunked_message: 100
      )

    [consumer] = Pulsar.get_consumers(consumer_group)

    Utils.wait_for(fn ->
      state = :sys.get_state(consumer)
      state.callback_state != nil
    end)

    state = :sys.get_state(consumer)
    assert state.expire_time_of_incomplete_chunked_message == 100
  end

  test "respects max pending chunked messages limit" do
    {:ok, consumer_group} =
      Pulsar.start_consumer(
        @topic,
        "chunking-limit",
        @consumer_callback,
        client: @client,
        initial_position: :latest,
        consumer_count: 1,
        max_pending_chunked_messages: 5
      )

    [consumer] = Pulsar.get_consumers(consumer_group)

    Utils.wait_for(fn ->
      state = :sys.get_state(consumer)
      state.callback_state != nil
    end)

    state = :sys.get_state(consumer)
    assert state.max_pending_chunked_messages == 5
  end

  @tag telemetry_listen: [
         [:pulsar, :consumer, :chunk, :received],
         [:pulsar, :consumer, :chunk, :complete]
       ]
  test "handles interleaved chunks from multiple chunked messages" do
    {:ok, producer1} =
      Pulsar.start_producer(
        @topic,
        client: @client,
        name: :chunking_interleaved_producer1,
        chunking_enabled: true,
        max_message_size: 512
      )

    {:ok, producer2} =
      Pulsar.start_producer(
        @topic,
        client: @client,
        name: :chunking_interleaved_producer2,
        chunking_enabled: true,
        max_message_size: 512
      )

    {:ok, consumer_group} =
      Pulsar.start_consumer(
        @topic,
        "chunking-interleaved",
        @consumer_callback,
        client: @client,
        initial_position: :latest,
        consumer_count: 1,
        max_pending_chunked_messages: 5
      )

    [consumer] = Pulsar.get_consumers(consumer_group)

    Utils.wait_for(fn ->
      state = :sys.get_state(consumer)
      state.callback_state != nil
    end)

    message1 = String.duplicate("Message from producer 1. ", 50)
    message2 = String.duplicate("Message from producer 2. ", 50)

    task1 = Task.async(fn -> Pulsar.send(producer1, message1) end)
    task2 = Task.async(fn -> Pulsar.send(producer2, message2) end)

    Task.await(task1)
    Task.await(task2)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == 2
    end)

    messages = @consumer_callback.get_messages(consumer)
    assert length(messages) == 2
    payloads = messages |> Enum.map(& &1.payload) |> Enum.sort()
    assert message1 in payloads
    assert message2 in payloads
  end

  test "handles mix of chunked and non-chunked messages" do
    {:ok, producer} =
      Pulsar.start_producer(
        @topic,
        client: @client,
        name: :chunking_mixed_producer,
        chunking_enabled: true,
        max_message_size: 1024
      )

    {:ok, consumer_group} =
      Pulsar.start_consumer(
        @topic,
        "chunking-mixed",
        @consumer_callback,
        client: @client,
        initial_position: :latest,
        consumer_count: 1
      )

    [consumer] = Pulsar.get_consumers(consumer_group)

    Utils.wait_for(fn ->
      state = :sys.get_state(consumer)
      state.callback_state != nil
    end)

    small_message = "Small message"
    large_message = String.duplicate("Large message content. ", 100)

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

  describe "chunking configuration validation" do
    test "uses default chunking configuration" do
      {:ok, consumer_group} =
        Pulsar.start_consumer(
          @topic,
          "chunking-defaults",
          @consumer_callback,
          client: @client,
          initial_position: :latest,
          consumer_count: 1
        )

      [consumer] = Pulsar.get_consumers(consumer_group)

      Utils.wait_for(fn ->
        state = :sys.get_state(consumer)
        state.callback_state != nil
      end)

      state = :sys.get_state(consumer)
      assert state.max_pending_chunked_messages == 10
      assert state.expire_incomplete_chunked_message_after == 60_000
    end

    test "accepts custom chunking configuration" do
      {:ok, consumer_group} =
        Pulsar.start_consumer(
          @topic,
          "chunking-custom",
          @consumer_callback,
          client: @client,
          initial_position: :latest,
          consumer_count: 1,
          max_pending_chunked_messages: 20,
          expire_incomplete_chunked_message_after: 30_000
        )

      [consumer] = Pulsar.get_consumers(consumer_group)

      Utils.wait_for(fn ->
        state = :sys.get_state(consumer)
        state.callback_state != nil
      end)

      state = :sys.get_state(consumer)
      assert state.max_pending_chunked_messages == 20
      assert state.expire_incomplete_chunked_message_after == 30_000
    end
  end
end
