defmodule Pulsar.Integration.Consumer.ChunkingTest do
  use Pulsar.Test.Case, async: true

  alias Pulsar.Consumer.ChunkedMessageContext
  alias Pulsar.Protocol.Binary.Pulsar.Proto

  @topic "persistent://public/default/chunking-test"
  @consumer_callback Pulsar.Test.Support.DummyConsumer

  @tag telemetry_listen: [[:pulsar, :producer, :chunk, :start]]
  test "receives and reassembles a simple chunked message" do
    topic = isolated_topic("simple")
    large_message = "This is a test message that will be chunked."

    {:ok, producer} =
      Pulsar.Producer.start(
        topic,
        client: @client,
        name: :chunking_simple_producer,
        chunking_enabled: true,
        max_message_size: 32
      )

    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        topic,
        "chunking-simple",
        @consumer_callback,
        client: @client
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [consumer] = Topology.workers(consumer_group)

    assert byte_size(large_message) == 44

    {:ok, _msg_id} = Pulsar.Producer.send(producer, large_message)

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

    # Workers pick these up from their options by name, so a rename empties every event.
    assert_receive {:telemetry_event,
                    %{event: [:pulsar, :producer, :chunk, :start], metadata: %{topic: ^topic} = metadata}}

    assert metadata.base_topic == topic
    assert metadata.partition == nil
  end

  test "reassembles two chunked messages whose chunks arrive interleaved" do
    topic = isolated_topic("interleaved")
    p1_large_message = "This is a test message that will be chunked from producer 1."
    p2_large_message = "This is a test message that will be chunked from producer 2."

    {:ok, producer1} =
      Pulsar.Producer.start(
        topic,
        client: @client,
        name: :chunking_interleaved_producer1,
        chunking_enabled: true,
        max_message_size: 8
      )

    {:ok, producer2} =
      Pulsar.Producer.start(
        topic,
        client: @client,
        name: :chunking_interleaved_producer2,
        chunking_enabled: true,
        max_message_size: 8
      )

    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        topic,
        "chunking-interleaved",
        @consumer_callback,
        client: @client
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [consumer] = Topology.workers(consumer_group)

    task1 = Task.async(fn -> Pulsar.Producer.send(producer1, p1_large_message) end)
    task2 = Task.async(fn -> Pulsar.Producer.send(producer2, p2_large_message) end)

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

  @tag telemetry_listen: [[:pulsar, :producer, :message, :published]]
  test "delivers chunked and unchunked messages off the same producer" do
    topic = isolated_topic("mixed")
    small_message = "Small message"
    large_message = "This is a test message that will be chunked."

    {:ok, producer} =
      Pulsar.Producer.start(
        topic,
        client: @client,
        name: :chunking_mixed_producer,
        chunking_enabled: true,
        max_message_size: 32
      )

    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        topic,
        "chunking-mixed",
        @consumer_callback,
        client: @client
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [consumer] = Topology.workers(consumer_group)

    {:ok, _} = Pulsar.Producer.send(producer, small_message)
    {:ok, _} = Pulsar.Producer.send(producer, large_message)
    {:ok, _} = Pulsar.Producer.send(producer, small_message)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == 3
    end)

    messages = @consumer_callback.get_messages(consumer)
    assert length(messages) == 3

    payloads = Enum.map(messages, & &1.payload)
    assert Enum.count(payloads, &(&1 == small_message)) == 2
    assert large_message in payloads

    # An unchunked send groups by the same keys a chunked one does.
    assert_receive {:telemetry_event,
                    %{event: [:pulsar, :producer, :message, :published], metadata: %{topic: ^topic} = metadata}}

    assert metadata.base_topic == topic
    assert metadata.partition == nil
  end

  test "refuses an oversized message outright when chunking is off" do
    topic = isolated_topic("disabled")
    very_large_message = String.duplicate("x", 6_291_456)

    {:ok, producer} =
      Pulsar.Producer.start(
        topic,
        client: @client,
        name: :chunking_disabled_producer,
        chunking_enabled: false
      )

    assert byte_size(very_large_message) == 6_291_456

    Utils.wait_for(
      fn -> Pulsar.Producer.send(producer, very_large_message) end,
      until: &(&1 == {:error, :message_too_large}),
      description: "the producer to refuse the oversized message"
    )

    # Reaching the broker with this would have closed the connection.
    assert {:ok, _msg_id} = Pulsar.Producer.send(producer, "still connected")
  end

  test "splits a message past the broker's limit and reassembles it whole" do
    topic = isolated_topic("5mb")
    very_large_message = String.duplicate("x", 6_291_456)

    {:ok, producer} =
      Pulsar.Producer.start(
        topic,
        client: @client,
        name: :chunking_enabled_5mb_producer,
        chunking_enabled: true
      )

    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        topic,
        "chunking-5mb",
        @consumer_callback,
        client: @client
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [consumer] = Topology.workers(consumer_group)

    assert byte_size(very_large_message) == 6_291_456

    {:ok, chunked_msg_id} = Pulsar.Producer.send(producer, very_large_message)
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

  @chunk_size 1024

  for compression <- [:none, :lz4, :zlib, :snappy, :zstd] do
    test "reassembles a chunked message compressed with #{compression}" do
      compression = unquote(compression)
      topic = isolated_topic("#{compression}")

      # Half incompressible so it still needs several chunks once compressed, half trivially
      # compressible so every codec removes something.
      large_message = :crypto.strong_rand_bytes(4096) <> String.duplicate("a", 4096)

      {:ok, producer} =
        Pulsar.Producer.start(
          topic,
          client: @client,
          name: :"chunking_#{compression}_producer",
          chunking_enabled: true,
          compression: compression,
          max_message_size: @chunk_size
        )

      {:ok, consumer_group} =
        Pulsar.Consumer.start(
          topic,
          "chunking-#{compression}",
          @consumer_callback,
          client: @client
        )

      :ok = Pulsar.Consumer.await_ready(consumer_group)
      [consumer] = Topology.workers(consumer_group)

      {:ok, _msg_id} = Pulsar.Producer.send(producer, large_message)

      Utils.wait_for(fn ->
        @consumer_callback.count_messages(consumer) == 1
      end)

      [received_msg] = @consumer_callback.get_messages(consumer)
      assert received_msg.payload == large_message
      assert received_msg.chunk_metadata.complete == true
      assert received_msg.chunk_metadata.num_chunks > 1

      # Every chunk describes the message it belongs to, not the slice it carries.
      for metadata <- received_msg.raw.metadata do
        assert metadata.uncompressed_size == byte_size(large_message)
      end

      [%{total_chunk_msg_size: total} | _] = received_msg.raw.metadata
      assert Enum.all?(received_msg.raw.metadata, &(&1.total_chunk_msg_size == total))

      # An uncompressed total would not account for the chunks sent, for any codec that
      # removed a byte.
      assert received_msg.chunk_metadata.num_chunks == ceil(total / @chunk_size)

      if compression == :none do
        assert total == byte_size(large_message)
      else
        assert total < byte_size(large_message)
      end
    end
  end

  test "compresses before deciding whether to chunk" do
    topic = isolated_topic("compress-first")

    # Compresses to well under :max_message_size, so there is nothing left to split.
    # Splitting first would have sent 64 chunks.
    compressible_message = String.duplicate("a", 65_536)

    {:ok, producer} =
      Pulsar.Producer.start(
        topic,
        client: @client,
        name: :chunking_compress_first_producer,
        chunking_enabled: true,
        compression: :zlib,
        max_message_size: 1024
      )

    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        topic,
        "chunking-compress-first",
        @consumer_callback,
        client: @client
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [consumer] = Topology.workers(consumer_group)

    {:ok, _msg_id} = Pulsar.Producer.send(producer, compressible_message)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == 1
    end)

    [received_msg] = @consumer_callback.get_messages(consumer)
    assert received_msg.payload == compressible_message
    assert received_msg.chunk_metadata == nil
  end

  test "leaves room for the message metadata inside the broker's size limit" do
    topic = isolated_topic("metadata-overhead")

    # Properties ride along with every chunk, so a chunk sized to :max_message_size on its
    # own would put the frame over what the broker accepts.
    bulky_properties = %{"padding" => String.duplicate("p", 32_768)}
    very_large_message = String.duplicate("x", 6_291_456)

    {:ok, producer} =
      Pulsar.Producer.start(
        topic,
        client: @client,
        name: :chunking_metadata_overhead_producer,
        chunking_enabled: true
      )

    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        topic,
        "chunking-metadata-overhead",
        @consumer_callback,
        client: @client
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [consumer] = Topology.workers(consumer_group)

    {:ok, _msg_id} = Pulsar.Producer.send(producer, very_large_message, properties: bulky_properties)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == 1
    end)

    [received_msg] = @consumer_callback.get_messages(consumer)
    assert received_msg.payload == very_large_message
    assert Pulsar.Message.properties(received_msg) == bulky_properties
  end

  test "sizes a chunk so a full one still clears the broker's own limit" do
    # Chunks budgeted right up to :max_message_size, which defaults to the broker's limit.
    # The producer's deduction and the broker's check on the whole frame are separate
    # calculations, and this is where they have to agree.
    topic = isolated_topic("budget-boundary")

    # Just past the limit, so the first chunk fills the budget exactly and the second holds
    # the remainder.
    message = String.duplicate("x", 6_291_456)

    {:ok, producer} =
      Pulsar.Producer.start(
        topic,
        client: @client,
        name: :chunking_budget_boundary_producer,
        chunking_enabled: true
      )

    {:ok, consumer_group} =
      Pulsar.Consumer.start(topic, "chunking-budget-boundary", @consumer_callback, client: @client)

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [consumer] = Topology.workers(consumer_group)

    assert {:ok, _msg_id} = Pulsar.Producer.send(producer, message)

    Utils.wait_for(fn -> @consumer_callback.count_messages(consumer) == 1 end)

    [received_msg] = @consumer_callback.get_messages(consumer)
    assert received_msg.payload == message
  end

  test "refuses a message whose metadata alone exceeds the broker's limit" do
    # Has to fail in the producer: reaching the broker with this costs the whole connection.
    oversized_properties = %{"padding" => String.duplicate("p", 6_291_456)}

    {:ok, producer} =
      Pulsar.Producer.start(
        isolated_topic("oversized-metadata"),
        client: @client,
        name: :chunking_oversized_metadata_producer,
        chunking_enabled: true
      )

    Utils.wait_for(
      fn -> Pulsar.Producer.send(producer, "small payload", properties: oversized_properties) end,
      until: &(&1 == {:error, :metadata_too_large}),
      description: "the producer to refuse the oversized metadata"
    )
  end

  @tag telemetry_listen: [[:pulsar, :consumer, :chunk, :discarded]]
  test "evicts the message that has been waiting longest when the queue is full" do
    topic = isolated_topic("evict")

    {:ok, producer} =
      Pulsar.Producer.start(
        topic,
        client: @client,
        name: :chunking_evict_producer,
        chunking_enabled: true,
        max_message_size: 32
      )

    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        topic,
        "chunking-evict",
        @consumer_callback,
        client: @client,
        max_pending_chunked_messages: 2
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [consumer] = Topology.workers(consumer_group)

    # Two chunked messages already waiting, so the real one below arrives to a full queue.
    :sys.replace_state(consumer, fn state ->
      now = :erlang.monotonic_time(:millisecond)

      waiting = %{
        "fake-uuid-oldest" => %{incomplete("fake-uuid-oldest", 100) | created_at: now - 100},
        "fake-uuid-newer" => %{incomplete("fake-uuid-newer", 101) | created_at: now}
      }

      %{state | chunked_message_contexts: Map.merge(state.chunked_message_contexts, waiting)}
    end)

    assert map_size(:sys.get_state(consumer).chunked_message_contexts) == 2

    large_message = "This is a real message that will be chunked and trigger eviction"
    {:ok, _msg_id} = Pulsar.Producer.send(producer, large_message)

    assert_receive {:telemetry_event,
                    %{
                      event: [:pulsar, :consumer, :chunk, :discarded],
                      measurements: measurements,
                      metadata: %{uuid: "fake-uuid-oldest"} = metadata
                    }},
                   2000

    assert metadata.reason == :queue_full
    assert measurements.received_chunks == 1
    assert measurements.num_chunks == 3
    assert metadata.uuid == "fake-uuid-oldest"

    assert metadata.topic == topic
    assert metadata.base_topic == topic
    assert metadata.partition == nil
    assert metadata.subscription_name == "chunking-evict"

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

  # These tests run async against one broker, and a subscription starting at :latest would
  # otherwise pick up whatever the others are publishing. Naming a topic is enough while the
  # cluster has allowAutoTopicCreation on; without it this needs System.create_topic/1.
  defp isolated_topic(suffix), do: @topic <> "-" <> suffix

  # One chunk of three, so the message it belongs to is still waiting on the other two.
  defp incomplete(uuid, ledger) do
    command = %Proto.CommandMessage{message_id: %Proto.MessageIdData{ledgerId: ledger, entryId: 1}}

    metadata = %Proto.MessageMetadata{
      producer_name: uuid,
      sequence_id: 1,
      publish_time: :erlang.system_time(:millisecond),
      uuid: uuid,
      chunk_id: 0,
      num_chunks_from_msg: 3,
      total_chunk_msg_size: 100
    }

    {:ok, context} = ChunkedMessageContext.new(command, metadata, "chunk-0", nil)
    context
  end
end
