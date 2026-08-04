defmodule Pulsar.Integration.Consumer.ChunkingTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.Protocol.Binary.Pulsar.Proto
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :chunking_test_client
  @topic "persistent://public/default/chunking-test"
  @consumer_callback Pulsar.Test.Support.DummyConsumer

  setup [:telemetry_listen]

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
      Pulsar.Producer.start(
        @topic,
        client: @client,
        name: :chunking_simple_producer,
        chunking_enabled: true,
        max_message_size: 32
      )

    {:ok, _consumer_group} =
      Pulsar.Consumer.start(
        @topic,
        "chunking-simple",
        @consumer_callback,
        client: @client,
        init_args: [notify_pid: self()]
      )

    [consumer] = Utils.wait_for_consumer_ready(1)

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
  end

  test "handles interleaved chunks from multiple chunked messages" do
    p1_large_message = "This is a test message that will be chunked from producer 1."
    p2_large_message = "This is a test message that will be chunked from producer 2."

    {:ok, producer1} =
      Pulsar.Producer.start(
        @topic,
        client: @client,
        name: :chunking_interleaved_producer1,
        chunking_enabled: true,
        max_message_size: 8
      )

    {:ok, producer2} =
      Pulsar.Producer.start(
        @topic,
        client: @client,
        name: :chunking_interleaved_producer2,
        chunking_enabled: true,
        max_message_size: 8
      )

    {:ok, _consumer_group} =
      Pulsar.Consumer.start(
        @topic,
        "chunking-interleaved",
        @consumer_callback,
        client: @client,
        init_args: [notify_pid: self()]
      )

    [consumer] = Utils.wait_for_consumer_ready(1)

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

  test "handles mix of chunked and non-chunked messages" do
    small_message = "Small message"
    large_message = "This is a test message that will be chunked."

    {:ok, producer} =
      Pulsar.Producer.start(
        @topic,
        client: @client,
        name: :chunking_mixed_producer,
        chunking_enabled: true,
        max_message_size: 32
      )

    {:ok, _consumer_group} =
      Pulsar.Consumer.start(
        @topic,
        "chunking-mixed",
        @consumer_callback,
        client: @client,
        init_args: [notify_pid: self()]
      )

    [consumer] = Utils.wait_for_consumer_ready(1)

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
  end

  test "producer with chunking disabled cannot send 5MB messages" do
    very_large_message = String.duplicate("x", 6_291_456)

    {:ok, producer} =
      Pulsar.Producer.start(
        @topic,
        client: @client,
        name: :chunking_disabled_producer,
        chunking_enabled: false
      )

    assert byte_size(very_large_message) == 6_291_456

    assert {:error, _reason} = Pulsar.Producer.send(producer, very_large_message)
  end

  test "producer with chunking enabled can send and receive messages larger than 5MB" do
    very_large_message = String.duplicate("x", 6_291_456)

    {:ok, producer} =
      Pulsar.Producer.start(
        @topic,
        client: @client,
        name: :chunking_enabled_5mb_producer,
        chunking_enabled: true
      )

    {:ok, _consumer_group} =
      Pulsar.Consumer.start(
        @topic,
        "chunking-5mb",
        @consumer_callback,
        client: @client,
        init_args: [notify_pid: self()]
      )

    [consumer] = Utils.wait_for_consumer_ready(1)

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

  for compression <- [:none, :lz4, :zlib, :snappy, :zstd] do
    test "reassembles a chunked message compressed with #{compression}" do
      compression = unquote(compression)
      large_message = :crypto.strong_rand_bytes(8192)

      {:ok, producer} =
        Pulsar.Producer.start(
          @topic,
          client: @client,
          name: :"chunking_#{compression}_producer",
          chunking_enabled: true,
          compression: compression,
          max_message_size: 1024
        )

      {:ok, _consumer_group} =
        Pulsar.Consumer.start(
          @topic,
          "chunking-#{compression}",
          @consumer_callback,
          client: @client,
          init_args: [notify_pid: self()]
        )

      [consumer] = Utils.wait_for_consumer_ready(1)

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
        assert metadata.total_chunk_msg_size == hd(received_msg.raw.metadata).total_chunk_msg_size
      end
    end
  end

  test "compresses before deciding whether to chunk" do
    # Compresses to well under :max_message_size, so there is nothing left to split.
    # Splitting first would have sent 64 chunks.
    compressible_message = String.duplicate("a", 65_536)

    {:ok, producer} =
      Pulsar.Producer.start(
        @topic,
        client: @client,
        name: :chunking_compress_first_producer,
        chunking_enabled: true,
        compression: :zlib,
        max_message_size: 1024
      )

    {:ok, _consumer_group} =
      Pulsar.Consumer.start(
        @topic,
        "chunking-compress-first",
        @consumer_callback,
        client: @client,
        init_args: [notify_pid: self()]
      )

    [consumer] = Utils.wait_for_consumer_ready(1)

    {:ok, _msg_id} = Pulsar.Producer.send(producer, compressible_message)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == 1
    end)

    [received_msg] = @consumer_callback.get_messages(consumer)
    assert received_msg.payload == compressible_message
    assert received_msg.chunk_metadata == nil
  end

  test "leaves room for the message metadata inside the broker's size limit" do
    # Properties ride along with every chunk, so a chunk sized to :max_message_size on its
    # own would put the frame over what the broker accepts.
    bulky_properties = %{"padding" => String.duplicate("p", 32_768)}
    very_large_message = String.duplicate("x", 6_291_456)

    {:ok, producer} =
      Pulsar.Producer.start(
        @topic,
        client: @client,
        name: :chunking_metadata_overhead_producer,
        chunking_enabled: true
      )

    {:ok, _consumer_group} =
      Pulsar.Consumer.start(
        @topic,
        "chunking-metadata-overhead",
        @consumer_callback,
        client: @client,
        init_args: [notify_pid: self()]
      )

    [consumer] = Utils.wait_for_consumer_ready(1)

    {:ok, _msg_id} = Pulsar.Producer.send(producer, very_large_message, properties: bulky_properties)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == 1
    end)

    [received_msg] = @consumer_callback.get_messages(consumer)
    assert received_msg.payload == very_large_message
    assert Pulsar.Message.properties(received_msg) == bulky_properties
  end

  @tag telemetry_listen: [[:pulsar, :consumer, :chunk, :expired]]
  test "expired incomplete chunked messages are cleaned up and delivered" do
    alias Proto, as: Binary
    alias Pulsar.Consumer.ChunkedMessageContext

    {:ok, _consumer_group} =
      Pulsar.Consumer.start(
        @topic,
        "chunking-expire",
        @consumer_callback,
        client: @client,
        expire_incomplete_chunked_message_after: 100,
        chunk_cleanup_interval: 50,
        init_args: [notify_pid: self()]
      )

    [consumer] = Utils.wait_for_consumer_ready(1)

    :sys.replace_state(consumer, fn state ->
      old_timestamp = :erlang.monotonic_time(:millisecond) - 200

      fake_command = %Binary.CommandMessage{
        consumer_id: state.consumer_id,
        message_id: %Binary.MessageIdData{ledgerId: 1, entryId: 1}
      }

      fake_metadata = %Binary.MessageMetadata{
        producer_name: "test-producer",
        sequence_id: 1,
        publish_time: :erlang.system_time(:millisecond),
        uuid: "test-uuid-expired",
        chunk_id: 0,
        num_chunks_from_msg: 3,
        total_chunk_msg_size: 100
      }

      fake_ctx = %ChunkedMessageContext{
        uuid: "test-uuid-expired",
        chunks: %{0 => "chunk0", 1 => "chunk1"},
        chunk_message_ids: %{
          0 => fake_command.message_id,
          1 => %{fake_command.message_id | entryId: 2}
        },
        num_chunks_from_msg: 3,
        total_chunk_msg_size: 100,
        received_chunks: 2,
        first_chunk_message_id: fake_command.message_id,
        last_chunk_message_id: %{fake_command.message_id | entryId: 2},
        created_at: old_timestamp,
        commands: [fake_command, fake_command],
        metadatas: [fake_metadata, fake_metadata],
        broker_metadatas: [nil, nil]
      }

      %{state | chunked_message_contexts: Map.put(state.chunked_message_contexts, "test-uuid-expired", fake_ctx)}
    end)

    Process.sleep(200)

    assert_receive {:telemetry_event,
                    %{
                      event: [:pulsar, :consumer, :chunk, :expired],
                      measurements: measurements,
                      metadata: metadata
                    }}

    assert measurements.received_chunks == 2
    assert metadata.uuid == "test-uuid-expired"

    updated_state = :sys.get_state(consumer)
    refute Map.has_key?(updated_state.chunked_message_contexts, "test-uuid-expired")
  end

  @tag telemetry_listen: [[:pulsar, :consumer, :chunk, :discarded]]
  test "evicts oldest incomplete chunked message when queue is full" do
    alias Proto, as: Binary
    alias Pulsar.Consumer.ChunkedMessageContext

    {:ok, producer} =
      Pulsar.Producer.start(
        @topic,
        client: @client,
        name: :chunking_evict_producer,
        chunking_enabled: true,
        max_message_size: 32
      )

    {:ok, _consumer_group} =
      Pulsar.Consumer.start(
        @topic,
        "chunking-evict",
        @consumer_callback,
        client: @client,
        max_pending_chunked_messages: 2,
        init_args: [notify_pid: self()]
      )

    [consumer] = Utils.wait_for_consumer_ready(1)

    :sys.replace_state(consumer, fn state ->
      now = :erlang.monotonic_time(:millisecond)

      fake_command1 = %Binary.CommandMessage{
        consumer_id: state.consumer_id,
        message_id: %Binary.MessageIdData{ledgerId: 100, entryId: 1}
      }

      fake_metadata1 = %Binary.MessageMetadata{
        producer_name: "fake-producer-1",
        sequence_id: 1,
        publish_time: :erlang.system_time(:millisecond),
        uuid: "fake-uuid-oldest",
        chunk_id: 0,
        num_chunks_from_msg: 3,
        total_chunk_msg_size: 100
      }

      fake_ctx1 = %ChunkedMessageContext{
        uuid: "fake-uuid-oldest",
        chunks: %{0 => "fake-chunk0"},
        chunk_message_ids: %{0 => fake_command1.message_id},
        num_chunks_from_msg: 3,
        total_chunk_msg_size: 100,
        received_chunks: 1,
        first_chunk_message_id: fake_command1.message_id,
        last_chunk_message_id: fake_command1.message_id,
        created_at: now - 100,
        commands: [fake_command1],
        metadatas: [fake_metadata1],
        broker_metadatas: [nil]
      }

      fake_command2 = %Binary.CommandMessage{
        consumer_id: state.consumer_id,
        message_id: %Binary.MessageIdData{ledgerId: 101, entryId: 1}
      }

      fake_metadata2 = %Binary.MessageMetadata{
        producer_name: "fake-producer-2",
        sequence_id: 2,
        publish_time: :erlang.system_time(:millisecond),
        uuid: "fake-uuid-newer",
        chunk_id: 0,
        num_chunks_from_msg: 3,
        total_chunk_msg_size: 100
      }

      fake_ctx2 = %ChunkedMessageContext{
        uuid: "fake-uuid-newer",
        chunks: %{0 => "fake-chunk0"},
        chunk_message_ids: %{0 => fake_command2.message_id},
        num_chunks_from_msg: 3,
        total_chunk_msg_size: 100,
        received_chunks: 1,
        first_chunk_message_id: fake_command2.message_id,
        last_chunk_message_id: fake_command2.message_id,
        created_at: now,
        commands: [fake_command2],
        metadatas: [fake_metadata2],
        broker_metadatas: [nil]
      }

      %{
        state
        | chunked_message_contexts:
            Map.merge(state.chunked_message_contexts, %{
              "fake-uuid-oldest" => fake_ctx1,
              "fake-uuid-newer" => fake_ctx2
            })
      }
    end)

    state = :sys.get_state(consumer)
    assert map_size(state.chunked_message_contexts) == 2

    large_message = "This is a real message that will be chunked and trigger eviction"
    {:ok, _msg_id} = Pulsar.Producer.send(producer, large_message)

    assert_receive {:telemetry_event,
                    %{
                      event: [:pulsar, :consumer, :chunk, :discarded],
                      measurements: measurements,
                      metadata: metadata
                    }},
                   2000

    assert measurements.reason == :queue_full
    assert metadata.uuid == "fake-uuid-oldest"

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
end
