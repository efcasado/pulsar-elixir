defmodule Pulsar.Consumer.WorkerTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Pulsar.Consumer.Ack
  alias Pulsar.Consumer.Worker
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

  # An unreadable payload is reported at :warning, which every test here provokes on purpose.
  @moduletag :capture_log

  defmodule Callback do
    @moduledoc false
    use Pulsar.Consumer.Callback

    def init(:refuse, _context), do: {:error, :init_refused}
    def init(init_args, context), do: {:ok, {init_args, context}}

    def handle_message(_message, state), do: {:ok, state}
  end

  defmodule Reporting do
    @moduledoc false
    use Pulsar.Consumer.Callback

    def init(pid, _context), do: {:ok, pid}

    def handle_message(message, pid) do
      send(pid, {:handled, message})
      {:ok, pid}
    end

    def handle_invalid_message(message, pid) do
      send(pid, {:invalid, message})
      {:ok, pid}
    end
  end

  @topic "persistent://public/default/orders"
  @chunked "first second"

  def forward(_event, measurements, metadata, pid), do: send(pid, {:telemetry, measurements, metadata})

  defp worker_state do
    struct(Worker,
      topic: Pulsar.Topic.partition(@topic, 2),
      base_topic: @topic,
      partition: 2,
      subscription_name: "order-service",
      subscription_type: :shared,
      consumer_name: "orders-order-service-partition-2-1",
      callback_module: Callback
    )
  end

  # This path only casts to the broker, so the test process can stand in for one.
  defp reporting_state do
    struct(worker_state(),
      callback_module: Reporting,
      callback_state: self(),
      consumer_id: 1,
      broker_pid: self(),
      acks: Ack.new(),
      flow_policy: :auto,
      flow_outstanding_permits: 100,
      flow_threshold: 0,
      flow_refill: 50
    )
  end

  defp delivery(compression, payload, opts) do
    metadata = struct(Binary.MessageMetadata, [producer_name: "orders-producer", compression: compression] ++ opts)
    command = %Binary.CommandMessage{message_id: %Binary.MessageIdData{ledgerId: 1, entryId: 1}}

    {:broker_message, {command, metadata, payload, nil}}
  end

  defp chunk_delivery(chunk_id, payload, opts) do
    delivery(
      :ZLIB,
      payload,
      [uuid: "orders-producer-1", chunk_id: chunk_id, num_chunks_from_msg: 2] ++ opts
    )
  end

  describe "callback initialization" do
    test "hands the callback its resolved topic and subscription" do
      assert {:noreply, state} = Worker.handle_continue({:init_callback, :args}, worker_state())

      assert {:args, context} = state.callback_state

      assert context == %{
               topic: "persistent://public/default/orders-partition-2",
               base_topic: @topic,
               partition: 2,
               subscription_name: "order-service",
               subscription_type: :shared,
               consumer_name: "orders-order-service-partition-2-1"
             }
    end

    test "marks the consumer ready only once the callback has initialized" do
      refute worker_state().ready

      assert {:noreply, state} = Worker.handle_continue({:init_callback, :args}, worker_state())
      assert state.ready
    end

    test "stops without a state when the callback refuses to initialize" do
      assert {:stop, :init_refused, nil} = Worker.handle_continue({:init_callback, :refuse}, worker_state())
    end
  end

  test "reports protocol-invalid messages from the worker" do
    handler = {__MODULE__, :protocol_invalid, self()}
    event = [:pulsar, :consumer, :message, :invalid]
    :ok = :telemetry.attach(handler, event, &__MODULE__.forward/4, self())
    on_exit(fn -> :telemetry.detach(handler) end)

    command = %Binary.CommandMessage{message_id: %Binary.MessageIdData{ledgerId: 1, entryId: 2}}
    invalid_delivery = {:broker_message, {:invalid, command, <<"invalid">>, :checksum_mismatch}}

    assert {:noreply, _state} = Worker.handle_info(invalid_delivery, reporting_state())

    assert_received {:telemetry, %{count: 1}, metadata}
    assert metadata.reason == :checksum_mismatch
    assert metadata.decompression_reason == nil
    assert metadata.topic == "persistent://public/default/orders-partition-2"

    assert_received {:invalid, invalid}
    assert invalid.validation_error == :checksum_mismatch
  end

  describe "a payload that cannot be decompressed" do
    for compression <- [:ZLIB, :LZ4, :ZSTD, :SNAPPY] do
      test "reaches handle_invalid_message/2 under #{compression}" do
        delivery = delivery(unquote(compression), <<"not a compressed frame">>, uncompressed_size: 64)

        assert {:noreply, _state} = Worker.handle_info(delivery, reporting_state())

        assert_received {:invalid, invalid}
        refute_received {:handled, _message}

        assert invalid.validation_error == :decompression_failed
      end
    end

    # CompressionType has no sixth value; this is the worker surviving one anyway.
    test "reaches it under a codec this client does not know" do
      delivery = delivery(9, <<"compressed by something else">>, uncompressed_size: 64)

      assert {:noreply, _state} = Worker.handle_info(delivery, reporting_state())

      assert_received {:invalid, invalid}
      assert invalid.validation_error == :decompression_failed
    end

    test "reaches it when the payload decompresses to the wrong size" do
      delivery = delivery(:ZLIB, :zlib.compress(<<"first ">>) <> :zlib.compress(<<"second">>), uncompressed_size: 12)
      assert {:noreply, _state} = Worker.handle_info(delivery, reporting_state())

      assert_received {:invalid, invalid}
      assert invalid.validation_error == :uncompressed_size_corruption
    end

    test "keeps the compressed bytes and the metadata it arrived with" do
      delivery = delivery(:ZSTD, <<"garbage">>, uncompressed_size: 64)

      Worker.handle_info(delivery, reporting_state())

      assert_received {:invalid, invalid}

      assert invalid.payload == <<"garbage">>
      assert Pulsar.Message.producer_name(invalid) == "orders-producer"
    end

    test "acknowledges it as the protocol's DecompressionError" do
      delivery = delivery(:ZSTD, <<"garbage">>, uncompressed_size: 64)

      Worker.handle_info(delivery, reporting_state())

      assert_received {:"$gen_cast", {:send_command, %Binary.CommandAck{} = ack}}
      assert ack.validation_error == :DecompressionError
    end

    test "acknowledges an output-size mismatch as UncompressedSizeCorruption" do
      delivery = delivery(:ZLIB, :zlib.compress(<<"first ">>) <> :zlib.compress(<<"second">>), uncompressed_size: 12)

      Worker.handle_info(delivery, reporting_state())

      assert_received {:"$gen_cast", {:send_command, %Binary.CommandAck{} = ack}}
      assert ack.validation_error == :UncompressedSizeCorruption
    end

    test "credits every message of an undecompressable batch to the flow window" do
      delivery = delivery(:ZSTD, <<"garbage">>, uncompressed_size: 64, num_messages_in_batch: 5)

      {:noreply, state} = Worker.handle_info(delivery, reporting_state())

      assert state.flow_outstanding_permits == 95

      assert_received {:invalid, invalid}
      assert Pulsar.Message.num_broker_messages(invalid) == 5
    end

    test "reports the codec's reason in telemetry" do
      handler = {__MODULE__, :size_mismatch, self()}
      event = [:pulsar, :consumer, :message, :invalid]
      :ok = :telemetry.attach(handler, event, &__MODULE__.forward/4, self())
      on_exit(fn -> :telemetry.detach(handler) end)
      delivery = delivery(:ZLIB, :zlib.compress(<<"first ">>) <> :zlib.compress(<<"second">>), uncompressed_size: 12)
      Worker.handle_info(delivery, reporting_state())

      assert_received {:telemetry, %{count: 1}, metadata}
      assert metadata.reason == :uncompressed_size_corruption
      assert metadata.decompression_reason == :uncompressed_size_mismatch
    end

    test "reports decompression failures through the invalid-message event" do
      handler = {__MODULE__, :invalid_message, self()}
      event = [:pulsar, :consumer, :message, :invalid]
      :ok = :telemetry.attach(handler, event, &__MODULE__.forward/4, self())
      on_exit(fn -> :telemetry.detach(handler) end)

      Worker.handle_info(delivery(:ZSTD, <<"garbage">>, uncompressed_size: 64), reporting_state())

      assert_received {:telemetry, %{count: 1}, metadata}
      assert metadata.reason == :decompression_failed
      assert metadata.decompression_reason == :decompression_error
      assert metadata.topic == "persistent://public/default/orders-partition-2"
    end

    test "decodes an empty zstd payload" do
      delivery = delivery(:ZSTD, :ezstd.compress(<<>>), uncompressed_size: 0)

      assert {:noreply, _state} = Worker.handle_info(delivery, reporting_state())

      assert_received {:handled, message}
      assert message.payload == <<>>
      assert Pulsar.Message.valid?(message)
    end

    test "a payload that does decompress is delivered as usual" do
      delivery = delivery(:ZSTD, :ezstd.compress(<<"the real payload">>), uncompressed_size: 16)

      assert {:noreply, _state} = Worker.handle_info(delivery, reporting_state())

      assert_received {:handled, message}
      refute_received {:invalid, _message}

      assert message.payload == "the real payload"
      assert Pulsar.Message.valid?(message)
    end
  end

  describe "chunks that were compressed one by one" do
    # A pre-3.0 producer split the payload and then compressed each chunk, recording that
    # chunk's own uncompressed size and the message's uncompressed total. zlib reads the
    # reassembled streams as the first of them, which is the size the chunk recorded.
    setup do
      chunks =
        for plain <- [<<"first ">>, <<"second">>] do
          {:zlib.compress(plain), uncompressed_size: byte_size(plain), total_chunk_msg_size: byte_size(@chunked)}
        end

      {:ok, chunks: chunks, assembled: Enum.map_join(chunks, fn {payload, _opts} -> payload end)}
    end

    test "reach handle_invalid_message/2 assembled and still compressed", context do
      {:noreply, _state} = deliver_chunks(context.chunks)

      assert_received {:invalid, invalid}
      refute_received {:handled, _message}

      assert invalid.validation_error == :uncompressed_size_corruption
      assert invalid.payload == context.assembled
      assert Pulsar.Message.complete?(invalid)
    end

    test "credit one permit per chunk to the flow window", context do
      {:noreply, state} = deliver_chunks(context.chunks)

      assert state.flow_outstanding_permits == 98

      assert_received {:invalid, invalid}
      assert Pulsar.Message.num_broker_messages(invalid) == 2
    end

    test "are delivered as usual when the producer compressed the whole message" do
      compressed = :zlib.compress(@chunked)
      half = div(byte_size(compressed), 2)
      <<first::binary-size(^half), second::binary>> = compressed

      # 3.0 compresses before splitting, so every chunk repeats the whole message's
      # uncompressed size and the compressed total.
      opts = [uncompressed_size: byte_size(@chunked), total_chunk_msg_size: byte_size(compressed)]

      {:noreply, _state} = deliver_chunks([{first, opts}, {second, opts}])

      assert_received {:handled, message}
      refute_received {:invalid, _message}

      assert message.payload == @chunked
    end

    test "rejects independently compressed chunks even when their sizes collide" do
      chunk = :binary.copy(<<"a">>, 11)
      compressed = :zlib.compress(chunk)
      opts = [uncompressed_size: byte_size(chunk), total_chunk_msg_size: byte_size(chunk) * 2]

      {:noreply, _state} = deliver_chunks([{compressed, opts}, {compressed, opts}])

      assert_received {:invalid, invalid}
      refute_received {:handled, _message}
      assert invalid.validation_error == :decompression_failed
    end

    test "uses chunk zero metadata when chunks arrive out of order" do
      compressed = :zlib.compress(@chunked)
      half = div(byte_size(compressed), 2)
      <<first::binary-size(^half), second::binary>> = compressed
      canonical = [uncompressed_size: byte_size(@chunked), total_chunk_msg_size: byte_size(compressed)]
      inconsistent = [uncompressed_size: 1, total_chunk_msg_size: byte_size(compressed)]
      state = struct(reporting_state(), max_pending_chunked_messages: 10)

      {:noreply, state} = Worker.handle_info(chunk_delivery(1, second, inconsistent), state)
      {:noreply, _state} = Worker.handle_info(chunk_delivery(0, first, canonical), state)

      assert_received {:handled, message}
      assert message.payload == @chunked
    end
  end

  defp deliver_chunks(chunks) do
    state = struct(reporting_state(), max_pending_chunked_messages: 10)

    chunks
    |> Enum.with_index()
    |> Enum.reduce({:noreply, state}, fn {{payload, opts}, chunk_id}, {:noreply, acc} ->
      Worker.handle_info(chunk_delivery(chunk_id, payload, opts), acc)
    end)
  end
end
