defmodule Pulsar.ProtocolTest do
  use ExUnit.Case, async: true

  alias Pulsar.Protocol
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.Protocol.Binary.Pulsar.Proto.BaseCommand

  @magic_message 0x0E01
  @magic_broker_entry 0x0E02

  describe "latest_version/0" do
    test "returns the highest protocol version declared in the schema" do
      versions =
        %Binary.ProtocolVersion{}
        |> Map.keys()
        |> Enum.flat_map(fn key ->
          case Atom.to_string(key) do
            "v" <> version -> [String.to_integer(version)]
            _other -> []
          end
        end)

      assert Protocol.latest_version() == Enum.max(versions)
    end

    test "is an integer suitable for CommandConnect.protocol_version" do
      assert is_integer(Protocol.latest_version())
      assert Protocol.latest_version() > 0
    end
  end

  describe "encode/1 framing" do
    test "frames a command as total_size, command_size and the command itself" do
      frame = Protocol.encode(%Binary.CommandPing{})

      assert <<total_size::32, command_size::32, command::bytes-size(command_size)>> = frame
      assert total_size == command_size + 4
      assert byte_size(frame) == total_size + 4
      assert %BaseCommand{type: :PING} = BaseCommand.decode(command)
    end

    test "total_size never counts its own 4 bytes" do
      for type <- schema_command_types() do
        {_field, module} = schema_oneof_for(type)
        frame = Protocol.encode(sample_command(module))
        <<total_size::32, _rest::binary>> = frame

        assert byte_size(frame) - 4 == total_size,
               "total_size must describe everything after the size prefix, for #{inspect(module)}"
      end
    end

    test "sets the BaseCommand type and populates the matching oneof field" do
      for type <- schema_command_types() do
        {field, module} = schema_oneof_for(type)
        command = sample_command(module)

        <<_total_size::32, command_size::32, encoded::bytes-size(command_size)>> = Protocol.encode(command)

        base_command = BaseCommand.decode(encoded)

        assert base_command.type == type
        assert Map.fetch!(base_command, field) == command
      end
    end
  end

  describe "decode/1 of command-only frames" do
    test "round-trips every command type declared in the schema" do
      types = schema_command_types()

      # Guards against the reflection silently yielding nothing, which would
      # make the loop below vacuously true.
      refute Enum.empty?(types)

      for type <- types do
        {_field, module} = schema_oneof_for(type)
        command = sample_command(module)

        assert Protocol.decode(Protocol.encode(command)) == {:ok, command},
               "expected #{type} to round-trip as a #{inspect(module)}"
      end
    end

    test "decodes a broker-sent command frame" do
      success = %Binary.CommandSuccess{request_id: 42}
      frame = command_frame(:SUCCESS, :success, success)

      assert Protocol.decode(frame) == {:ok, success}
    end
  end

  describe "encode_message/3" do
    setup do
      %{
        command: %Binary.CommandSend{producer_id: 7, sequence_id: 3},
        metadata: %Binary.MessageMetadata{producer_name: "p", sequence_id: 3, publish_time: 1_700_000_000_000},
        payload: "hello world"
      }
    end

    test "lays out command, magic, checksum, metadata and payload", ctx do
      frame = Protocol.encode_message(ctx.command, ctx.metadata, ctx.payload)

      assert <<total_size::32, command_size::32, command::bytes-size(command_size), @magic_message::16, _checksum::32,
               metadata_size::32, metadata::bytes-size(metadata_size), payload::binary>> = frame

      assert byte_size(frame) == total_size + 4
      assert %BaseCommand{type: :SEND} = BaseCommand.decode(command)
      assert Binary.MessageMetadata.decode(metadata) == ctx.metadata
      assert payload == ctx.payload
    end

    test "checksum covers the metadata size, metadata and payload", ctx do
      frame = Protocol.encode_message(ctx.command, ctx.metadata, ctx.payload)

      <<_total_size::32, command_size::32, _command::bytes-size(command_size), @magic_message::16, checksum::32,
        checksummed::binary>> = frame

      assert checksum == :crc32cer.nif(checksummed)
    end

    test "round-trips through decode/1", ctx do
      frame = Protocol.encode_message(ctx.command, ctx.metadata, ctx.payload)

      assert {:ok, {command, metadata, payload, nil}} = Protocol.decode(frame)
      assert command == ctx.command
      assert metadata == ctx.metadata
      assert payload == ctx.payload
    end

    test "handles an empty payload", ctx do
      frame = Protocol.encode_message(ctx.command, ctx.metadata, "")

      assert {:ok, {_command, _metadata, "", nil}} = Protocol.decode(frame)
    end

    test "handles a payload large enough to need multi-byte varints", ctx do
      payload = :binary.copy("x", 100_000)
      frame = Protocol.encode_message(ctx.command, ctx.metadata, payload)

      assert {:ok, {_command, _metadata, ^payload, nil}} = Protocol.decode(frame)
    end
  end

  describe "decode_stream/2" do
    setup do
      %{ping: Protocol.encode(%Binary.CommandPing{}), pong: Protocol.encode(%Binary.CommandPong{})}
    end

    test "returns nothing for an empty buffer" do
      assert Protocol.decode_stream(<<>>) == {:ok, [], <<>>}
    end

    test "buffers a fragment shorter than the length prefix", ctx do
      fragment = binary_part(ctx.ping, 0, 3)

      assert Protocol.decode_stream(fragment) == {:ok, [], fragment}
    end

    test "buffers a complete length prefix with no body", ctx do
      header = binary_part(ctx.ping, 0, 4)

      assert Protocol.decode_stream(header) == {:ok, [], header}
    end

    test "decodes a single complete frame", ctx do
      assert {:ok, [%Binary.CommandPing{}], <<>>} = Protocol.decode_stream(ctx.ping)
    end

    test "decodes several frames from one buffer in order", ctx do
      buffer = ctx.ping <> ctx.pong <> ctx.ping

      assert {:ok, commands, <<>>} = Protocol.decode_stream(buffer)
      assert [%Binary.CommandPing{}, %Binary.CommandPong{}, %Binary.CommandPing{}] = commands
    end

    test "returns the trailing bytes of an incomplete frame", ctx do
      partial = binary_part(ctx.pong, 0, 5)

      assert {:ok, [%Binary.CommandPing{}], ^partial} = Protocol.decode_stream(ctx.ping <> partial)
    end

    test "reassembles a frame delivered one byte at a time", ctx do
      chunks = for <<byte <- ctx.ping>>, do: <<byte>>

      assert {:ok, [%Binary.CommandPing{}], <<>>} = feed(chunks)
    end

    test "reassembles a frame split at every possible offset", ctx do
      for offset <- 0..byte_size(ctx.ping) do
        head = binary_part(ctx.ping, 0, offset)
        tail = binary_part(ctx.ping, offset, byte_size(ctx.ping) - offset)

        assert {:ok, [%Binary.CommandPing{}], <<>>} = feed([head, tail]),
               "frame split at offset #{offset} did not reassemble"
      end
    end

    test "keeps frame order when a chunk boundary falls mid-frame", ctx do
      buffer = ctx.ping <> ctx.pong
      split = byte_size(ctx.ping) + 3

      chunks = [binary_part(buffer, 0, split), binary_part(buffer, split, byte_size(buffer) - split)]

      assert {:ok, commands, <<>>} = feed(chunks)
      assert [%Binary.CommandPing{}, %Binary.CommandPong{}] = commands
    end

    test "halts at a corrupt frame and discards the frames behind it", ctx do
      command = %Binary.CommandMessage{consumer_id: 1, message_id: %Binary.MessageIdData{ledgerId: 5, entryId: 6}}
      metadata = %Binary.MessageMetadata{producer_name: "p", sequence_id: 1, publish_time: 1}

      frame = message_frame(command, metadata, "payload")
      corrupted = binary_part(frame, 0, byte_size(frame) - 7) <> "corrupt"

      assert Protocol.decode_stream(ctx.ping <> corrupted <> ctx.pong) == {:error, :checksum_mismatch}
    end

    test "halts when a frame does not match the framing at all", ctx do
      garbage = <<0::32, 0::32>>

      assert Protocol.decode_stream(ctx.ping <> garbage) == {:error, :malformed_frame}
    end

    test "rejects an oversized frame on its length prefix alone" do
      max = Pulsar.Config.max_frame_size()
      oversized = <<max - 3::32>>

      assert byte_size(oversized) == 4
      assert Protocol.decode_stream(oversized) == {:error, {:frame_too_large, max + 1}}
    end

    test "accepts a length prefix at exactly the maximum frame size" do
      prefix = <<Pulsar.Config.max_frame_size() - 4::32>>

      assert Protocol.decode_stream(prefix) == {:ok, [], prefix}
    end

    test "rejects a length prefix claiming the largest value the field can hold" do
      assert Protocol.decode_stream(<<0xFFFFFFFF::32, 0::32>>) ==
               {:error, {:frame_too_large, 0xFFFFFFFF + 4}}
    end

    test "rejects an oversized frame before the frames behind it are decoded", ctx do
      max = Pulsar.Config.max_frame_size()

      assert Protocol.decode_stream(ctx.ping <> <<max::32>>) == {:error, {:frame_too_large, max + 4}}
    end

    test "takes the limit from the caller, not from global config", ctx do
      assert Protocol.decode_stream(ctx.ping, byte_size(ctx.ping)) == {:ok, [%Binary.CommandPing{}], <<>>}

      assert Protocol.decode_stream(ctx.ping, byte_size(ctx.ping) - 1) ==
               {:error, {:frame_too_large, byte_size(ctx.ping)}}
    end

    test "reassembles a large message frame split across many chunks" do
      command = %Binary.CommandMessage{consumer_id: 1, message_id: %Binary.MessageIdData{ledgerId: 5, entryId: 6}}
      metadata = %Binary.MessageMetadata{producer_name: "p", sequence_id: 1, publish_time: 1}
      payload = :binary.copy("x", 100_000)

      frame = message_frame(command, metadata, payload)
      chunks = chunk_every(frame, 1_400)

      assert {:ok, [{^command, _metadata, ^payload, nil}], <<>>} = feed(chunks)
    end
  end

  describe "decode/1 of message frames" do
    test "decodes a MESSAGE frame without broker entry metadata" do
      command = %Binary.CommandMessage{consumer_id: 1, message_id: %Binary.MessageIdData{ledgerId: 5, entryId: 6}}
      metadata = %Binary.MessageMetadata{producer_name: "p", sequence_id: 1, publish_time: 1}

      frame = message_frame(command, metadata, "payload")

      assert {:ok, {^command, decoded_metadata, "payload", nil}} = Protocol.decode(frame)
      assert decoded_metadata == metadata
    end

    test "decodes a MESSAGE frame with broker entry metadata" do
      command = %Binary.CommandMessage{consumer_id: 1, message_id: %Binary.MessageIdData{ledgerId: 5, entryId: 6}}
      metadata = %Binary.MessageMetadata{producer_name: "p", sequence_id: 1, publish_time: 1}
      broker_entry = %Binary.BrokerEntryMetadata{broker_timestamp: 1_700_000_000_000, index: 99}

      frame = message_frame(command, metadata, "payload", broker_entry_metadata: broker_entry)

      assert {:ok, {^command, decoded_metadata, "payload", decoded_broker_entry}} = Protocol.decode(frame)
      assert decoded_metadata == metadata
      assert decoded_broker_entry == broker_entry
    end

    test "decodes every combination of the two optional sections" do
      command = %Binary.CommandMessage{consumer_id: 1, message_id: %Binary.MessageIdData{ledgerId: 5, entryId: 6}}
      metadata = %Binary.MessageMetadata{producer_name: "p", sequence_id: 1, publish_time: 1}
      broker_entry = %Binary.BrokerEntryMetadata{broker_timestamp: 1_700_000_000_000, index: 99}

      combinations = [
        {"checksum, broker entry metadata", [checksum: true, broker_entry_metadata: broker_entry], broker_entry},
        {"checksum, no broker entry metadata", [checksum: true], nil},
        {"no checksum, broker entry metadata", [checksum: false, broker_entry_metadata: broker_entry], broker_entry},
        {"no checksum, no broker entry metadata", [checksum: false], nil}
      ]

      for {label, opts, expected_broker_entry} <- combinations do
        frame = message_frame(command, metadata, "payload", opts)

        assert {:ok, {^command, decoded_metadata, "payload", decoded_broker_entry}} = Protocol.decode(frame),
               "failed to decode a frame with #{label}"

        assert decoded_metadata == metadata
        assert decoded_broker_entry == expected_broker_entry
      end
    end

    test "treats a payload containing the message magic as opaque bytes" do
      command = %Binary.CommandMessage{consumer_id: 1, message_id: %Binary.MessageIdData{ledgerId: 5, entryId: 6}}
      metadata = %Binary.MessageMetadata{producer_name: "p", sequence_id: 1, publish_time: 1}
      payload = <<@magic_message::16, 0, 0, 0, 0, "not a frame">>

      frame = message_frame(command, metadata, payload)

      assert {:ok, {_command, _metadata, ^payload, nil}} = Protocol.decode(frame)
    end
  end

  describe "decode/1 on damaged input" do
    setup do
      command = %Binary.CommandSend{producer_id: 1, sequence_id: 1}
      metadata = %Binary.MessageMetadata{producer_name: "p", sequence_id: 1, publish_time: 1}

      %{frame: Protocol.encode_message(command, metadata, "aaaa")}
    end

    test "rejects a frame whose payload does not match its checksum", ctx do
      corrupted = binary_part(ctx.frame, 0, byte_size(ctx.frame) - 4) <> "bbbb"

      assert Protocol.decode(corrupted) == {:error, :checksum_mismatch}
    end

    test "rejects a frame whose metadata does not match its checksum", ctx do
      <<_total_size::32, command_size::32, _rest::binary>> = ctx.frame
      offset = 18 + command_size

      <<head::bytes-size(^offset), byte, tail::binary>> = ctx.frame
      corrupted = <<head::binary, Bitwise.bxor(byte, 0xFF), tail::binary>>

      assert Protocol.decode(corrupted) == {:error, :checksum_mismatch}
    end

    test "accepts a frame that is bit-for-bit intact", ctx do
      assert {:ok, {_command, _metadata, "aaaa", nil}} = Protocol.decode(ctx.frame)
    end

    test "rejects a truncated frame" do
      <<partial::bytes-size(6), _rest::binary>> = Protocol.encode(%Binary.CommandPing{})

      assert Protocol.decode(partial) == {:error, :malformed_frame}
    end

    test "rejects an empty binary" do
      assert Protocol.decode(<<>>) == {:error, :malformed_frame}
    end

    test "rejects a frame claiming a metadata size larger than its payload", ctx do
      <<total_size::32, command_size::32, command::bytes-size(command_size), @magic_message::16, _checksum::32,
        _metadata_size::32, rest::binary>> = ctx.frame

      checksummed = <<0xFFFF::32, rest::binary>>

      corrupted =
        <<total_size::32, command_size::32, command::binary, @magic_message::16, :crc32cer.nif(checksummed)::32,
          checksummed::binary>>

      assert Protocol.decode(corrupted) == {:error, :malformed_frame}
    end

    test "rejects a command type the vendored schema does not know" do
      unknown_type = 99
      command = <<8, unknown_type>>

      assert Protocol.decode(command_only_frame(command)) == {:error, {:unsupported_command_type, unknown_type}}
    end

    test "rejects a frame whose command is missing its required type" do
      assert Protocol.decode(command_only_frame(<<>>)) == {:error, {:unsupported_command_type, nil}}
    end

    test "rejects a frame whose command bytes protobuf cannot read" do
      assert Protocol.decode(command_only_frame(<<255, 255, 255, 255>>)) == {:error, :malformed_command}
    end

    test "does not leak unexpected protobuf decoder exceptions" do
      assert Protocol.decode(command_only_frame(<<0xF5, 0x0E>>)) == {:error, :malformed_command}
    end

    test "rejects a message frame whose metadata bytes protobuf cannot read" do
      command =
        BaseCommand.encode(%BaseCommand{
          type: :MESSAGE,
          message: %Binary.CommandMessage{consumer_id: 1, message_id: %Binary.MessageIdData{ledgerId: 1, entryId: 1}}
        })

      frame = raw_message_frame(command, <<255, 255, 255, 255>>, "payload")

      assert Protocol.decode(frame) == {:error, :malformed_message_metadata}
    end

    test "reports a bad checksum before touching the broker entry metadata" do
      command = %Binary.CommandMessage{consumer_id: 1, message_id: %Binary.MessageIdData{ledgerId: 5, entryId: 6}}
      metadata = %Binary.MessageMetadata{producer_name: "p", sequence_id: 1, publish_time: 1}
      broker_entry = %Binary.BrokerEntryMetadata{broker_timestamp: 1, index: 1}

      frame = message_frame(command, metadata, "payload", broker_entry_metadata: broker_entry)

      corrupted =
        frame
        |> binary_part(0, byte_size(frame) - 4)
        |> Kernel.<>("bbbb")
        |> corrupt_broker_entry_metadata()

      assert Protocol.decode(corrupted) == {:error, :checksum_mismatch}
    end

    test "never raises, whatever the bytes are", ctx do
      frames =
        [<<>>, <<0>>, <<0::32>>, <<4::32, 0::32>>, <<0xFFFFFFFF::32, 0::32>>, ctx.frame] ++
          truncations(ctx.frame) ++ bit_flips(ctx.frame)

      for frame <- frames do
        result =
          try do
            Protocol.decode(frame)
          rescue
            error -> {:raised, error}
          catch
            kind, value -> {:caught, kind, value}
          end

        assert match?({:ok, _}, result) or match?({:error, _}, result),
               "decode/1 must not raise, got #{inspect(result)} for #{inspect(frame, limit: 8)}"
      end
    end
  end

  describe "to_key_value_list/1" do
    test "returns an empty list for nil" do
      assert Protocol.to_key_value_list(nil) == []
    end

    test "returns an empty list for an empty map" do
      assert Protocol.to_key_value_list(%{}) == []
    end

    test "converts a map into KeyValue structs" do
      assert Protocol.to_key_value_list(%{"trace_id" => "abc"}) == [%Binary.KeyValue{key: "trace_id", value: "abc"}]
    end

    test "stringifies atom keys and non-binary values" do
      result = Protocol.to_key_value_list(%{:retries => 3, "flag" => true})

      assert Enum.sort_by(result, & &1.key) == [
               %Binary.KeyValue{key: "flag", value: "true"},
               %Binary.KeyValue{key: "retries", value: "3"}
             ]
    end
  end

  ## Helpers

  defp command_frame(type, field, command) do
    %BaseCommand{type: type}
    |> Map.put(field, command)
    |> frame_base_command()
  end

  defp feed(chunks) do
    Enum.reduce_while(chunks, {:ok, [], <<>>}, fn chunk, {:ok, commands, buffer} ->
      case Protocol.decode_stream(buffer <> chunk) do
        {:ok, new_commands, rest} -> {:cont, {:ok, commands ++ new_commands, rest}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp chunk_every(binary, size) when byte_size(binary) <= size, do: [binary]

  defp chunk_every(binary, size) do
    <<head::bytes-size(^size), rest::binary>> = binary

    [head | chunk_every(rest, size)]
  end

  defp frame_base_command(base_command) do
    base_command |> BaseCommand.encode() |> command_only_frame()
  end

  defp command_only_frame(command) do
    size = byte_size(command)

    <<size + 4::32, size::32, command::binary>>
  end

  defp raw_message_frame(command, metadata, payload) do
    command_size = byte_size(command)
    checksummed = <<byte_size(metadata)::32, metadata::binary, payload::binary>>
    message_part = <<@magic_message::16, :crc32cer.nif(checksummed)::32, checksummed::binary>>

    <<4 + command_size + byte_size(message_part)::32, command_size::32, command::binary, message_part::binary>>
  end

  defp corrupt_broker_entry_metadata(
         <<total_size::32, command_size::32, command::bytes-size(command_size), @magic_broker_entry::16, size::32,
           _broker_metadata::bytes-size(size), rest::binary>>
       ) do
    garbage = :binary.copy(<<255>>, size)

    <<total_size::32, command_size::32, command::binary, @magic_broker_entry::16, size::32, garbage::binary,
      rest::binary>>
  end

  defp truncations(frame) do
    for length <- 0..(byte_size(frame) - 1), do: binary_part(frame, 0, length)
  end

  defp bit_flips(frame) do
    for offset <- 0..(byte_size(frame) - 1) do
      <<head::bytes-size(^offset), byte, tail::binary>> = frame

      <<head::binary, Bitwise.bxor(byte, 0xFF), tail::binary>>
    end
  end

  defp schema_command_types do
    BaseCommand.Type.__message_props__().field_props
    |> Map.values()
    |> Enum.map(& &1.name_atom)
  end

  defp schema_oneof_for(type) do
    tag = BaseCommand.Type.value(type)
    props = Map.fetch!(BaseCommand.__message_props__().field_props, tag)

    {props.name_atom, props.type}
  end

  defp sample_command(module) do
    fields =
      for {_tag, props} <- module.__message_props__().field_props,
          props.required?,
          into: %{},
          do: {props.name_atom, sample_value(props)}

    struct!(module, fields)
  end

  defp sample_value(%{enum?: true, type: {:enum, enum}}) do
    enum.__message_props__().field_props |> Map.values() |> List.first() |> Map.fetch!(:name_atom)
  end

  defp sample_value(%{embedded?: true, type: module}), do: sample_command(module)
  defp sample_value(%{type: type}) when type in [:string, :bytes], do: "x"
  defp sample_value(%{type: :bool}), do: true
  defp sample_value(%{type: type}) when type in [:double, :float], do: 1.0
  defp sample_value(_props), do: 1

  defp message_frame(command, metadata, payload, opts \\ []) do
    command_binary = BaseCommand.encode(%BaseCommand{type: :MESSAGE, message: command})

    command_size = byte_size(command_binary)

    metadata_encoded = Binary.MessageMetadata.encode(metadata)
    metadata_size = byte_size(metadata_encoded)
    checksummed = <<metadata_size::32, metadata_encoded::binary, payload::binary>>

    message_part =
      if Keyword.get(opts, :checksum, true) do
        <<@magic_message::16, :crc32cer.nif(checksummed)::32, checksummed::binary>>
      else
        checksummed
      end

    message_part =
      case Keyword.get(opts, :broker_entry_metadata) do
        nil ->
          message_part

        broker_entry_metadata ->
          encoded = Binary.BrokerEntryMetadata.encode(broker_entry_metadata)
          <<@magic_broker_entry::16, byte_size(encoded)::32, encoded::binary, message_part::binary>>
      end

    total_size = 4 + command_size + byte_size(message_part)

    <<total_size::32, command_size::32, command_binary::binary, message_part::binary>>
  end
end
