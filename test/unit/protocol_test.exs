defmodule Pulsar.ProtocolTest do
  use ExUnit.Case, async: true

  alias Pulsar.Protocol
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

  @magic_message 0x0E01
  @magic_broker_entry 0x0E02

  # Every command `Pulsar.Protocol.encode/1` knows how to frame, with its
  # proto2 required fields populated.
  @client_commands [
    {:PING, :ping, %Binary.CommandPing{}},
    {:PONG, :pong, %Binary.CommandPong{}},
    {:CONNECT, :connect, %Binary.CommandConnect{client_version: "pulsar-elixir", protocol_version: 21}},
    {:SUBSCRIBE, :subscribe,
     %Binary.CommandSubscribe{
       topic: "persistent://public/default/t",
       subscription: "sub",
       subType: :Shared,
       consumer_id: 1,
       request_id: 2
     }},
    {:PRODUCER, :producer,
     %Binary.CommandProducer{topic: "persistent://public/default/t", producer_id: 1, request_id: 2}},
    {:SEND, :send, %Binary.CommandSend{producer_id: 1, sequence_id: 2}},
    {:FLOW, :flow, %Binary.CommandFlow{consumer_id: 1, messagePermits: 100}},
    {:LOOKUP, :lookupTopic, %Binary.CommandLookupTopic{topic: "persistent://public/default/t", request_id: 1}},
    {:PARTITIONED_METADATA, :partitionMetadata,
     %Binary.CommandPartitionedTopicMetadata{topic: "persistent://public/default/t", request_id: 1}},
    {:ACK, :ack, %Binary.CommandAck{consumer_id: 1, ack_type: :Individual}},
    {:CLOSE_CONSUMER, :close_consumer, %Binary.CommandCloseConsumer{consumer_id: 1, request_id: 2}},
    {:CLOSE_PRODUCER, :close_producer, %Binary.CommandCloseProducer{producer_id: 1, request_id: 2}},
    {:SEEK, :seek, %Binary.CommandSeek{consumer_id: 1, request_id: 2}},
    {:REDELIVER_UNACKNOWLEDGED_MESSAGES, :redeliverUnacknowledgedMessages,
     %Binary.CommandRedeliverUnacknowledgedMessages{consumer_id: 1}}
  ]

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
      assert %Binary.BaseCommand{type: :PING} = Binary.BaseCommand.decode(command)
    end

    test "total_size never counts its own 4 bytes" do
      for {_type, _field, command} <- @client_commands do
        frame = Protocol.encode(command)
        <<total_size::32, _rest::binary>> = frame

        assert byte_size(frame) - 4 == total_size,
               "total_size must describe everything after the size prefix, for #{inspect(command.__struct__)}"
      end
    end

    test "sets the BaseCommand type and populates the matching oneof field" do
      for {type, field, command} <- @client_commands do
        <<_total_size::32, command_size::32, encoded::bytes-size(command_size)>> = Protocol.encode(command)

        base_command = Binary.BaseCommand.decode(encoded)

        assert base_command.type == type
        assert Map.fetch!(base_command, field) == command
      end
    end
  end

  describe "decode/1 of command-only frames" do
    test "round-trips every command the client can send" do
      for {_type, _field, command} <- @client_commands do
        assert Protocol.decode(Protocol.encode(command)) == command
      end
    end

    test "decodes a broker-sent command frame" do
      success = %Binary.CommandSuccess{request_id: 42}
      frame = command_frame(:SUCCESS, :success, success)

      assert Protocol.decode(frame) == success
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
      assert %Binary.BaseCommand{type: :SEND} = Binary.BaseCommand.decode(command)
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

      assert {command, metadata, payload, nil} = Protocol.decode(frame)
      assert command == ctx.command
      assert metadata == ctx.metadata
      assert payload == ctx.payload
    end

    test "handles an empty payload", ctx do
      frame = Protocol.encode_message(ctx.command, ctx.metadata, "")

      assert {_command, _metadata, "", nil} = Protocol.decode(frame)
    end

    test "handles a payload large enough to need multi-byte varints", ctx do
      payload = :binary.copy("x", 100_000)
      frame = Protocol.encode_message(ctx.command, ctx.metadata, payload)

      assert {_command, _metadata, ^payload, nil} = Protocol.decode(frame)
    end
  end

  describe "decode/1 of message frames" do
    test "decodes a MESSAGE frame without broker entry metadata" do
      command = %Binary.CommandMessage{consumer_id: 1, message_id: %Binary.MessageIdData{ledgerId: 5, entryId: 6}}
      metadata = %Binary.MessageMetadata{producer_name: "p", sequence_id: 1, publish_time: 1}

      frame = message_frame(command, metadata, "payload")

      assert {^command, decoded_metadata, "payload", nil} = Protocol.decode(frame)
      assert decoded_metadata == metadata
    end

    test "decodes a MESSAGE frame with broker entry metadata" do
      command = %Binary.CommandMessage{consumer_id: 1, message_id: %Binary.MessageIdData{ledgerId: 5, entryId: 6}}
      metadata = %Binary.MessageMetadata{producer_name: "p", sequence_id: 1, publish_time: 1}
      broker_entry = %Binary.BrokerEntryMetadata{broker_timestamp: 1_700_000_000_000, index: 99}

      frame = message_frame(command, metadata, "payload", broker_entry_metadata: broker_entry)

      assert {^command, decoded_metadata, "payload", decoded_broker_entry} = Protocol.decode(frame)
      assert decoded_metadata == metadata
      assert decoded_broker_entry == broker_entry
    end

    test "treats a payload containing the message magic as opaque bytes" do
      command = %Binary.CommandMessage{consumer_id: 1, message_id: %Binary.MessageIdData{ledgerId: 5, entryId: 6}}
      metadata = %Binary.MessageMetadata{producer_name: "p", sequence_id: 1, publish_time: 1}
      payload = <<@magic_message::16, 0, 0, 0, 0, "not a frame">>

      frame = message_frame(command, metadata, payload)

      assert {_command, _metadata, ^payload, nil} = Protocol.decode(frame)
    end
  end

  describe "decode/1 current behaviour on damaged input" do
    # These document what the codec does today. They are expected to change
    # when checksum verification and tagged error returns land.

    test "does not verify the checksum: a corrupted payload decodes as valid" do
      command = %Binary.CommandSend{producer_id: 1, sequence_id: 1}
      metadata = %Binary.MessageMetadata{producer_name: "p", sequence_id: 1, publish_time: 1}

      frame = Protocol.encode_message(command, metadata, "aaaa")
      corrupted = binary_part(frame, 0, byte_size(frame) - 4) <> "bbbb"

      assert {_command, _metadata, "bbbb", nil} = Protocol.decode(corrupted)
    end

    test "raises when handed a truncated frame" do
      <<partial::bytes-size(6), _rest::binary>> = Protocol.encode(%Binary.CommandPing{})

      assert_raise FunctionClauseError, fn -> Protocol.decode(partial) end
    end

    test "raises when handed an empty binary" do
      assert_raise FunctionClauseError, fn -> Protocol.decode(<<>>) end
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
    encoded =
      %Binary.BaseCommand{type: type}
      |> Map.put(field, command)
      |> Binary.BaseCommand.encode()

    size = byte_size(encoded)
    <<size + 4::32, size::32, encoded::binary>>
  end

  defp message_frame(command, metadata, payload, opts \\ []) do
    command_binary = Binary.BaseCommand.encode(%Binary.BaseCommand{type: :MESSAGE, message: command})

    command_size = byte_size(command_binary)

    metadata_encoded = Binary.MessageMetadata.encode(metadata)
    metadata_size = byte_size(metadata_encoded)
    checksum = :crc32cer.nif(<<metadata_size::32, metadata_encoded::binary, payload::binary>>)

    message_part =
      <<@magic_message::16, checksum::32, metadata_size::32, metadata_encoded::binary, payload::binary>>

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
