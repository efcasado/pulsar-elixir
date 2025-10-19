defmodule Pulsar.Protocol do
  # https://pulsar.apache.org/docs/next/developing-binary-protocol/#framing
  @doc """
  Helper module to simplify working with the Pulsar binary protocol.
  """

  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

  require Logger

  def latest_version() do
    %Binary.ProtocolVersion{}
    |> Map.keys()
    |> Enum.map(&Atom.to_string(&1))
    |> Enum.reduce([], fn
      <<"v", version::binary>>, acc -> [String.to_integer(version) | acc]
      _, acc -> acc
    end)
    |> Enum.sort()
    |> Enum.at(-1)
  end

  def encode(command) do
    type = command_to_type(command)

    field_name = field_name_from_type(type)

    encoded =
      %Binary.BaseCommand{}
      |> Map.put(:type, type)
      |> Map.put(field_name, command)
      |> Binary.BaseCommand.encode()

    size = byte_size(encoded)
    <<size + 4::32, size::32, encoded::binary>>
  end

  # Message command with broker entry metadata
  def decode(<<
        _total_size::32,
        size::32,
        command::bytes-size(size),
        0x0E02::16,
        broker_metadata_size::32,
        broker_metadata::bytes-size(broker_metadata_size),
        0x0E01::16,
        _checksum::32,
        metadata_size::32,
        metadata::bytes-size(metadata_size),
        payload::binary
      >>) do
    # Decode broker entry metadata
    broker_entry_metadata = Binary.BrokerEntryMetadata.decode(broker_metadata)

    # Decode message metadata
    message_metadata = Binary.MessageMetadata.decode(metadata)

    command =
      Binary.BaseCommand.decode(command)
      |> do_decode

    # Handle batch messages
    payload = unwrap_messages(message_metadata, payload)

    {command, message_metadata, payload, broker_entry_metadata}
  end

  # Message command without broker entry metadata (original format)
  def decode(<<
        _total_size::32,
        size::32,
        command::bytes-size(size),
        0x0E01::16,
        _checksum::32,
        metadata_size::32,
        metadata::bytes-size(metadata_size),
        payload::binary
      >>) do
    # message command
    metadata = Binary.MessageMetadata.decode(metadata)

    command =
      Binary.BaseCommand.decode(command)
      |> do_decode

    # Handle batch messages
    payload = unwrap_messages(metadata, payload)

    {command, metadata, payload, nil}
  end

  def decode(<<_total_size::32, size::32, command::bytes-size(size)>>) do
    # single command
    Binary.BaseCommand.decode(command)
    |> do_decode
  end

  defp do_decode(%Binary.BaseCommand{} = base_command) do
    command_from_type(base_command)
  end

  defp do_decode(other) do
    Logger.warning("Unhandled command #{inspect(other)}")
    other
  end

  # only required for client-sent commands
  defp command_to_type(%Binary.CommandConnect{}), do: :CONNECT
  defp command_to_type(%Binary.CommandPing{}), do: :PING
  defp command_to_type(%Binary.CommandPong{}), do: :PONG
  defp command_to_type(%Binary.CommandSubscribe{}), do: :SUBSCRIBE
  defp command_to_type(%Binary.CommandProducer{}), do: :PRODUCER
  defp command_to_type(%Binary.CommandSend{}), do: :SEND
  defp command_to_type(%Binary.CommandFlow{}), do: :FLOW
  defp command_to_type(%Binary.CommandLookupTopic{}), do: :LOOKUP
  defp command_to_type(%Binary.CommandPartitionedTopicMetadata{}), do: :PARTITIONED_METADATA
  defp command_to_type(%Binary.CommandAck{}), do: :ACK
  defp command_to_type(%Binary.CommandCloseConsumer{}), do: :CLOSE_CONSUMER
  defp command_to_type(%Binary.CommandSeek{}), do: :SEEK
  # defp command_to_type(command) do
  #   command
  #   |> Map.get(:__struct__)
  #   |> Atom.to_string
  #   |> String.split(".")
  #   |> Enum.at(-1)
  # end

  defp command_from_type(%Binary.BaseCommand{type: type} = base_command) do
    field_name = field_name_from_type(type)

    base_command
    |> Map.fetch!(field_name)
  end

  defp field_name_from_type(:LOOKUP) do
    :lookupTopic
  end

  defp field_name_from_type(:LOOKUP_RESPONSE) do
    :lookupTopicResponse
  end

  defp field_name_from_type(:PARTITIONED_METADATA) do
    :partitionMetadata
  end

  defp field_name_from_type(:PARTITIONED_METADATA_RESPONSE) do
    :partitionMetadataResponse
  end

  defp field_name_from_type(:ACK_RESPONSE) do
    :ackResponse
  end

  defp field_name_from_type(type) do
    type
    |> Atom.to_string()
    |> String.downcase()
    |> String.to_existing_atom()
  end

  defp unwrap_messages(metadata, payload) do
    case metadata.num_messages_in_batch > 0 do
      true -> parse_batch_messages(payload, metadata.num_messages_in_batch, [])
      false -> [{nil, payload}]
    end
  end

  defp parse_batch_messages(<<>>, 0, acc), do: Enum.reverse(acc)
  defp parse_batch_messages(_, 0, acc), do: Enum.reverse(acc)

  defp parse_batch_messages(
         <<
           metadata_size::32,
           metadata::bytes-size(metadata_size),
           data::binary
         >> = payload,
         count,
         acc
       ) do
    single_metadata = Binary.SingleMessageMetadata.decode(metadata)

    payload_size = single_metadata.payload_size

    <<payload::bytes-size(payload_size), rest::binary>> = data

    # Build individual message as {metadata, payload} tuple
    message = {single_metadata, payload}

    parse_batch_messages(rest, count - 1, [message | acc])
  rescue
    _ -> [{nil, payload}]
  end

  defp parse_batch_messages(payload, _, _) do
    [{nil, payload}]
  end
end
