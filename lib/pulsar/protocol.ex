defmodule Pulsar.Protocol do
  # https://pulsar.apache.org/docs/next/developing-binary-protocol/#framing
  @moduledoc false
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

  require Logger

  @doc """
  Helper module to simplify working with the Pulsar binary protocol.
  """

  def latest_version do
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

  @doc """
  Encodes a CommandSend with message metadata and payload.
  Returns the complete binary frame ready to send to the broker.
  """
  def encode_message(command_send, message_metadata, payload) do
    type = command_to_type(command_send)
    field_name = field_name_from_type(type)

    command_binary =
      %Binary.BaseCommand{}
      |> Map.put(:type, type)
      |> Map.put(field_name, command_send)
      |> Binary.BaseCommand.encode()

    command_size = byte_size(command_binary)

    metadata_encoded = Binary.MessageMetadata.encode(message_metadata)
    metadata_size = byte_size(metadata_encoded)

    checksum_data = <<metadata_size::32, metadata_encoded::binary, payload::binary>>
    checksum = :crc32cer.nif(checksum_data)

    message_part = <<
      0x0E01::16,
      checksum::32,
      metadata_size::32,
      metadata_encoded::binary,
      payload::binary
    >>

    message_part_size = byte_size(message_part)
    total_size = 4 + command_size + message_part_size

    <<
      total_size::32,
      command_size::32,
      command_binary::binary,
      message_part::binary
    >>
  end

  # Message command with broker entry metadata
  def decode(
        <<_total_size::32, size::32, command::bytes-size(size), 0x0E02::16, broker_metadata_size::32,
          broker_metadata::bytes-size(broker_metadata_size), 0x0E01::16, _checksum::32, metadata_size::32,
          metadata::bytes-size(metadata_size), payload::binary>>
      ) do
    # Decode broker entry metadata
    broker_entry_metadata = Binary.BrokerEntryMetadata.decode(broker_metadata)

    # Decode message metadata
    message_metadata = Binary.MessageMetadata.decode(metadata)

    command =
      command
      |> Binary.BaseCommand.decode()
      |> do_decode()

    {command, message_metadata, payload, broker_entry_metadata}
  end

  # Message command without broker entry metadata (original format)
  def decode(
        <<_total_size::32, size::32, command::bytes-size(size), 0x0E01::16, _checksum::32, metadata_size::32,
          metadata::bytes-size(metadata_size), payload::binary>>
      ) do
    # message command
    metadata = Binary.MessageMetadata.decode(metadata)

    command =
      command
      |> Binary.BaseCommand.decode()
      |> do_decode()

    {command, metadata, payload, nil}
  end

  def decode(<<_total_size::32, size::32, command::bytes-size(size)>>) do
    # single command
    command
    |> Binary.BaseCommand.decode()
    |> do_decode()
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
  defp command_to_type(%Binary.CommandCloseProducer{}), do: :CLOSE_PRODUCER
  defp command_to_type(%Binary.CommandSeek{}), do: :SEEK

  defp command_to_type(%Binary.CommandRedeliverUnacknowledgedMessages{}), do: :REDELIVER_UNACKNOWLEDGED_MESSAGES

  # defp command_to_type(command) do
  #   command
  #   |> Map.get(:__struct__)
  #   |> Atom.to_string
  #   |> String.split(".")
  #   |> Enum.at(-1)
  # end

  defp command_from_type(%Binary.BaseCommand{type: type} = base_command) do
    field_name = field_name_from_type(type)

    Map.fetch!(base_command, field_name)
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

  defp field_name_from_type(:SEND_RECEIPT) do
    :send_receipt
  end

  defp field_name_from_type(:SEND_ERROR) do
    :send_error
  end

  defp field_name_from_type(:REDELIVER_UNACKNOWLEDGED_MESSAGES) do
    :redeliverUnacknowledgedMessages
  end

  defp field_name_from_type(type) do
    type
    |> Atom.to_string()
    |> String.downcase()
    |> String.to_existing_atom()
  end

  @spec to_key_value_list(map() | nil) :: [Binary.KeyValue.t()]
  def to_key_value_list(nil), do: []

  def to_key_value_list(props) when is_map(props) do
    Enum.map(props, fn {k, v} -> %Binary.KeyValue{key: to_string(k), value: to_string(v)} end)
  end
end
