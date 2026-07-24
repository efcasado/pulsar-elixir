defmodule Pulsar.Protocol do
  # https://pulsar.apache.org/docs/next/developing-binary-protocol/#framing
  @moduledoc false
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

  require Logger

  @doc """
  Helper module to simplify working with the Pulsar binary protocol.
  """

  # A command's field in the BaseCommand oneof carries the same protobuf tag as
  # its entry in the BaseCommand.Type enum, so both directions of the mapping
  # come from the generated schema. Pairing by tag also covers the camelCase
  # fields (:authChallenge, :reachedEndOfTopic, ...), which cannot be derived
  # from the type name.
  @type_by_tag Map.new(Binary.BaseCommand.Type.__message_props__().field_props, fn {tag, props} ->
                 {tag, props.name_atom}
               end)

  @oneof_by_tag Map.new(Binary.BaseCommand.__message_props__().field_props, fn {tag, props} ->
                  {tag, props}
                end)

  @field_by_type (for {tag, type} <- @type_by_tag, into: %{} do
                    {type, Map.fetch!(@oneof_by_tag, tag).name_atom}
                  end)

  @type_by_module (for {tag, type} <- @type_by_tag, into: %{} do
                     {Map.fetch!(@oneof_by_tag, tag).type, type}
                   end)

  @magic_crc32c 0x0E01
  @magic_broker_entry_metadata 0x0E02

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
      @magic_crc32c::16,
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

  @doc """
  Splits a stream of bytes into complete frames and decodes them.

  Frames are length-prefixed, so reassembling one that spans TCP packets needs
  no state beyond the leftover bytes returned alongside the commands.
  """
  @spec decode_stream(binary()) :: {[term()], binary()}
  def decode_stream(buffer), do: decode_stream(buffer, [])

  defp decode_stream(<<total_size::32, rest::binary>> = buffer, commands) when byte_size(rest) >= total_size do
    frame_size = total_size + 4
    <<frame::bytes-size(^frame_size), tail::binary>> = buffer

    decode_stream(tail, [decode(frame) | commands])
  end

  defp decode_stream(buffer, commands), do: {Enum.reverse(commands), buffer}

  # Message command with broker entry metadata
  def decode(
        <<_total_size::32, size::32, command::bytes-size(size), @magic_broker_entry_metadata::16,
          broker_metadata_size::32, broker_metadata::bytes-size(broker_metadata_size), @magic_crc32c::16, _checksum::32,
          metadata_size::32, metadata::bytes-size(metadata_size), payload::binary>>
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
        <<_total_size::32, size::32, command::bytes-size(size), @magic_crc32c::16, _checksum::32, metadata_size::32,
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

  defp command_to_type(%module{}) do
    Map.fetch!(@type_by_module, module)
  end

  defp command_from_type(%Binary.BaseCommand{type: type} = base_command) do
    field_name = field_name_from_type(type)

    Map.fetch!(base_command, field_name)
  end

  defp field_name_from_type(type) do
    Map.fetch!(@field_by_type, type)
  end

  @spec to_key_value_list(map() | nil) :: [Binary.KeyValue.t()]
  def to_key_value_list(nil), do: []

  def to_key_value_list(props) when is_map(props) do
    Enum.map(props, fn {k, v} -> %Binary.KeyValue{key: to_string(k), value: to_string(v)} end)
  end
end
