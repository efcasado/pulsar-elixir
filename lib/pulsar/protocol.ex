defmodule Pulsar.Protocol do
  # https://pulsar.apache.org/docs/next/developing-binary-protocol/#framing
  @moduledoc false
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

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

  Halts at the first frame that fails to decode: a frame whose checksum does not
  match, or that does not match the framing at all, means the position in the
  stream can no longer be trusted either, so the caller must discard the
  connection rather than keep parsing.
  """
  @spec decode_stream(binary()) :: {:ok, [term()], binary()} | {:error, term()}
  def decode_stream(buffer), do: decode_stream(buffer, [])

  defp decode_stream(<<total_size::32, rest::binary>> = buffer, commands) when byte_size(rest) >= total_size do
    frame_size = total_size + 4
    <<frame::bytes-size(^frame_size), tail::binary>> = buffer

    case decode(frame) do
      {:ok, command} -> decode_stream(tail, [command | commands])
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_stream(buffer, commands), do: {:ok, Enum.reverse(commands), buffer}

  @doc """
  Decodes a single complete frame.

  Never raises: frames come from the network, and a raise here would take the
  connection down along with its consumers and producers. Every way the bytes can
  be wrong comes back as `{:error, reason}`.
  """
  @spec decode(binary()) :: {:ok, term()} | {:error, term()}
  def decode(frame)

  # Message command with broker entry metadata
  def decode(
        <<_total_size::32, size::32, command::bytes-size(size), @magic_broker_entry_metadata::16,
          broker_metadata_size::32, broker_metadata::bytes-size(broker_metadata_size), @magic_crc32c::16, checksum::32,
          checksummed::binary>>
      ) do
    decode_message(command, checksummed, checksum, broker_metadata)
  end

  # Message command without broker entry metadata (original format)
  def decode(
        <<_total_size::32, size::32, command::bytes-size(size), @magic_crc32c::16, checksum::32, checksummed::binary>>
      ) do
    decode_message(command, checksummed, checksum, nil)
  end

  def decode(<<_total_size::32, size::32, command::bytes-size(size)>>) do
    case decode_base_command(command) do
      {:ok, base_command} -> command_from_type(base_command)
      {:error, reason} -> {:error, reason}
    end
  end

  def decode(_frame), do: {:error, :malformed_frame}

  # The checksummed region is everything following the checksum field, which is
  # also exactly the metadata size, metadata and payload. The broker entry
  # metadata lies outside it, so it is left undecoded until the checksum passes.
  defp decode_message(command, checksummed, checksum, broker_metadata) do
    if :crc32cer.nif(checksummed) == checksum do
      decode_message_parts(command, checksummed, broker_metadata)
    else
      {:error, :checksum_mismatch}
    end
  end

  defp decode_message_parts(
         command,
         <<metadata_size::32, metadata::bytes-size(metadata_size), payload::binary>>,
         broker_metadata
       ) do
    with {:ok, base_command} <- decode_base_command(command),
         {:ok, decoded_command} <- command_from_type(base_command),
         {:ok, decoded_metadata} <- decode_message_metadata(metadata),
         {:ok, broker_entry_metadata} <- decode_broker_entry_metadata(broker_metadata) do
      {:ok, {decoded_command, decoded_metadata, payload, broker_entry_metadata}}
    end
  end

  defp decode_message_parts(_command, _checksummed, _broker_metadata), do: {:error, :malformed_frame}

  defp decode_base_command(binary), do: decode_protobuf(Binary.BaseCommand, binary, :malformed_command)

  defp decode_broker_entry_metadata(nil), do: {:ok, nil}

  defp decode_broker_entry_metadata(binary),
    do: decode_protobuf(Binary.BrokerEntryMetadata, binary, :malformed_broker_entry_metadata)

  defp decode_message_metadata(binary), do: decode_protobuf(Binary.MessageMetadata, binary, :malformed_message_metadata)

  defp decode_protobuf(module, binary, reason) do
    {:ok, module.decode(binary)}
  rescue
    _exception -> {:error, reason}
  end

  defp command_to_type(%module{}) do
    Map.fetch!(@type_by_module, module)
  end

  # Encode path only: the type came from a command struct, so the field exists.
  defp field_name_from_type(type) do
    Map.fetch!(@field_by_type, type)
  end

  # An unknown type is not an error to protobuf: it keeps the raw integer.
  defp command_from_type(%Binary.BaseCommand{type: type} = base_command) do
    case Map.fetch(@field_by_type, type) do
      {:ok, field_name} -> {:ok, Map.fetch!(base_command, field_name)}
      :error -> {:error, {:unsupported_command_type, type}}
    end
  end

  @spec to_key_value_list(map() | nil) :: [Binary.KeyValue.t()]
  def to_key_value_list(nil), do: []

  def to_key_value_list(props) when is_map(props) do
    Enum.map(props, fn {k, v} -> %Binary.KeyValue{key: to_string(k), value: to_string(v)} end)
  end
end
