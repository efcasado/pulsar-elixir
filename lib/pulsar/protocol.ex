defmodule Pulsar.Protocol do
  # https://pulsar.apache.org/docs/next/developing-binary-protocol/#framing
  @moduledoc false

  alias Pulsar.Config
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

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

  @doc """
  The highest protocol version the vendored schema declares.
  """
  @spec latest_version() :: pos_integer()
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

  @doc """
  Frames a command for the wire.

  A `CommandSend` also carries message metadata and a payload, which are framed
  after the command and covered by a CRC32C checksum.
  """
  @spec encode(struct()) :: binary()
  def encode(command), do: frame(encode_base_command(command), <<>>)

  @spec encode(struct(), struct(), binary()) :: binary()
  def encode(command_send, message_metadata, payload) do
    frame(encode_base_command(command_send), message_part(message_metadata, payload))
  end

  defp encode_base_command(command) do
    type = command_to_type(command)

    %Binary.BaseCommand{}
    |> Map.put(:type, type)
    |> Map.put(field_name_from_type(type), command)
    |> Binary.BaseCommand.encode()
  end

  # The counterpart of decode_message/3: the checksum covers the metadata size,
  # metadata and payload, and nothing else.
  defp message_part(message_metadata, payload) do
    metadata = Binary.MessageMetadata.encode(message_metadata)
    checksummed = <<byte_size(metadata)::32, metadata::binary, payload::binary>>

    <<@magic_crc32c::16, :crc32cer.nif(checksummed)::32, checksummed::binary>>
  end

  defp frame(command, rest) do
    command_size = byte_size(command)

    <<4 + command_size + byte_size(rest)::32, command_size::32, command::binary, rest::binary>>
  end

  @doc """
  Splits a stream of bytes into complete frames and decodes them.

  Frames are length-prefixed, so reassembling one that spans TCP packets needs
  no state beyond the leftover bytes returned alongside the commands.

  Halts at the first frame it cannot locate the end of, since the position in the
  stream is then lost and the caller must discard the connection. A frame whose
  contents are untrustworthy but whose boundaries are known is returned as
  `{:invalid, command, bytes, validation_error}` and parsing continues.

  A frame claiming more than `max_frame_size` bytes is rejected on its length
  prefix alone, before its bytes are waited for, so a desynced or absurd length
  cannot buffer without bound. It defaults to `Pulsar.Config.max_frame_size/0`;
  `Pulsar.Broker` passes the limit of the client it belongs to, since each client
  may face a cluster with its own `maxMessageSize`.
  """
  @spec decode_stream(binary(), pos_integer()) :: {:ok, [term()], binary()} | {:error, term()}
  def decode_stream(buffer, max_frame_size \\ Config.max_frame_size()) do
    decode_stream(buffer, [], max_frame_size)
  end

  defp decode_stream(<<total_size::32, _rest::binary>>, _commands, max_frame_size) when total_size + 4 > max_frame_size do
    {:error, {:frame_too_large, total_size + 4}}
  end

  defp decode_stream(<<total_size::32, rest::binary>> = buffer, commands, max_frame_size)
       when byte_size(rest) >= total_size do
    frame_size = total_size + 4
    <<frame::bytes-size(^frame_size), tail::binary>> = buffer

    case decode(frame) do
      {:ok, command} -> decode_stream(tail, [command | commands], max_frame_size)
      {:invalid, _command, _bytes, _reason} = invalid -> decode_stream(tail, [invalid | commands], max_frame_size)
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_stream(buffer, commands, _max_frame_size), do: {:ok, Enum.reverse(commands), buffer}

  @doc """
  Decodes a single complete frame.

  A frame carries a command, optionally followed by broker entry metadata, a
  CRC32C checksum, and the message metadata and payload. Both optional sections
  are recognised by their leading magic number.

  Never raises: frames come from the network, and a raise here would take the
  connection down along with its consumers and producers.

  A frame whose contents cannot be trusted, but whose command says which consumer
  it was headed for, is returned as `{:invalid, command, bytes, validation_error}`
  so the consumer can hand it to its callback and acknowledge it. `{:error, _}` is
  reserved for frames that leave the stream unusable.
  """
  @spec decode(binary()) ::
          {:ok, term()} | {:invalid, struct(), binary(), atom()} | {:error, term()}
  def decode(frame)

  def decode(<<_total_size::32, size::32, command::bytes-size(size), rest::binary>>) do
    decode_sections(command, rest)
  end

  def decode(_frame), do: {:error, :malformed_frame}

  defp decode_sections(command, <<>>) do
    with {:ok, base_command} <- decode_base_command(command),
         {:ok, decoded_command} <- command_from_type(base_command) do
      command_only(decoded_command)
    end
  end

  defp decode_sections(
         command,
         <<@magic_broker_entry_metadata::16, size::32, broker_metadata::bytes-size(size), rest::binary>>
       ) do
    decode_message(command, rest, broker_metadata)
  end

  defp decode_sections(command, rest), do: decode_message(command, rest, nil)
  # Every other command is complete on its own, but a MESSAGE is an envelope: one
  # that stops at the command has lost what it was carrying.
  defp command_only(%Binary.CommandMessage{} = command), do: {:invalid, command, <<>>, :malformed_frame}
  defp command_only(command), do: {:ok, command}

  # The checksummed region is everything following the checksum field, which is
  # also exactly the metadata size, metadata and payload. The broker entry
  # metadata lies outside it, so it is left undecoded until the checksum passes.
  defp decode_message(command, <<@magic_crc32c::16, checksum::32, checksummed::binary>>, broker_metadata) do
    if :crc32cer.nif(checksummed) == checksum do
      decode_message_parts(command, checksummed, broker_metadata)
    else
      invalid(command, checksummed, :checksum_mismatch)
    end
  end

  # No magic number, so no checksum to verify against.
  defp decode_message(command, rest, broker_metadata) do
    decode_message_parts(command, rest, broker_metadata)
  end

  defp decode_message_parts(
         command,
         <<metadata_size::32, metadata::bytes-size(metadata_size), payload::binary>> = bytes,
         broker_metadata
       ) do
    with {:ok, base_command} <- decode_base_command(command),
         {:ok, decoded_command} <- command_from_type(base_command) do
      case {decode_message_metadata(metadata), decode_broker_entry_metadata(broker_metadata)} do
        {{:ok, decoded_metadata}, {:ok, broker_entry_metadata}} ->
          {:ok, {decoded_command, decoded_metadata, payload, broker_entry_metadata}}

        {{:error, reason}, _} ->
          flag_invalid(decoded_command, bytes, reason)

        {_, {:error, reason}} ->
          flag_invalid(decoded_command, bytes, reason)
      end
    end
  end

  defp decode_message_parts(command, bytes, _broker_metadata), do: invalid(command, bytes, :malformed_frame)

  # The command region is never covered by the checksum, so it still names the
  # consumer even when nothing after it can be trusted.
  defp invalid(command, bytes, reason) do
    with {:ok, base_command} <- decode_base_command(command),
         {:ok, decoded_command} <- command_from_type(base_command) do
      flag_invalid(decoded_command, bytes, reason)
    end
  end

  defp flag_invalid(%Binary.CommandMessage{} = command, bytes, reason), do: {:invalid, command, bytes, reason}
  defp flag_invalid(_command, _bytes, reason), do: {:error, reason}

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
