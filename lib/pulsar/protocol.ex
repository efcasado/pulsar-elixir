defmodule Pulsar.Protocol do
  # https://pulsar.apache.org/docs/next/developing-binary-protocol/#framing
  @doc """
  Helper module to simplify working with the Pulsar binary protocol.
  """

  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

  require Logger
  

  def latest_version() do
    %Binary.ProtocolVersion{}
    |> Map.keys
    |> Enum.map(&(Atom.to_string(&1)))
    |> Enum.reduce([], fn(<<"v", version::binary>>, acc) -> [String.to_integer(version)| acc]; (_, acc) -> acc end)
    |> Enum.sort
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
    <<(size + 4)::32, size::32, encoded::binary>>
  end

  def decode(<<_total_size::32, _size::32, encoded::binary>>) do
    Binary.BaseCommand.decode(encoded)
    |> do_decode
  end

  defp do_decode(%Binary.BaseCommand{} = base_command) do
    command_from_type(base_command)
  end
  defp do_decode(other) do
    Logger.warning("Unhandled command #{inspect other}")
    other
  end

  # only required for client-sent commands
  defp command_to_type(%Binary.CommandConnect{}), do: :CONNECT
  defp command_to_type(%Binary.CommandPing{}), do: :PING
  defp command_to_type(%Binary.CommandPong{}), do: :PONG
  
  defp command_from_type(%Binary.BaseCommand{type: type} = base_command) do
    field_name = field_name_from_type(type)
    base_command
    |> Map.fetch!(field_name)
  end

  defp field_name_from_type(type) do
    type
    |> Atom.to_string
    |> String.downcase
    |> String.to_existing_atom
  end
end
