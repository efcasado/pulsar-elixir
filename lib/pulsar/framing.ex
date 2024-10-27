defmodule Pulsar.Framing do
  # https://pulsar.apache.org/docs/next/developing-binary-protocol/#framing
  @doc false

  require Logger

  alias Pulsar.Proto

  def encode(%Proto.CommandConnect{} = connect) do
    encoded =
      %Proto.BaseCommand{type: :CONNECT, connect: connect}
      |> Proto.BaseCommand.encode

    size = byte_size(encoded)
    <<(size + 4)::32, size::32, encoded::binary>>

  end
  def encode(%Proto.CommandPing{} = ping) do
    encoded =
      %Proto.BaseCommand{type: :PING, ping: ping}
      |> Proto.BaseCommand.encode

    size = byte_size(encoded)
    <<(size + 4)::32, size::32, encoded::binary>>
  end
  def encode(%Proto.CommandPong{} = pong) do
    encoded =
      %Proto.BaseCommand{type: :PONG, ping: pong}
      |> Proto.BaseCommand.encode

    size = byte_size(encoded)
    <<(size + 4)::32, size::32, encoded::binary>>
  end

  def decode(<<_total_size::32, size::32, encoded::binary>>) do
    # TO-DO: Implement buffering
    Proto.BaseCommand.decode(encoded)
    |> do_decode
  end

  defp do_decode(%Proto.BaseCommand{} = base_command) do
    command_from_type(base_command)
  end
  defp do_decode(other) do
    Logger.warning("Unhandled command #{inspect other}")
    other
  end

  defp command_from_type(%Proto.BaseCommand{type: type} = base_command) do
    command =
      type
      |> Atom.to_string
      |> String.downcase
      |> String.to_existing_atom

    base_command
    |> Map.fetch!(command)
  end
end
