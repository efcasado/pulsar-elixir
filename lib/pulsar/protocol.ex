defmodule Pulsar.Protocol do
  @doc """
  Helper module to simplify working with the Pulsar binary protocol.
  """

  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary


  def latest_version() do
    %Binary.ProtocolVersion{}
    |> Map.keys
    |> Enum.map(&(Atom.to_string(&1)))
    |> Enum.reduce([], fn(<<"v", version::binary>>, acc) -> [String.to_integer(version)| acc]; (_, acc) -> acc end)
    |> Enum.sort
    |> Enum.at(-1)
  end
end
