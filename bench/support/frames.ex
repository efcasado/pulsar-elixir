defmodule Pulsar.Bench.Frames do
  @moduledoc """
  Builders for the wire shapes the benchmarks feed through `Pulsar.Protocol`.
  """

  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

  @magic_crc32c 0x0E01

  @doc """
  A complete MESSAGE frame carrying a payload of `size` bytes.
  """
  @spec message(non_neg_integer()) :: binary()
  def message(size) do
    command =
      Binary.BaseCommand.encode(%Binary.BaseCommand{
        type: :MESSAGE,
        message: %Binary.CommandMessage{
          consumer_id: 1,
          message_id: %Binary.MessageIdData{ledgerId: 1, entryId: 1}
        }
      })

    metadata =
      Binary.MessageMetadata.encode(%Binary.MessageMetadata{
        producer_name: "bench",
        sequence_id: 1,
        publish_time: 1
      })

    checksummed = <<byte_size(metadata)::32, metadata::binary, :binary.copy("x", size)::binary>>
    part = <<@magic_crc32c::16, :crc32cer.nif(checksummed)::32, checksummed::binary>>

    <<4 + byte_size(command) + byte_size(part)::32, byte_size(command)::32, command::binary, part::binary>>
  end

  @doc """
  Splits a frame the way a socket delivers it, into packets of `size` bytes.

  The last packet is whatever is left, so the packets always add up to the whole
  frame. Dropping it would mean the frame never completes, and a benchmark over
  incomplete frames measures accumulation without the reassembly and decoding it
  is there to time.
  """
  @spec packets(binary(), pos_integer()) :: [binary()]
  def packets(frame, size) when byte_size(frame) <= size, do: [frame]

  def packets(frame, size) do
    <<packet::binary-size(size), rest::binary>> = frame

    [packet | packets(rest, size)]
  end
end
