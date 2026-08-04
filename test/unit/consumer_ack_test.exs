defmodule Pulsar.Consumer.AckTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Pulsar.Consumer.Ack
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

  doctest Ack

  describe "record_ack/2" do
    test "counts messages from different entries independently" do
      {ackable, _ack} = Ack.record_ack(Ack.new(), [batch_id(0, 2), batch_id(0, 2, entry: 43)])

      assert ackable == []
    end

    test "treats a batch of one as a plain message" do
      assert {[_id], ack} = Ack.record_ack(Ack.new(), [batch_id(0, 1)])
      assert ack.acked == %{}
    end
  end

  describe "ack sets" do
    test "round trip through the wire encoding, including the sign bit" do
      for size <- [2, 63, 64, 65, 100, 129] do
        outstanding = MapSet.new(Enum.take_every(0..(size - 1), 3))

        assert Ack.outstanding(encode(outstanding, size)) == outstanding
      end
    end

    test "writes a word with its top bit set as a negative int64" do
      # Bits 1..63 outstanding of a 64-message entry.
      assert [word] = encode(MapSet.new(1..63), 64)
      assert word == -2
      assert word >= -0x8000_0000_0000_0000
    end

    test "survives a protobuf round trip, so the broker sees what was meant" do
      outstanding = MapSet.new([0, 63, 64, 99])

      message_id = %{id() | batch_index: -1, batch_size: 100, ack_set: encode(outstanding, 100)}
      decoded = Binary.MessageIdData.decode(Binary.MessageIdData.encode(message_id))

      assert Ack.outstanding(decoded.ack_set) == outstanding
    end

    test "treats every message as outstanding when the entry carries no set" do
      refute Ack.acknowledged?(nil, 7)
    end
  end

  describe "record_nack/2" do
    test "collapses ids to their entry and drops what was counted off" do
      {[], ack} = Ack.record_ack(Ack.new(), [batch_id(0, 3)])
      ack = Ack.record_nack(ack, [batch_id(1, 3)])

      assert ack.acked == %{}
      assert {[%{batch_index: -1, entryId: 42}], ack} = Ack.take_nacked(ack)
      assert Ack.nacked_count(ack) == 0
    end
  end

  ## Helpers

  # The set the broker would be sent once everything outside `outstanding` has been acked.
  defp encode(outstanding, size) do
    acked = Enum.reject(0..(size - 1), &MapSet.member?(outstanding, &1))

    {ids, _ack} =
      Enum.reduce(acked, {[], Ack.new(true)}, fn index, {ids, ack} ->
        {ackable, ack} = Ack.record_ack(ack, [batch_id(index, size)])
        {ids ++ ackable, ack}
      end)

    ids |> List.last() |> Map.fetch!(:ack_set)
  end

  defp id(entry \\ 42) do
    %Binary.MessageIdData{ledgerId: 7, entryId: entry, partition: -1, batch_index: -1}
  end

  defp batch_id(index, size, opts \\ []) do
    %{id(Keyword.get(opts, :entry, 42)) | batch_index: index, batch_size: size}
  end
end
