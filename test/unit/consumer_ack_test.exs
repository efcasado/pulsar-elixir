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

  describe "record_ack/2 when cumulative" do
    setup do: %{ack: Ack.new(ack_type: :cumulative)}

    test "sends only the furthest of the ids it is given", %{ack: ack} do
      {[acked], _ack} = Ack.record_ack(ack, [id(41), id(43), id(42)])

      assert acked.entryId == 43
    end

    test "orders by ledger before entry, so a new ledger wins a lower entry id", %{ack: ack} do
      {[acked], _ack} = Ack.record_ack(ack, [%{id(99) | ledgerId: 7}, %{id(1) | ledgerId: 8}])

      assert {acked.ledgerId, acked.entryId} == {8, 1}
    end

    test "counts nothing off, since one ack covers the messages before it", %{ack: ack} do
      {[_acked], ack} = Ack.record_ack(ack, [batch_id(0, 3)])

      assert ack.acked == %{}
    end

    test "acknowledges a batched message by its entry, along with the messages batched with it",
         %{ack: ack} do
      {[acked], _ack} = Ack.record_ack(ack, [%{batch_id(1, 3) | ack_set: [0b101]}])

      assert {acked.batch_index, acked.batch_size, acked.ack_set} == {-1, nil, []}
    end

    test "sends nothing for an id the cursor has already passed", %{ack: ack} do
      {[_acked], ack} = Ack.record_ack(ack, [id(43)])

      assert {[], ^ack} = Ack.record_ack(ack, [id(42)])
      assert {[], ^ack} = Ack.record_ack(ack, [id(43)])
    end

    test "sends again once an id moves the cursor on", %{ack: ack} do
      {[_acked], ack} = Ack.record_ack(ack, [id(43)])
      {[acked], _ack} = Ack.record_ack(ack, [id(44)])

      assert acked.entryId == 44
    end

    test "has nothing to send for no ids at all", %{ack: ack} do
      assert {[], ^ack} = Ack.record_ack(ack, [])
    end
  end

  describe "entry_id/1" do
    test "leaves an id that names no batch untouched" do
      assert Ack.entry_id(id()) == id()
    end

    test "drops what an ack of the whole entry must not carry" do
      entry = Ack.entry_id(%{batch_id(2, 3) | ack_set: [0b101]})

      assert {entry.batch_index, entry.batch_size, entry.ack_set} == {-1, nil, []}
      assert {entry.ledgerId, entry.entryId} == {7, 42}
    end
  end

  describe "ack sets" do
    test "round trip through the wire encoding, including the sign bit" do
      for size <- [2, 63, 64, 65, 100, 129] do
        owed = Enum.take_every(0..(size - 1), 3)
        decoded = Ack.outstanding(encode(owed, size))

        for index <- 0..(size - 1) do
          assert Ack.deliverable?(decoded, index) == index in owed
        end
      end
    end

    test "writes a word with its top bit set as a negative int64" do
      # Bits 1..63 outstanding of a 64-message entry.
      assert [word] = encode(1..63, 64)
      assert word == -2
      assert word >= -0x8000_0000_0000_0000
    end

    test "survives a protobuf round trip, so the broker sees what was meant" do
      owed = [0, 63, 64, 99]

      message_id = %{id() | batch_index: -1, batch_size: 100, ack_set: encode(owed, 100)}
      decoded = Binary.MessageIdData.decode(Binary.MessageIdData.encode(message_id))

      assert Ack.outstanding(decoded.ack_set) == Ack.outstanding(encode(owed, 100))
      for index <- owed, do: assert(Ack.deliverable?(Ack.outstanding(decoded.ack_set), index))
    end

    test "treats every message as deliverable when the entry carries no set" do
      assert Ack.deliverable?(nil, 7)
    end
  end

  describe "record_nack/2" do
    test "collapses ids to their entry and drops what was counted off" do
      {[], ack} = Ack.record_ack(Ack.new(), [batch_id(0, 3)])
      ack = Ack.record_nack(ack, [batch_id(1, 3)])

      assert ack.acked == %{}
      assert {[%{batch_index: -1, entryId: 42}], ack} = Ack.take_nacked(ack)
      assert {[], _ack} = Ack.take_nacked(ack)
    end

    test "makes the redelivered entry answer for every message again" do
      {[], ack} = Ack.record_ack(Ack.new(), [batch_id(0, 3)])
      {[], ack} = Ack.record_ack(ack, [batch_id(1, 3)])

      ack = Ack.record_nack(ack, [batch_id(1, 3)])

      # Counting on from the first delivery would acknowledge the entry on the redelivery of
      # message 1, before message 2 had been dealt with.
      assert {[], ack} = Ack.record_ack(ack, [batch_id(0, 3)])
      assert {[], ack} = Ack.record_ack(ack, [batch_id(1, 3)])
      assert {[_entry], _ack} = Ack.record_ack(ack, [batch_id(2, 3)])
    end

    test "leaves entries the nacked ids do not belong to alone" do
      {[], ack} = Ack.record_ack(Ack.new(), [batch_id(0, 3)])

      ack = Ack.record_nack(ack, [batch_id(0, 3, entry: 43), id()])

      assert map_size(ack.acked) == 1
    end
  end

  ## Helpers

  # The set the broker would be sent once everything outside `owed` has been acked.
  defp encode(owed, size) do
    acked = Enum.reject(0..(size - 1), &(&1 in owed))

    {ids, _ack} =
      Enum.reduce(acked, {[], Ack.new(batch_index_ack_enabled: true)}, fn index, {ids, ack} ->
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
