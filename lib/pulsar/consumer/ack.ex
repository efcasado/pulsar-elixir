defmodule Pulsar.Consumer.Ack do
  @moduledoc false

  # Tracks what a consumer still owes the broker, and works out which message ids can be sent.
  #
  # An ack names the entry it lands in, so acking one message of a batch acknowledges its
  # siblings unless it carries an `ack_set` — which only a broker with
  # `acknowledgmentAtBatchIndexLevelEnabled` honours.

  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

  # An `ack_set` is `repeated int64` (`MessageIdData` in pulsar.proto), holding a bitset in the
  # layout Java's `BitSet.toLongArray/0` produces: 64 bits per signed word, lowest index first.
  @word_bits 64

  defstruct [{:acked, %{}}, {:nacked, MapSet.new()}, {:batch_index_ack_enabled, false}]

  @type entry_key :: {non_neg_integer(), non_neg_integer(), integer()}
  @type message_id :: Binary.MessageIdData.t()

  @type t :: %__MODULE__{
          acked: %{optional(entry_key()) => MapSet.t(non_neg_integer())},
          nacked: MapSet.t(message_id()),
          batch_index_ack_enabled: boolean()
        }

  @spec new(boolean()) :: t()
  def new(batch_index_ack_enabled \\ false) do
    %__MODULE__{batch_index_ack_enabled: !!batch_index_ack_enabled}
  end

  @doc """
  Records an acknowledgement locally. Returns `{ids_to_send, ledger}`.

  Nothing here reaches the broker: the caller hands over every id its callback acked, sends
  whatever comes back — often nothing — and keeps the updated ledger.

      {to_send, acks} = Ack.record_ack(state.acks, message_ids)

  A message that arrived on its own always comes back:

      iex> alias Pulsar.Consumer.Ack
      iex> alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
      iex> {[id], _acks} = Ack.record_ack(Ack.new(), [%MessageIdData{ledgerId: 7, entryId: 42}])
      iex> {id.entryId, id.batch_index}
      {42, -1}

  One from a batch comes back only once its entry is complete, since an ack names the entry it
  lands in. The id that goes out then names the entry rather than a message in it:

      iex> alias Pulsar.Consumer.Ack
      iex> alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
      iex> in_batch = fn index ->
      ...>   %MessageIdData{ledgerId: 7, entryId: 42, batch_index: index, batch_size: 3}
      ...> end
      iex> {[], acks} = Ack.record_ack(Ack.new(), [in_batch.(0)])
      iex> {[], acks} = Ack.record_ack(acks, [in_batch.(1)])
      iex> {[entry], _acks} = Ack.record_ack(acks, [in_batch.(2)])
      iex> {entry.entryId, entry.batch_index}
      {42, -1}

  With `:batch_index_ack_enabled` every ack goes out instead, carrying the entry's still
  outstanding messages as a bitset:

      iex> alias Pulsar.Consumer.Ack
      iex> alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
      iex> id = %MessageIdData{ledgerId: 7, entryId: 42, batch_index: 0, batch_size: 3}
      iex> {[acked], _acks} = Ack.record_ack(Ack.new(true), [id])
      iex> {acked.ack_set, acked.batch_size}
      {[0b110], 3}
  """
  @spec record_ack(t(), [message_id()]) :: {[message_id()], t()}
  def record_ack(%__MODULE__{} = ack, message_ids) do
    {ackable, new_ack} =
      Enum.reduce(message_ids, {[], ack}, fn message_id, {ackable, acc} ->
        case batch_entry(message_id) do
          nil -> {[entry_id(message_id) | ackable], acc}
          entry -> count_off(acc, entry, message_id, ackable)
        end
      end)

    {Enum.reverse(ackable), new_ack}
  end

  @doc """
  Records a negative acknowledgement locally, to be drained by `take_nacked/1` when the
  redelivery request is due.

  Redelivery is whole entries, so two nacked messages of one batch are one thing to ask for
  again:

      iex> alias Pulsar.Consumer.Ack
      iex> alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
      iex> in_batch = fn index ->
      ...>   %MessageIdData{ledgerId: 7, entryId: 42, batch_index: index, batch_size: 3}
      ...> end
      iex> acks = Ack.record_nack(Ack.new(), [in_batch.(0), in_batch.(2)])
      iex> Ack.nacked_count(acks)
      1
  """
  @spec record_nack(t(), [message_id()]) :: t()
  def record_nack(%__MODULE__{} = ack, message_ids) do
    ack = forget(ack, message_ids)
    %{ack | nacked: MapSet.union(ack.nacked, MapSet.new(message_ids, &entry_id/1))}
  end

  @doc """
  Drops what has been counted off for the entries these ids belong to.

  Redelivery is whole entries, so a nacked message brings its batch back with it: the tally
  would otherwise be left unable to complete.
  """
  @spec forget(t(), [message_id()]) :: t()
  def forget(%__MODULE__{} = ack, message_ids) do
    keys =
      message_ids
      |> Enum.map(&batch_entry/1)
      |> Enum.reject(&is_nil/1)
      |> Enum.map(fn {key, _index, _size} -> key end)

    %{ack | acked: Map.drop(ack.acked, keys)}
  end

  @spec nacked_count(t()) :: non_neg_integer()
  def nacked_count(%__MODULE__{nacked: nacked}), do: MapSet.size(nacked)

  @spec take_nacked(t()) :: {[message_id()], t()}
  def take_nacked(%__MODULE__{nacked: nacked} = ack) do
    {MapSet.to_list(nacked), %{ack | nacked: MapSet.new()}}
  end

  @doc """
  The entry an id belongs to, which is what the broker acknowledges and redelivers.

      iex> alias Pulsar.Consumer.Ack
      iex> alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
      iex> id = %MessageIdData{ledgerId: 7, entryId: 42, batch_index: 2, batch_size: 3}
      iex> entry = Ack.entry_id(id)
      iex> {entry.batch_index, entry.batch_size}
      {-1, nil}
  """
  @spec entry_id(message_id()) :: message_id()
  def entry_id(%{batch_index: index} = message_id) when is_integer(index) and index >= 0 do
    %{message_id | batch_index: -1, batch_size: nil, ack_set: []}
  end

  def entry_id(message_id), do: message_id

  @doc """
  The messages still owed in a redelivered entry, or `nil` when it carries no set and every
  message in it is to be delivered.

  Set bits are the messages outstanding, so a three-message entry with only its middle
  message left owed arrives as `0b010`:

      iex> Pulsar.Consumer.Ack.outstanding([0b010])
      MapSet.new([1])

      iex> Pulsar.Consumer.Ack.outstanding([])
      nil
  """
  @spec outstanding([integer()] | nil) :: MapSet.t(non_neg_integer()) | nil
  def outstanding(ack_set) when ack_set in [nil, []], do: nil
  def outstanding(ack_set), do: decode_ack_set(ack_set)

  @spec acknowledged?(MapSet.t(non_neg_integer()) | nil, non_neg_integer()) :: boolean()
  def acknowledged?(nil, _index), do: false
  def acknowledged?(outstanding, index), do: not MapSet.member?(outstanding, index)

  ## Private

  defp count_off(ack, {key, index, size}, message_id, ackable) do
    acked = ack.acked |> Map.get(key, MapSet.new()) |> MapSet.put(index)

    cond do
      MapSet.size(acked) == size ->
        {[entry_id(message_id) | ackable], %{ack | acked: Map.delete(ack.acked, key)}}

      ack.batch_index_ack_enabled ->
        {[batch_index_ack_id(message_id, acked, size) | ackable], %{ack | acked: Map.put(ack.acked, key, acked)}}

      true ->
        {ackable, %{ack | acked: Map.put(ack.acked, key, acked)}}
    end
  end

  # A batch of one needs no tally: its entry is complete on the single ack it takes.
  defp batch_entry(%{batch_index: index, batch_size: size} = message_id)
       when is_integer(index) and index >= 0 and is_integer(size) and size > 1 do
    {{message_id.ledgerId, message_id.entryId, message_id.partition}, index, size}
  end

  defp batch_entry(_message_id), do: nil

  # `batch_size` is what the broker sizes its own bitset from.
  defp batch_index_ack_id(message_id, acked, size) do
    %{message_id | batch_index: -1, batch_size: size, ack_set: encode_ack_set(acked, size)}
  end

  # Set bits are the messages still outstanding, not the acked ones: the broker starts an entry
  # with every bit set and deletes it once nothing is left.
  defp encode_ack_set(acked, size) do
    words = div(size - 1, @word_bits) + 1

    0..(words - 1)
    |> Enum.map(&ack_set_word(acked, &1, size))
    |> trim_trailing_zeroes()
  end

  defp ack_set_word(acked, word, size) do
    base = word * @word_bits

    bits =
      Enum.reduce(0..(@word_bits - 1), 0, fn bit, acc ->
        index = base + bit

        if index < size and not MapSet.member?(acked, index) do
          Bitwise.bor(acc, Bitwise.bsl(1, bit))
        else
          acc
        end
      end)

    to_signed(bits)
  end

  defp decode_ack_set(words) do
    words
    |> Enum.with_index()
    |> Enum.flat_map(fn {word, word_index} ->
      unsigned = to_unsigned(word)
      base = word_index * @word_bits

      for bit <- 0..(@word_bits - 1), Bitwise.band(unsigned, Bitwise.bsl(1, bit)) != 0, do: base + bit
    end)
    |> MapSet.new()
  end

  defp to_signed(word) do
    <<signed::signed-size(@word_bits)>> = <<word::size(@word_bits)>>
    signed
  end

  defp to_unsigned(word) do
    <<unsigned::size(@word_bits)>> = <<word::signed-size(@word_bits)>>
    unsigned
  end

  defp trim_trailing_zeroes(words) do
    words
    |> Enum.reverse()
    |> Enum.drop_while(&(&1 == 0))
    |> Enum.reverse()
  end
end
