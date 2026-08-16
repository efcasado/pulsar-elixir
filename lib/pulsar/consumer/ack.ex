defmodule Pulsar.Consumer.Ack do
  @moduledoc false

  # Tracks what a consumer still owes the broker, and works out which message ids can be sent.
  #
  # An ack names the entry it lands in, so acking one message of a batch acknowledges its
  # siblings unless it carries an `ack_set` — which only a broker with
  # `acknowledgmentAtBatchIndexLevelEnabled` honours. `:ack_type` picks between that, counting
  # a batch off locally, and moving a cursor.

  import Bitwise

  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

  # An `ack_set` is `repeated int64` (`MessageIdData` in pulsar.proto), holding a bitset in the
  # layout Java's `BitSet.toLongArray/0` produces: 64 bits per signed word, lowest index first.
  @word_bits 64

  defstruct acked: %{}, nacked: MapSet.new(), ack_type: :individual, cumulative: nil

  @type entry_key :: {non_neg_integer(), non_neg_integer()}

  @typedoc "Messages of an entry, one bit per batch index."
  @type bitset :: non_neg_integer()

  @typedoc """
  How far a cumulative cursor has been moved: an entry, and either a batch index within it or
  `:whole` for the entry in full. `:whole` sorts above every index, so a partial ack of an
  entry never reads as further along than an ack of all of it.
  """
  @type cursor :: {non_neg_integer(), integer(), non_neg_integer() | :whole}

  @type ack_type :: :individual | :batch_index | :cumulative

  @type message_id :: Binary.MessageIdData.t()

  @type t :: %__MODULE__{
          acked: %{optional(entry_key()) => bitset()},
          nacked: MapSet.t(message_id()),
          ack_type: ack_type(),
          cumulative: cursor() | nil
        }

  @type opt :: {:ack_type, ack_type()}

  @spec new([opt()]) :: t()
  def new(opts \\ []) do
    %__MODULE__{ack_type: Keyword.get(opts, :ack_type, :individual)}
  end

  @doc """
  Records an acknowledgement locally, returning `{ids_to_send, ledger}`.

  Nothing here reaches the broker: the caller sends whatever comes back, which for a batched
  message is usually nothing.

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

  Under `:batch_index` every ack goes out instead, carrying the entry's still outstanding
  messages as a bitset:

      iex> alias Pulsar.Consumer.Ack
      iex> alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
      iex> id = %MessageIdData{ledgerId: 7, entryId: 42, batch_index: 0, batch_size: 3}
      iex> {[acked], _acks} = Ack.record_ack(Ack.new(ack_type: :batch_index), [id])
      iex> {acked.ack_set, acked.batch_size}
      {[0b110], 3}

  A `:cumulative` ledger counts nothing off, since one ack covers everything up to the message
  it names. Only the furthest id it is given goes out, and it names that message's entry:

      iex> alias Pulsar.Consumer.Ack
      iex> alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
      iex> ids = for entry <- [42, 41], do: %MessageIdData{ledgerId: 7, entryId: entry}
      iex> {[id], _acks} = Ack.record_ack(Ack.new(ack_type: :cumulative), ids)
      iex> {id.entryId, id.batch_index}
      {42, -1}

  An id no further along than one already acknowledged is dropped rather than sent, since the
  broker only ever moves the cursor forwards:

      iex> alias Pulsar.Consumer.Ack
      iex> alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
      iex> at = fn entry -> %MessageIdData{ledgerId: 7, entryId: entry} end
      iex> {[_id], acks} = Ack.record_ack(Ack.new(ack_type: :cumulative), [at.(42)])
      iex> Ack.record_ack(acks, [at.(41)])
      {[], acks}

  Stopping part-way through a batch is the hard case, since a cursor names entries.
  Acknowledging the entry would take the messages batched after this one with it, so the entry
  before is as far as the cursor can go and the batch is redelivered whole:

      iex> alias Pulsar.Consumer.Ack
      iex> alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
      iex> id = %MessageIdData{ledgerId: 7, entryId: 42, batch_index: 0, batch_size: 3}
      iex> {[acked], _acks} = Ack.record_ack(Ack.new(ack_type: :cumulative), [id])
      iex> {acked.entryId, acked.batch_index}
      {41, -1}

  Acking the last message of the entry covers it in full, so it goes out whole either way:

      iex> alias Pulsar.Consumer.Ack
      iex> alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
      iex> id = %MessageIdData{ledgerId: 7, entryId: 42, batch_index: 2, batch_size: 3}
      iex> {[acked], _acks} = Ack.record_ack(Ack.new(ack_type: :cumulative), [id])
      iex> {acked.entryId, acked.batch_index, acked.ack_set}
      {42, -1, []}
  """
  @spec record_ack(t(), [message_id()]) :: {[message_id()], t()}
  def record_ack(%__MODULE__{ack_type: :cumulative} = ack, []), do: {[], ack}

  def record_ack(%__MODULE__{ack_type: :cumulative} = ack, message_ids) do
    {position, message_id} =
      message_ids
      |> Enum.max_by(&position/1)
      |> cumulative_target()

    if ack.cumulative == nil or position > ack.cumulative do
      {[message_id], %{ack | cumulative: position}}
    else
      {[], ack}
    end
  end

  def record_ack(%__MODULE__{} = ack, message_ids) do
    {to_send, new_ack} =
      Enum.reduce(message_ids, {[], ack}, fn message_id, {to_send, acc} ->
        case count_off(acc, message_id) do
          {nil, acc} -> {to_send, acc}
          {id, acc} -> {[id | to_send], acc}
        end
      end)

    {Enum.reverse(to_send), new_ack}
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
      iex> {ids, _acks} = Ack.take_nacked(acks)
      iex> length(ids)
      1
  """
  @spec record_nack(t(), [message_id()]) :: t()
  def record_nack(%__MODULE__{} = ack, message_ids) do
    ack = forget(ack, message_ids)
    %{ack | nacked: MapSet.union(ack.nacked, MapSet.new(message_ids, &entry_id/1))}
  end

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
  def entry_id(%Binary.MessageIdData{batch_index: index} = message_id) when is_integer(index) and index >= 0 do
    %{message_id | batch_index: -1, batch_size: nil, ack_set: []}
  end

  def entry_id(message_id), do: message_id

  @doc """
  The messages still owed in a redelivered entry, or `nil` when it carries none and every
  message in it is to be delivered.

  A partly acknowledged entry is redelivered whole, so pair this with `deliverable?/2` to skip
  the messages that have already been acked:

      iex> alias Pulsar.Consumer.Ack
      iex> owed = Ack.outstanding([0b010])
      iex> {Ack.deliverable?(owed, 0), Ack.deliverable?(owed, 1)}
      {false, true}

      iex> alias Pulsar.Consumer.Ack
      iex> Ack.deliverable?(Ack.outstanding([]), 7)
      true
  """
  @spec outstanding([integer()] | nil) :: bitset() | nil
  def outstanding(ack_set) when ack_set in [nil, []], do: nil
  def outstanding(words), do: decode_ack_set(words)

  @spec deliverable?(bitset() | nil, non_neg_integer()) :: boolean()
  def deliverable?(nil, _index), do: true
  def deliverable?(outstanding, index), do: (outstanding &&& 1 <<< index) != 0

  ## Private

  # How far a cumulative ack of `message_id` can move the cursor, and the id that says so.
  #
  # A cursor names entries, so a message part-way through a batch is the awkward case: the entry
  # cannot be acknowledged without taking the messages batched after it, which may be deferred,
  # nacked, or still being processed. An `ack_set` is no help — the broker honours one on an
  # individual ack but not on a cumulative one, where it acknowledges the entry regardless — so
  # the entry before is as far as the cursor can go, and the batch is redelivered whole.
  defp cumulative_target(message_id) do
    case batch_entry(message_id) do
      # Not batched, or the last message of its batch: the entry is covered in full.
      nil ->
        {whole_entry(message_id), entry_id(message_id)}

      {_key, index, size} when index == size - 1 ->
        {whole_entry(message_id), entry_id(message_id)}

      {_key, _index, _size} ->
        previous = %{entry_id(message_id) | entryId: message_id.entryId - 1}

        {whole_entry(previous), previous}
    end
  end

  # An entry acknowledged whole outranks any message within it, which `:whole` sorting above
  # every integer index gives for free.
  defp whole_entry(%Binary.MessageIdData{} = message_id), do: {message_id.ledgerId, message_id.entryId, :whole}

  defp position(%Binary.MessageIdData{} = message_id) do
    case batch_entry(message_id) do
      nil -> whole_entry(message_id)
      {_key, index, _size} -> {message_id.ledgerId, message_id.entryId, index}
    end
  end

  # A redelivered entry has to be dealt with in full again before it can be acknowledged, so
  # what it counted off before is dropped rather than counted on from.
  defp forget(ack, message_ids) do
    keys =
      message_ids
      |> Enum.map(&batch_entry/1)
      |> Enum.reject(&is_nil/1)
      |> Enum.map(fn {key, _index, _size} -> key end)

    %{ack | acked: Map.drop(ack.acked, keys)}
  end

  defp count_off(ack, message_id) do
    case batch_entry(message_id) do
      nil ->
        {entry_id(message_id), ack}

      {key, index, size} ->
        acked = acked_with(ack, key, index)

        cond do
          # Only real acks and messages the broker will not deliver again reach the width, so
          # there is nothing left in the entry for this to acknowledge unread.
          acked == every_message(size) ->
            {entry_id(message_id), %{ack | acked: Map.delete(ack.acked, key)}}

          ack.ack_type == :batch_index ->
            {batch_index_ack_id(message_id, acked, size), %{ack | acked: Map.put(ack.acked, key, acked)}}

          true ->
            {nil, %{ack | acked: Map.put(ack.acked, key, acked)}}
        end
    end
  end

  # A batch of one needs no tally: its entry is complete on the single ack it takes.
  defp batch_entry(%Binary.MessageIdData{batch_index: index, batch_size: size} = message_id)
       when is_integer(index) and index >= 0 and is_integer(size) and size > 1 do
    {{message_id.ledgerId, message_id.entryId}, index, size}
  end

  defp batch_entry(_message_id), do: nil

  # `batch_size` is what the broker sizes its own bitset from.
  defp batch_index_ack_id(message_id, acked, size) do
    %{message_id | batch_index: -1, batch_size: size, ack_set: encode_ack_set(acked, size)}
  end

  # Set bits are the messages still outstanding, not the acked ones: the broker starts an entry
  # with every bit set and deletes it once nothing is left.
  defp encode_ack_set(acked, size) do
    outstanding = bnot(acked) &&& every_message(size)
    bits = word_count(size) * @word_bits

    for <<(word::little-signed-size(@word_bits) <- <<outstanding::little-size(bits)>>)>>, do: word
  end

  defp decode_ack_set(ack_set) do
    binary = for word <- ack_set, into: <<>>, do: <<word::little-signed-size(@word_bits)>>
    bits = bit_size(binary)
    <<outstanding::little-size(^bits)>> = binary
    outstanding
  end

  defp acked_with(ack, key, index), do: Map.get(ack.acked, key, 0) ||| 1 <<< index

  defp every_message(size), do: (1 <<< size) - 1

  defp word_count(size), do: div(size - 1, @word_bits) + 1
end
