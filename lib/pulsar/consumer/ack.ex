defmodule Pulsar.Consumer.Ack do
  @moduledoc false

  # Tracks what a consumer still owes the broker, and works out which message ids can be sent.
  #
  # An ack names the entry it lands in, so acking one message of a batch acknowledges its
  # siblings unless it carries an `ack_set` — which only a broker with
  # `acknowledgmentAtBatchIndexLevelEnabled` honours.

  import Bitwise

  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

  # An `ack_set` is `repeated int64` (`MessageIdData` in pulsar.proto), holding a bitset in the
  # layout Java's `BitSet.toLongArray/0` produces: 64 bits per signed word, lowest index first.
  @word_bits 64
  @max_uint64 (1 <<< @word_bits) - 1

  defstruct acked: %{},
            nacked: MapSet.new(),
            batch_index_ack_enabled: false,
            ack_type: :individual,
            cumulative_cursor: nil

  @type entry_key :: {non_neg_integer(), non_neg_integer()}

  @typedoc "Messages of an entry, one bit per batch index."
  @type bitset :: non_neg_integer()

  @typedoc """
  How far a cumulative cursor has been moved: an entry, and either a batch index within it or
  `:whole` for the entry in full. `:whole` sorts above every index, so a partial ack of an
  entry never reads as further along than an ack of all of it.
  """
  @type cursor_position :: {non_neg_integer(), integer(), non_neg_integer() | :whole}

  @type message_id :: Binary.MessageIdData.t()

  @type t :: %__MODULE__{
          acked: %{optional(entry_key()) => bitset()},
          nacked: MapSet.t(message_id()),
          batch_index_ack_enabled: boolean(),
          ack_type: :individual | :cumulative,
          cumulative_cursor: cursor_position() | nil
        }

  @type opt :: {:batch_index_ack_enabled, boolean()} | {:ack_type, :individual | :cumulative}

  @spec new([opt()]) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      batch_index_ack_enabled: Keyword.get(opts, :batch_index_ack_enabled, false),
      ack_type: Keyword.get(opts, :ack_type, :individual)
    }
  end

  @doc """
  Records an acknowledgement locally, returning `{ids_to_send, tracker}`.

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

  With `:batch_index_ack_enabled` every ack goes out instead, carrying the entry's still
  outstanding messages as a bitset:

      iex> alias Pulsar.Consumer.Ack
      iex> alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
      iex> id = %MessageIdData{ledgerId: 7, entryId: 42, batch_index: 0, batch_size: 3}
      iex> {[acked], _acks} = Ack.record_ack(Ack.new(batch_index_ack_enabled: true), [id])
      iex> {acked.ack_set, acked.batch_size}
      {[0b110], 3}

  A `:cumulative` tracker counts nothing off, since one ack covers everything up to the message
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

  Without batch-index acknowledgement, stopping part-way through a batch cannot move the cursor
  into the entry. Acknowledging the entry would take the messages batched after this one with it,
  so the entry before is as far as the cursor can go and the batch is redelivered whole:

      iex> alias Pulsar.Consumer.Ack
      iex> alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
      iex> id = %MessageIdData{ledgerId: 7, entryId: 42, batch_index: 0, batch_size: 3}
      iex> {[acked], _acks} = Ack.record_ack(Ack.new(ack_type: :cumulative), [id])
      iex> {acked.entryId, acked.batch_index}
      {41, -1}

  With batch-index acknowledgement, a cumulative ack can stop within the entry. The set carries
  the messages after the target as still outstanding:

      iex> alias Pulsar.Consumer.Ack
      iex> alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
      iex> id = %MessageIdData{ledgerId: 7, entryId: 42, batch_index: 0, batch_size: 3}
      iex> opts = [ack_type: :cumulative, batch_index_ack_enabled: true]
      iex> {[acked], _acks} = Ack.record_ack(Ack.new(opts), [id])
      iex> {acked.entryId, acked.batch_index, acked.ack_set}
      {42, -1, [0b110]}

  Acking the last message of the entry covers it in full, so it goes out whole either way:

      iex> alias Pulsar.Consumer.Ack
      iex> alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
      iex> id = %MessageIdData{ledgerId: 7, entryId: 42, batch_index: 2, batch_size: 3}
      iex> {[acked], _acks} = Ack.record_ack(Ack.new(ack_type: :cumulative), [id])
      iex> {acked.entryId, acked.batch_index, acked.ack_set}
      {42, -1, []}
  """
  @spec record_ack(t(), [message_id()]) :: {[message_id()], t()}
  def record_ack(%__MODULE__{ack_type: :cumulative} = ack, message_ids) do
    # A redelivered batch omits previously acknowledged messages from callback delivery. The
    # worker counts those ids off automatically for individual acknowledgement, but a cumulative
    # ledger must not move merely because the broker reported an id as already acknowledged.
    targets =
      message_ids
      |> Enum.reject(&already_acknowledged?/1)
      |> Enum.map(&cumulative_target(&1, ack))

    case targets do
      [] ->
        {[], ack}

      targets ->
        targets
        |> Enum.max_by(&elem(&1, 0))
        |> maybe_advance_cumulative_cursor(ack)
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
  # A cursor normally names entries, so a message part-way through a batch is the awkward case.
  # With batch-index acknowledgement the ack set can put the cumulative mark inside the entry.
  # Without it, the entry before is as far as the cursor can safely go without acknowledging the
  # messages batched after the target.
  defp cumulative_target(message_id, %{batch_index_ack_enabled: batch_index_ack_enabled}) do
    case batch_entry(message_id) do
      # Not batched, or the last message of its batch: the entry is covered in full.
      nil ->
        {whole_entry_cursor(message_id), entry_id(message_id)}

      {_key, index, size} when index == size - 1 ->
        {whole_entry_cursor(message_id), entry_id(message_id)}

      {_key, index, size} when batch_index_ack_enabled ->
        {cursor_position(message_id), cumulative_batch_index_ack_id(message_id, index, size)}

      {_key, _index, _size} ->
        previous_entry_target(message_id)
    end
  end

  defp previous_entry_target(%Binary.MessageIdData{entryId: 0} = message_id) do
    # Java and Go use entry -1 as the position immediately before a ledger's first entry. The
    # protobuf field is uint64, so its wire representation is the largest uint64; keep -1 only in
    # the logical cursor so it continues to sort before entry zero.
    previous = %{entry_id(message_id) | entryId: @max_uint64}

    {{message_id.ledgerId, -1, :whole}, previous}
  end

  defp previous_entry_target(%Binary.MessageIdData{} = message_id) do
    previous = %{entry_id(message_id) | entryId: message_id.entryId - 1}

    {whole_entry_cursor(previous), previous}
  end

  defp maybe_advance_cumulative_cursor({position, message_id}, %{cumulative_cursor: cursor} = ack)
       when is_nil(cursor) or position > cursor, do: {[message_id], %{ack | cumulative_cursor: position}}

  defp maybe_advance_cumulative_cursor(_target, ack), do: {[], ack}

  # An entry acknowledged whole outranks any message within it, which `:whole` sorting above
  # every integer index gives for free.
  defp whole_entry_cursor(%Binary.MessageIdData{} = message_id), do: {message_id.ledgerId, message_id.entryId, :whole}

  defp cursor_position(%Binary.MessageIdData{} = message_id) do
    case batch_entry(message_id) do
      nil -> whole_entry_cursor(message_id)
      {_key, index, _size} -> {message_id.ledgerId, message_id.entryId, index}
    end
  end

  defp already_acknowledged?(message_id) do
    case {batch_entry(message_id), outstanding(message_id.ack_set)} do
      {{_key, index, _size}, outstanding} when is_integer(outstanding) ->
        not deliverable?(outstanding, index)

      _ ->
        false
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

          ack.batch_index_ack_enabled ->
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
    outstanding = bnot(acked) &&& every_message(size)
    ack_set_id(message_id, outstanding, size)
  end

  # A cumulative batch-index ack clears the whole prefix through `index`, while retaining any
  # messages the broker already reported as acknowledged on a redelivered entry.
  defp cumulative_batch_index_ack_id(message_id, index, size) do
    outstanding = outstanding(message_id.ack_set) || every_message(size)
    outstanding = outstanding &&& bnot(every_message(index + 1)) &&& every_message(size)

    ack_set_id(message_id, outstanding, size)
  end

  defp ack_set_id(message_id, outstanding, size) do
    %{message_id | batch_index: -1, batch_size: size, ack_set: encode_bitset(outstanding, size)}
  end

  # Set bits are the messages still outstanding: the broker starts an entry with every bit set
  # and deletes it once nothing is left.
  defp encode_bitset(outstanding, size) do
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
