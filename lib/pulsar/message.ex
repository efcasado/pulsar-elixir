defmodule Pulsar.Message do
  @moduledoc """
  Represents a message received from a Pulsar topic.

  This struct encapsulates all information about a message delivered to a consumer callback.

  ## Fields

  - `payload` - The message payload as a binary. For a chunked message, the assembled
    complete payload.

  - `message_id` - What to pass to `Pulsar.Consumer.ack/2` and `Pulsar.Consumer.nack/2`.
    Treat it as opaque rather than pattern matching it: it carries a batch index for a
    batched message, and for a chunked one it stands for every chunk, so it is a list there.

  - `chunk_metadata` - Metadata about chunked messages (`nil` for non-chunked messages).
    For complete chunked messages: `%{chunked: true, complete: true, uuid: "...", num_chunks: N}`
    For incomplete chunked messages: `%{chunked: true, complete: false, error: :reason, uuid: "..."}`

  - `validation_error` - `nil` for messages that arrived intact. Otherwise why `payload` cannot
    be treated as data: the frame could not be read, or it could and its payload did not
    decompress. See `valid?/1`.

  - `raw` - The underlying protocol structs, as a map of `:command`, `:metadata`,
    `:single_metadata` and `:broker_metadata`. **Unstable**: its shape follows the wire
    protocol and changes with how the broker delivered the message. Use the accessors below
    instead; reach for `raw` only for protocol details they do not cover.

  ## Reading a message

  Everything a callback normally needs has an accessor that answers the same way whether the
  message arrived on its own, inside a batch, or split across chunks:

  | Accessor | Returns |
  | --- | --- |
  | `producer_name/1` | The producer that published it |
  | `publish_time/1` | Broker publish timestamp, in milliseconds |
  | `event_time/1` | Application-set event time, or `nil` when unset |
  | `key/1` | The partition key, or `nil` |
  | `ordering_key/1` | The ordering key, or `nil` |
  | `properties/1` | User properties as a map |
  | `redelivery_count/1` | How many times the broker has redelivered it |
  | `message_id_string/1` | Its id as Pulsar prints it, for logging and correlation |

  This matters because the same datum lives in different places depending on delivery: a
  batched message carries its key and properties per message, a non-batched one carries them
  in the message metadata, and a chunked one has a list of both. The accessors resolve that;
  reading `raw` does not.

  ## Usage

      def handle_message(%Pulsar.Message{payload: payload}, state) do
        process(payload)
        {:ok, state}
      end

      def handle_message(%Pulsar.Message{} = message, state) do
        Logger.info("from \#{Pulsar.Message.producer_name(message)}, key \#{Pulsar.Message.key(message)}")
        {:ok, state}
      end

  Manual acknowledgement, where the id is captured for use after the callback returns:

      def handle_message(%Pulsar.Message{message_id: message_id} = message, state) do
        consumer = self()

        spawn(fn ->
          case process_async(message) do
            :ok -> Pulsar.Consumer.ack(consumer, message_id)
            {:error, _reason} -> Pulsar.Consumer.nack(consumer, message_id)
          end
        end)

        {:noreply, state}
      end
  """

  @typedoc """
  The protocol structs a message was built from. Unstable; see the `:raw` field.
  """
  @type raw :: %{
          command: struct() | [struct()],
          metadata: struct() | [struct()] | nil,
          single_metadata: struct() | [struct()] | nil,
          broker_metadata: term() | [term()]
        }

  @type t :: %__MODULE__{
          payload: binary(),
          message_id: term() | [term()],
          chunk_metadata: map() | nil,
          validation_error: atom() | nil,
          raw: raw() | nil
        }

  defstruct [
    :payload,
    :message_id,
    :chunk_metadata,
    :validation_error,
    :raw
  ]

  @doc """
  Returns the name of the producer that published the message.

  ## Examples

      iex> raw = %{metadata: %{producer_name: "orders-api"}}
      iex> Pulsar.Message.producer_name(%Pulsar.Message{raw: raw})
      "orders-api"
  """
  @spec producer_name(t()) :: String.t() | nil
  def producer_name(%__MODULE__{} = message), do: message |> metadata() |> field(:producer_name)

  @doc """
  Returns the time the broker published the message, in milliseconds since the epoch.
  """
  @spec publish_time(t()) :: non_neg_integer() | nil
  def publish_time(%__MODULE__{} = message), do: message |> metadata() |> field(:publish_time)

  @doc """
  Returns the event time the publishing application set, or `nil` when it set none.

  Pulsar represents an unset event time as `0`, which this reports as `nil`.

  ## Examples

      iex> raw = %{metadata: %{event_time: 0}}
      iex> Pulsar.Message.event_time(%Pulsar.Message{raw: raw})
      nil
  """
  @spec event_time(t()) :: non_neg_integer() | nil
  def event_time(%__MODULE__{} = message) do
    case per_message(message, :event_time) do
      0 -> nil
      event_time -> event_time
    end
  end

  @doc """
  Returns the message's partition key, or `nil` when it has none.

  A batched message carries its own key, so this reads that one and falls back to the key on
  the entry it arrived in.

  ## Examples

      iex> raw = %{metadata: %{partition_key: "entry"}, single_metadata: %{partition_key: "message"}}
      iex> Pulsar.Message.key(%Pulsar.Message{raw: raw})
      "message"
  """
  @spec key(t()) :: String.t() | nil
  def key(%__MODULE__{} = message), do: per_message(message, :partition_key)

  @doc """
  Returns the message's ordering key, or `nil` when it has none.
  """
  @spec ordering_key(t()) :: binary() | nil
  def ordering_key(%__MODULE__{} = message), do: per_message(message, :ordering_key)

  @doc """
  Returns the user properties published with the message, as a map.

  ## Examples

      iex> raw = %{metadata: %{properties: [%{key: "trace-id", value: "abc"}]}}
      iex> Pulsar.Message.properties(%Pulsar.Message{raw: raw})
      %{"trace-id" => "abc"}

      iex> Pulsar.Message.properties(%Pulsar.Message{raw: %{metadata: nil}})
      %{}
  """
  @spec properties(t()) :: %{String.t() => String.t()}
  def properties(%__MODULE__{} = message) do
    message
    |> per_message(:properties)
    |> List.wrap()
    |> Map.new(&{&1.key, &1.value})
  end

  @doc """
  Returns the maximum redelivery count across all commands.

  For chunked messages, returns the maximum redelivery count from all chunks.
  For non-chunked messages, returns the redelivery count from the single command.

  ## Examples

      iex> Pulsar.Message.redelivery_count(%Pulsar.Message{raw: %{command: %{redelivery_count: 3}}})
      3

      iex> chunks = [%{redelivery_count: 1}, %{redelivery_count: 3}]
      iex> Pulsar.Message.redelivery_count(%Pulsar.Message{raw: %{command: chunks}})
      3
  """
  @spec redelivery_count(t()) :: non_neg_integer()
  def redelivery_count(%__MODULE__{raw: %{command: command}}) when is_list(command) do
    command
    |> Enum.map(& &1.redelivery_count)
    |> Enum.max(fn -> 0 end)
  end

  def redelivery_count(%__MODULE__{raw: %{command: command}}), do: command.redelivery_count

  @doc """
  Returns the message's id as Pulsar prints it, or `nil` when it has none.

  The shape is `ledgerId:entryId:partition`, with a batched message's index within its entry
  appended, which is what the Java client's `MessageId.toString()` produces. It is what to log
  or carry when a message has to be correlated with one seen elsewhere; `message_id` itself
  stays opaque.

  A chunked message answers for the chunk it began at.

  ## Examples

      iex> id = %{ledgerId: 7, entryId: 42, partition: -1, batch_index: -1}
      iex> Pulsar.Message.message_id_string(%Pulsar.Message{message_id: id})
      "7:42:-1"

  A batched message appends its index, so two messages of one batch stay distinguishable:

      iex> id = %{ledgerId: 7, entryId: 42, partition: 3, batch_index: 1}
      iex> Pulsar.Message.message_id_string(%Pulsar.Message{message_id: id})
      "7:42:3:1"
  """
  @spec message_id_string(t()) :: String.t() | nil
  def message_id_string(%__MODULE__{message_id: message_id}), do: format_message_id(message_id)

  # A chunked message carries one id per chunk; the first is where it began.
  defp format_message_id([message_id | _rest]), do: format_message_id(message_id)
  defp format_message_id([]), do: nil
  defp format_message_id(nil), do: nil

  defp format_message_id(%{ledgerId: ledger_id, entryId: entry_id, partition: partition} = message_id) do
    base = "#{ledger_id}:#{entry_id}:#{partition}"

    case Map.get(message_id, :batch_index) do
      index when is_integer(index) and index >= 0 -> "#{base}:#{index}"
      _not_batched -> base
    end
  end

  # A batched message carries its own key, properties and times; one delivered on its own carries
  # them in the entry's metadata. Chunks of one message all repeat the same values.
  #
  # A message that set nothing decodes to whatever proto2 renders for that field kind: nil for
  # an optional with no default, 0 for one with a numeric default, [] for a repeated field. All
  # three mean "not set here, ask the entry", as they do in the Java client.
  defp per_message(message, key) do
    case message |> single_metadata() |> field(key) do
      unset when unset in [nil, 0, []] -> message |> metadata() |> field(key)
      value -> value
    end
  end

  defp metadata(%__MODULE__{raw: %{metadata: metadata}}), do: first(metadata)
  defp metadata(%__MODULE__{}), do: nil

  defp single_metadata(%__MODULE__{raw: %{single_metadata: single_metadata}}), do: first(single_metadata)
  defp single_metadata(%__MODULE__{}), do: nil

  defp first(values) when is_list(values), do: List.first(values)
  defp first(value), do: value

  defp field(nil, _key), do: nil
  defp field(metadata, key), do: Map.get(metadata, key)

  @doc """
  Returns the number of broker messages (permits) consumed.

  For ordinary non-chunked messages, this is 1. An invalid batched message still reports the
  batch count when its payload failed validation after the metadata was decoded.
  For chunked messages, this is the number of chunks actually received.

  This is used for flow control permit accounting.

  ## Examples

      iex> Pulsar.Message.num_broker_messages(%Pulsar.Message{payload: "one"})
      1

      iex> three_chunks = %{chunked: true, complete: true, message_ids: [1, 2, 3]}
      iex> Pulsar.Message.num_broker_messages(%Pulsar.Message{chunk_metadata: three_chunks})
      3

  Two chunks of three, given up on, still cost the two permits the broker charged:

      iex> expired = %{chunked: true, complete: false, message_ids: [1, 2]}
      iex> Pulsar.Message.num_broker_messages(%Pulsar.Message{chunk_metadata: expired})
      2

  A batch that could not be decompressed still knows how many messages it carried:

      iex> raw = %{metadata: %{num_messages_in_batch: 5}}
      iex> undecompressable = %Pulsar.Message{validation_error: :decompression_failed, raw: raw}
      iex> Pulsar.Message.num_broker_messages(undecompressable)
      5
  """
  @spec num_broker_messages(t()) :: pos_integer()
  def num_broker_messages(%__MODULE__{chunk_metadata: %{message_ids: ids}}) when is_list(ids) do
    length(ids)
  end

  # A payload that failed after its metadata was decoded keeps its batch count.
  def num_broker_messages(%__MODULE__{
        validation_error: :decompression_failed,
        raw: %{metadata: %{num_messages_in_batch: count}}
      })
      when is_integer(count) and count > 0 do
    count
  end

  def num_broker_messages(%__MODULE__{
        validation_error: :uncompressed_size_corruption,
        raw: %{metadata: %{num_messages_in_batch: count}}
      })
      when is_integer(count) and count > 0 do
    count
  end

  # Any other invalid frame had its batch count in the metadata that failed validation, and
  # CommandMessage does not carry it, so 1 is the most that can be assumed. A
  # corrupt batch therefore under-counts what the broker charged for it: enough of
  # them and outstanding permits never reach the refill threshold, and the consumer
  # stops being sent messages. The reference client credits 1 for the same reason.
  #
  # https://github.com/apache/pulsar/blob/v4.2.3/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerImpl.java#L2165-L2179
  def num_broker_messages(%__MODULE__{validation_error: error}) when not is_nil(error), do: 1

  def num_broker_messages(%__MODULE__{}), do: 1

  @doc """
  Returns `true` if the message is a chunked message, `false` otherwise.

  This checks for the presence of chunk metadata.

  ## Examples

      iex> Pulsar.Message.chunked?(%Pulsar.Message{chunk_metadata: %{chunked: true}})
      true

      iex> Pulsar.Message.chunked?(%Pulsar.Message{payload: "one"})
      false
  """
  @spec chunked?(t()) :: boolean()
  def chunked?(%__MODULE__{chunk_metadata: %{chunked: true}}), do: true
  def chunked?(%__MODULE__{}), do: false

  @doc """
  Returns `true` if the chunked message is complete, `false` otherwise.

  For non-chunked messages, always returns `true` since they are inherently complete.
  For chunked messages, returns `true` only if all chunks were successfully received.

  ## Examples

      iex> Pulsar.Message.complete?(%Pulsar.Message{chunk_metadata: %{chunked: true, complete: true}})
      true

      iex> Pulsar.Message.complete?(%Pulsar.Message{chunk_metadata: %{chunked: true, complete: false}})
      false

      iex> Pulsar.Message.complete?(%Pulsar.Message{payload: "one"})
      true
  """
  @spec complete?(t()) :: boolean()
  def complete?(%__MODULE__{chunk_metadata: %{complete: complete}}), do: complete
  def complete?(%__MODULE__{}), do: true

  @doc """
  Returns `true` if the message arrived intact, `false` if it did not.

  A message is invalid when its bytes could not be turned into a payload, and
  `validation_error` says why:

  - The frame itself could not be read: `:checksum_mismatch` when it failed its CRC32C check,
    `:malformed_frame`, `:malformed_message_metadata` or `:malformed_broker_entry_metadata`
    when its framing did not hold. Its `metadata` is `nil` and its `payload` is the bytes the
    framing points at, or the whole message section when even that does not hold.
  - `:decompression_failed` - the frame was intact and its metadata readable, but its payload
    could not be turned back into the message that was sent. `metadata` is kept and `payload`
    holds the bytes as they arrived, still compressed.
  - `:uncompressed_size_corruption` - the decoded or reassembled payload did not match the
    size advertised by the producer. `metadata` is kept and `payload` holds the bytes as they
    arrived, still compressed when compression was enabled.

  Any of them is delivered so the callback can record or divert it, but no such payload may be
  treated as data. Match `validation_error` with a catch-all: more reasons may be added.

  Messages that fail validation are routed to `c:Pulsar.Consumer.Callback.handle_invalid_message/2`,
  so `handle_message/2` never receives one and rarely needs this check.

  ## Examples

      iex> Pulsar.Message.valid?(%Pulsar.Message{payload: "hello"})
      true

      iex> Pulsar.Message.valid?(%Pulsar.Message{validation_error: :checksum_mismatch})
      false

  Validity is independent of chunk completeness — an incomplete chunked message is
  still made of bytes that arrived intact:

      iex> expired = %Pulsar.Message{chunk_metadata: %{chunked: true, complete: false}}
      iex> {Pulsar.Message.valid?(expired), Pulsar.Message.complete?(expired)}
      {true, false}

      def handle_invalid_message(%Pulsar.Message{} = message, state) do
        Logger.error("dropping corrupt message: \#{message.validation_error}")
        {:ok, state}
      end
  """
  @spec valid?(t()) :: boolean()
  def valid?(%__MODULE__{validation_error: nil}), do: true
  def valid?(%__MODULE__{}), do: false
end
