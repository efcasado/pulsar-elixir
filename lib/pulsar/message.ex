defmodule Pulsar.Message do
  @moduledoc """
  Represents a message received from a Pulsar topic.

  This struct encapsulates all information about a message delivered to a consumer callback.

  ## Fields

  - `command` - For non-chunked messages: single command struct. For chunked messages: list of
    commands from all chunks.
    Type: `struct() | [struct()]`

  - `metadata` - For non-chunked messages: single metadata struct. For chunked messages: list of
    metadata from all chunks. `nil` for invalid messages, whose metadata is either
    unreadable or not trustworthy.
    Type: `struct() | [struct()] | nil`

  - `payload` - The actual message payload as a binary. For chunked messages, this is the
    assembled complete payload.

  - `single_metadata` - For non-batch messages: nil. For batched messages: single message metadata.
    For chunked messages: list of metadata from all chunks.
    Type: `nil | struct() | [struct()]`

  - `broker_metadata` - For non-chunked messages: single broker metadata. For chunked messages:
    list of broker metadata from all chunks.
    Type: `term() | [term()]`

  - `message_id_to_ack` - For non-chunked messages: single message ID. For batch messages: message
    ID with batch_index. For chunked messages: list of all chunk message IDs.
    Type: `term() | [term()]`

  - `chunk_metadata` - Metadata about chunked messages (nil for non-chunked messages).
    For complete chunked messages: `%{chunked: true, complete: true, uuid: "...", num_chunks: N}`
    For incomplete chunked messages: `%{chunked: true, complete: false, error: :reason, uuid: "..."}`

  - `validation_error` - `nil` for messages that arrived intact. Otherwise why the
    frame could not be trusted, in which case `payload` holds unverified bytes and
    `metadata` is `nil`. See `valid?/1`.
    Type: `atom() | nil`

  ## Usage

  Messages are received in the `handle_message/2` callback:

      def handle_message(%Pulsar.Message{} = message, state) do
        # Access fields directly
        payload = message.payload

        {:ok, state}
      end

  ## Pattern Matching Examples

      # Match only the payload
      def handle_message(%Pulsar.Message{payload: payload}, state) do
        process(payload)
        {:ok, state}
      end

      # Access all fields via the struct (non-chunked)
      def handle_message(%Pulsar.Message{} = msg, state) do
        redelivery_count = Pulsar.Message.redelivery_count(msg)
        producer = msg.metadata.producer_name
        {:ok, state}
      end

      # Manual acknowledgment using message_id_to_ack
      def handle_message(%Pulsar.Message{message_id_to_ack: ack_id} = msg, state) do
        spawn(fn ->
          case process_async(msg) do
            :ok -> Pulsar.Consumer.ack(self(), ack_id)
            {:error, _} -> Pulsar.Consumer.nack(self(), ack_id)
          end
        end)
        {:noreply, state}
      end
  """

  @type t :: %__MODULE__{
          command: struct() | [struct()],
          metadata: struct() | [struct()] | nil,
          payload: binary(),
          single_metadata: struct() | nil | [struct()],
          broker_metadata: term() | [term()],
          message_id_to_ack: term() | [term()],
          chunk_metadata: map() | nil,
          validation_error: atom() | nil
        }

  defstruct [
    :command,
    :metadata,
    :payload,
    :single_metadata,
    :broker_metadata,
    :message_id_to_ack,
    :chunk_metadata,
    :validation_error
  ]

  @doc """
  Returns the maximum redelivery count across all commands.

  For chunked messages, returns the maximum redelivery count from all chunks.
  For non-chunked messages, returns the redelivery count from the single command.

  ## Examples

      iex> Pulsar.Message.redelivery_count(%Pulsar.Message{command: %{redelivery_count: 3}})
      3

      iex> chunks = [%{redelivery_count: 1}, %{redelivery_count: 3}]
      iex> Pulsar.Message.redelivery_count(%Pulsar.Message{command: chunks})
      3
  """
  @spec redelivery_count(t()) :: non_neg_integer()
  def redelivery_count(%__MODULE__{command: command}) when is_list(command) do
    command
    |> Enum.map(& &1.redelivery_count)
    |> Enum.max(fn -> 0 end)
  end

  def redelivery_count(%__MODULE__{command: command}) do
    command.redelivery_count
  end

  @doc """
  Returns the number of broker messages (permits) consumed.

  For non-chunked messages, this is always 1.
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
  """
  @spec num_broker_messages(t()) :: pos_integer()
  def num_broker_messages(%__MODULE__{chunk_metadata: %{message_ids: ids}}) when is_list(ids) do
    length(ids)
  end

  # An invalid frame's batch count was in the metadata that failed validation, and
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

  An invalid message failed its CRC32C check or carried metadata that could not be
  read, so its `metadata` is `nil` and its `payload` is unverified: the bytes the
  framing points at, or the whole message section when even that does not hold. It
  is delivered so the callback can record or divert it, but the payload must not be
  treated as data. `validation_error` says what went wrong.

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
