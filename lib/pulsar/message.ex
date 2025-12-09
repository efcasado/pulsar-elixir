defmodule Pulsar.Message do
  @moduledoc """
  Represents a message received from a Pulsar topic.

  This struct encapsulates all information about a message delivered to a consumer callback.

  ## Fields

  - `command` - For non-chunked messages: single command struct. For chunked messages: list of
    commands from all chunks.
    Type: `struct() | [struct()]`

  - `metadata` - For non-chunked messages: single metadata struct. For chunked messages: list of
    metadata from all chunks.
    Type: `struct() | [struct()]`

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
          metadata: struct() | [struct()],
          payload: binary(),
          single_metadata: struct() | nil | [struct()],
          broker_metadata: term() | [term()],
          message_id_to_ack: term() | [term()],
          chunk_metadata: map() | nil
        }

  defstruct [
    :command,
    :metadata,
    :payload,
    :single_metadata,
    :broker_metadata,
    :message_id_to_ack,
    :chunk_metadata
  ]

  @doc """
  Returns the maximum redelivery count across all commands.

  For chunked messages, returns the maximum redelivery count from all chunks.
  For non-chunked messages, returns the redelivery count from the single command.

  ## Examples

      iex> Pulsar.Message.redelivery_count(message)
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

      iex> Pulsar.Message.num_broker_messages(non_chunked_message)
      1

      iex> Pulsar.Message.num_broker_messages(complete_chunked_message)
      3  # if message had 3 chunks

      iex> Pulsar.Message.num_broker_messages(incomplete_chunked_message)
      2  # if only 2 out of 3 chunks were received before timeout
  """
  @spec num_broker_messages(t()) :: pos_integer()
  def num_broker_messages(%__MODULE__{chunk_metadata: %{message_ids: ids}}) when is_list(ids) do
    length(ids)
  end

  def num_broker_messages(%__MODULE__{}), do: 1

  @doc """
  Returns `true` if the message is a chunked message, `false` otherwise.

  This checks for the presence of chunk metadata.

  ## Examples

      iex> Pulsar.Message.chunked?(message)
      true

      iex> Pulsar.Message.chunked?(non_chunked_message)
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

      iex> Pulsar.Message.complete?(complete_chunked_message)
      true

      iex> Pulsar.Message.complete?(incomplete_chunked_message)
      false

      iex> Pulsar.Message.complete?(non_chunked_message)
      true
  """
  @spec complete?(t()) :: boolean()
  def complete?(%__MODULE__{chunk_metadata: %{complete: complete}}), do: complete
  def complete?(%__MODULE__{}), do: true
end
