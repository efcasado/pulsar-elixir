defmodule Pulsar.Message do
  @moduledoc """
  Represents a message received from a Pulsar topic.

  This struct encapsulates all information about a message delivered to a consumer callback.

  ## Fields

  - `command` - The Pulsar protocol command containing message metadata like message ID,
    redelivery count, etc. Type: `Pulsar.Protocol.Binary.Pulsar.Proto.CommandMessage.t()`

  - `metadata` - Message metadata including producer information, publish time, properties, etc.
    Type: `Pulsar.Protocol.Binary.Pulsar.Proto.MessageMetadata.t()`

  - `payload` - The actual message payload as a binary

  - `single_metadata` - Single message metadata (nil for non-batch messages, struct for batch messages).
    This contains metadata specific to an individual message within a batch.

  - `broker_metadata` - Additional broker information about the message

  - `message_id_to_ack` - The message ID to use for ACK/NACK operations. This includes the
    batch_index if the message came from a batch, ensuring proper acknowledgment

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

      # Access all fields via the struct
      def handle_message(%Pulsar.Message{} = msg, state) do
        redelivery_count = msg.command.redelivery_count
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
          command: struct(),
          metadata: struct(),
          payload: binary(),
          single_metadata: struct() | nil,
          broker_metadata: term(),
          message_id_to_ack: term()
        }

  defstruct [
    :command,
    :metadata,
    :payload,
    :single_metadata,
    :broker_metadata,
    :message_id_to_ack
  ]
end
