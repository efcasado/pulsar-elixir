defmodule Pulsar.Consumer.ChunkedMessageContext do
  @moduledoc """
  Manages chunked message assembly for Pulsar consumers.

  This module handles the buffering, ordering, and reassembly of message chunks
  that arrive from the broker. It tracks multiple concurrent chunked messages
  and ensures chunks are received in order before assembling the complete payload.
  """

  require Logger

  defstruct [
    :uuid,
    :chunks,
    :chunk_message_ids,
    :num_chunks_from_msg,
    :total_chunk_msg_size,
    :received_chunks,
    :first_chunk_message_id,
    :last_chunk_message_id,
    :created_at,
    :commands,
    :metadatas,
    :broker_metadatas
  ]

  @type t :: %__MODULE__{
          uuid: String.t(),
          chunks: %{non_neg_integer() => binary()},
          chunk_message_ids: %{non_neg_integer() => term()},
          num_chunks_from_msg: non_neg_integer(),
          total_chunk_msg_size: non_neg_integer(),
          received_chunks: non_neg_integer(),
          first_chunk_message_id: term(),
          last_chunk_message_id: term(),
          created_at: integer(),
          commands: [term()],
          metadatas: [term()],
          broker_metadatas: [term()]
        }

  @doc """
  Creates a new chunked message context from the first chunk.

  Returns `{:ok, context}` if the chunk is valid (chunk_id == 0),
  or `{:error, :out_of_order}` if it's not the first chunk.
  """
  @spec new(
          uuid :: String.t(),
          chunk_id :: non_neg_integer(),
          num_chunks :: non_neg_integer(),
          total_size :: non_neg_integer(),
          payload :: binary(),
          message_id :: term(),
          command :: term(),
          metadata :: term(),
          broker_metadata :: term()
        ) :: {:ok, t()} | {:error, :out_of_order}
  def new(uuid, chunk_id, num_chunks, total_size, payload, message_id, command, metadata, broker_metadata) do
    if chunk_id == 0 do
      ctx = %__MODULE__{
        uuid: uuid,
        chunks: %{chunk_id => payload},
        chunk_message_ids: %{chunk_id => message_id},
        num_chunks_from_msg: num_chunks,
        total_chunk_msg_size: total_size,
        received_chunks: 1,
        first_chunk_message_id: message_id,
        last_chunk_message_id: message_id,
        created_at: System.monotonic_time(:millisecond),
        commands: [command],
        metadatas: [metadata],
        broker_metadatas: [broker_metadata]
      }

      {:ok, ctx}
    else
      {:error, :out_of_order}
    end
  end

  @doc """
  Adds a chunk to an existing context.

  Returns `{:ok, updated_context}` if the chunk is in order,
  or `{:error, :out_of_order}` if it's not the expected next chunk.
  """
  @spec add_chunk(t(), non_neg_integer(), binary(), term(), term(), term(), term()) ::
          {:ok, t()} | {:error, :out_of_order}
  def add_chunk(ctx, chunk_id, payload, message_id, command, metadata, broker_metadata) do
    expected_chunk_id = ctx.received_chunks

    if chunk_id == expected_chunk_id do
      updated_ctx = %{
        ctx
        | chunks: Map.put(ctx.chunks, chunk_id, payload),
          chunk_message_ids: Map.put(ctx.chunk_message_ids, chunk_id, message_id),
          received_chunks: ctx.received_chunks + 1,
          last_chunk_message_id: message_id,
          commands: ctx.commands ++ [command],
          metadatas: ctx.metadatas ++ [metadata],
          broker_metadatas: ctx.broker_metadatas ++ [broker_metadata]
      }

      {:ok, updated_ctx}
    else
      {:error, :out_of_order}
    end
  end

  @doc """
  Checks if all chunks have been received.
  """
  @spec complete?(t()) :: boolean()
  def complete?(ctx) do
    ctx.received_chunks == ctx.num_chunks_from_msg
  end

  @doc """
  Assembles all chunks into a complete payload.

  Returns the binary payload with all chunks concatenated in order.
  """
  @spec assemble_payload(t()) :: binary()
  def assemble_payload(ctx) do
    0..(ctx.num_chunks_from_msg - 1)
    |> Enum.map(fn chunk_id -> Map.get(ctx.chunks, chunk_id, "") end)
    |> IO.iodata_to_binary()
  end

  @doc """
  Checks if a context has expired based on the given threshold.

  ## Parameters

  - `ctx` - The chunked message context
  - `expiration_threshold_ms` - Maximum age in milliseconds

  Returns `true` if the context is older than the threshold.
  """
  @spec expired?(t(), non_neg_integer()) :: boolean()
  def expired?(ctx, expiration_threshold_ms) do
    now = System.monotonic_time(:millisecond)
    now - ctx.created_at > expiration_threshold_ms
  end

  @doc """
  Finds the oldest context in a map of contexts.

  Returns `{uuid, context}` tuple for the oldest context, or `nil` if the map is empty.
  """
  @spec find_oldest(%{optional(String.t()) => t()}) :: {String.t(), t()} | nil
  def find_oldest(contexts) when map_size(contexts) == 0, do: nil

  def find_oldest(contexts) do
    Enum.min_by(contexts, fn {_uuid, ctx} -> ctx.created_at end)
  end

  @doc """
  Finds all expired contexts in a map.

  Returns a tuple `{expired_list, remaining_map}` where:
  - `expired_list` is a list of `{uuid, context}` tuples that have expired
  - `remaining_map` is a map of contexts that have not expired
  """
  @spec find_expired(%{optional(String.t()) => t()}, non_neg_integer()) ::
          {[{String.t(), t()}], %{optional(String.t()) => t()}}
  def find_expired(contexts, expiration_threshold_ms) do
    now = System.monotonic_time(:millisecond)

    Enum.split_with(contexts, fn {_uuid, ctx} ->
      now - ctx.created_at > expiration_threshold_ms
    end)
  end

  @doc """
  Returns the age of the context in milliseconds.
  """
  @spec age_ms(t()) :: non_neg_integer()
  def age_ms(ctx) do
    now = System.monotonic_time(:millisecond)
    now - ctx.created_at
  end

  @doc """
  Returns a list of all message IDs for chunks received so far.
  """
  @spec all_message_ids(t()) :: [term()]
  def all_message_ids(ctx) do
    Map.values(ctx.chunk_message_ids)
  end
end
