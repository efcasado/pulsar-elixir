defmodule Pulsar.Consumer.ChunkedMessageContext do
  @moduledoc """
  Manages chunked message assembly for Pulsar consumers.

  This module handles the buffering and reassembly of message chunks that arrive
  from the broker. It tracks multiple concurrent chunked messages and accepts
  chunks in any order, assembling the complete payload once all chunks are received.
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
  Creates a new chunked message context from any chunk.

  Accepts the first chunk received for a chunked message, regardless of its chunk_id.
  Returns `{:ok, context}` with the new context.
  """
  @spec new(command :: term(), metadata :: term(), payload :: binary(), broker_metadata :: term()) ::
          {:ok, t()}
  def new(command, metadata, payload, broker_metadata) do
    chunk_id = metadata.chunk_id
    message_id = command.message_id
    uuid = metadata.uuid
    num_chunks = metadata.num_chunks_from_msg
    total_size = Map.get(metadata, :total_chunk_msg_size, 0)

    ctx = %__MODULE__{
      uuid: uuid,
      chunks: %{chunk_id => payload},
      chunk_message_ids: %{chunk_id => message_id},
      num_chunks_from_msg: num_chunks,
      total_chunk_msg_size: total_size,
      received_chunks: 1,
      first_chunk_message_id: if(chunk_id == 0, do: message_id),
      last_chunk_message_id: message_id,
      created_at: System.monotonic_time(:millisecond),
      commands: [command],
      metadatas: [metadata],
      broker_metadatas: [broker_metadata]
    }

    {:ok, ctx}
  end

  @doc """
  Adds a chunk to an existing context.

  Accepts chunks in any order. If a chunk with the same chunk_id already exists,
  it is replaced (idempotent). Returns `{:ok, updated_context}`.
  """
  @spec add_chunk(t(), command :: term(), metadata :: term(), payload :: binary(), broker_metadata :: term()) ::
          {:ok, t()}
  def add_chunk(ctx, command, metadata, payload, broker_metadata) do
    chunk_id = metadata.chunk_id
    message_id = command.message_id
    already_has_chunk = Map.has_key?(ctx.chunks, chunk_id)

    updated_ctx = %{
      ctx
      | chunks: Map.put(ctx.chunks, chunk_id, payload),
        chunk_message_ids: Map.put(ctx.chunk_message_ids, chunk_id, message_id),
        received_chunks: if(already_has_chunk, do: ctx.received_chunks, else: ctx.received_chunks + 1),
        first_chunk_message_id: if(chunk_id == 0, do: message_id, else: ctx.first_chunk_message_id),
        last_chunk_message_id: message_id,
        commands: ctx.commands ++ [command],
        metadatas: ctx.metadatas ++ [metadata],
        broker_metadatas: ctx.broker_metadatas ++ [broker_metadata]
    }

    {:ok, updated_ctx}
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
  Pops the oldest context from a map.

  Returns a tuple `{{uuid, context}, remaining_map}` with the oldest context and
  remaining contexts, or `{nil, contexts}` if the map is empty.
  """
  @spec pop_oldest(%{optional(String.t()) => t()}) ::
          {{String.t(), t()}, %{optional(String.t()) => t()}} | {nil, %{optional(String.t()) => t()}}
  def pop_oldest(contexts) when map_size(contexts) == 0, do: {nil, contexts}

  def pop_oldest(contexts) do
    {oldest_uuid, _oldest_ctx} = Enum.min_by(contexts, fn {_uuid, ctx} -> ctx.created_at end)
    {oldest, remaining} = Map.pop(contexts, oldest_uuid)
    {{oldest_uuid, oldest}, remaining}
  end

  @doc """
  Pops all expired contexts from a map.

  Returns a tuple `{expired_list, remaining_map}` where:
  - `expired_list` is a list of `{uuid, context}` tuples that have expired
  - `remaining_map` is a map of contexts that have not expired
  """
  @spec pop_expired(%{optional(String.t()) => t()}, non_neg_integer()) ::
          {[{String.t(), t()}], %{optional(String.t()) => t()}}
  def pop_expired(contexts, expiration_threshold_ms) do
    now = System.monotonic_time(:millisecond)

    {expired, remaining} =
      Enum.split_with(contexts, fn {_uuid, ctx} ->
        now - ctx.created_at > expiration_threshold_ms
      end)

    {expired, Map.new(remaining)}
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
