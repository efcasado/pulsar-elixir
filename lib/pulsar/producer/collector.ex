defmodule Pulsar.Producer.Collector do
  @moduledoc """
  Accumulates messages into batches for producer-side batching.
  """

  use GenServer

  alias Pulsar.Producer.Flusher

  defstruct [:batch, :current_batch_size, :batch_size_threshold, :flusher_pid]

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Add a message to the batch and wait for broker acknowledgment.
  This is a blocking call that returns when the message is acknowledged.
  Be careful to not have the timeout shorter than the flush interval.
  """
  def add(collector, message, timeout \\ 5000) do
    GenServer.call(collector, {:add, message}, timeout)
  end

  @doc """
  Returns current batch and clears it.
  """
  def get_and_clear(collector) do
    GenServer.call(collector, :get_and_clear)
  end

  @impl true
  def init(opts) do
    state = %__MODULE__{
      batch: [],
      current_batch_size: 0,
      batch_size_threshold: Keyword.fetch!(opts, :batch_size),
      flusher_pid: Keyword.fetch!(opts, :flusher_pid)
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:add, message}, from, state) do
    batch = [{message, from} | state.batch]
    new_size = state.current_batch_size + 1

    current_size =
      if new_size >= state.batch_size_threshold do
        Flusher.flush_now(state.flusher_pid)
        0
      else
        new_size
      end

    # the Producer (or Flusher in case of error) will reply to the callers
    {:noreply, %{state | batch: batch, current_batch_size: current_size}}
  end

  @impl true
  def handle_call(:get_and_clear, _from, state) do
    batch = Enum.reverse(state.batch)
    {:reply, batch, %{state | batch: []}}
  end
end
