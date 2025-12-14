defmodule Pulsar.Producer.Flusher do
  @moduledoc """
  GenServer that takes care of flushing batched messages from a specific Producer pid.
  Periodically flushes batched messages to the broker, or when manually triggered via `flush_now/1`.

  The flush interval is configurable via the producer options (:flush_interval).
  """

  use GenServer

  require Logger

  defstruct [
    :timer_ref,
    :flush_interval,
    :producer_pid,
    :collector_pid,
    :group_pid
  ]

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Triggers an immediate flush (called when batch is full).
  Cancels current timer and restarts it after flush.
  """
  def flush_now(flusher) do
    GenServer.cast(flusher, :flush_now)
  end

  @impl true
  def init(opts) do
    flush_interval = Keyword.fetch!(opts, :flush_interval)
    timer_ref = Process.send_after(self(), :flush, flush_interval)

    state = %__MODULE__{
      timer_ref: timer_ref,
      flush_interval: flush_interval,
      producer_pid: Keyword.get(opts, :producer_pid),
      collector_pid: Keyword.fetch!(opts, :collector_pid),
      group_pid: Keyword.get(opts, :group_pid)
    }

    {:ok, state}
  end

  @impl true
  def handle_cast(:flush_now, state) do
    Process.cancel_timer(state.timer_ref)
    state = do_flush(state)
    timer_ref = Process.send_after(self(), :flush, state.flush_interval)
    {:noreply, %{state | timer_ref: timer_ref}}
  end

  @impl true
  def handle_info(:flush, state) do
    state = do_flush(state)
    timer_ref = Process.send_after(self(), :flush, state.flush_interval)
    {:noreply, %{state | timer_ref: timer_ref}}
  end

  defp do_flush(%{producer_pid: nil, group_pid: nil} = state), do: state

  defp do_flush(%{producer_pid: nil, group_pid: group_pid} = state) when is_pid(group_pid) do
    case Pulsar.ProducerGroup.get_producers(group_pid) do
      [producer_pid | _] -> do_flush(%{state | producer_pid: producer_pid})
      [] -> state
    end
  end

  defp do_flush(state) do
    state.collector_pid
    |> Pulsar.Producer.Collector.get_and_clear()
    |> send_batch(state)
  end

  defp send_batch([], state), do: state

  defp send_batch(batch, state) do
    messages = Enum.map(batch, fn {message, _from} -> message end)
    callers = Enum.map(batch, fn {_message, from} -> from end)

    case Pulsar.Producer.send_batch(state.producer_pid, messages, callers) do
      :ok ->
        state

      {:error, reason} ->
        Logger.error("Failed to send batch: #{inspect(reason)}")
        Enum.each(callers, fn from -> GenServer.reply(from, {:error, reason}) end)
        state
    end
  end
end
