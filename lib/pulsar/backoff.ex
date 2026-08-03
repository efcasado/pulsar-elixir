defmodule Pulsar.Backoff do
  @moduledoc false

  # Ceiling for a single wait in an unbounded retry, the kind `next/1` drives.
  @max_backoff 30_000

  # Maximum total wait in a bounded retry, the kind `run/3` drives.
  @retry_budget 3_000

  @retryable_errors [:disconnected, :ServiceNotReady]

  @spec next(non_neg_integer()) :: pos_integer()
  def next(0), do: :rand.uniform(100)

  def next(previous) do
    min(round(previous * 2), @max_backoff) + :rand.uniform(100)
  end

  @doc """
  Runs a broker operation using Pulsar's standard retry policy and budget.

  Disconnected brokers and `ServiceNotReady` responses can recover without the caller
  changing anything. Other errors are returned immediately.
  """
  @spec run((-> result)) :: result when result: term()
  def run(fun) when is_function(fun, 0), do: run(fun, &retryable?/1, @retry_budget)

  @doc """
  Runs `fun`, retrying the failures `retryable?` accepts and backing off between attempts.

  `fun` returns `{:error, reason}` to fail and anything else to succeed. `budget` is how long
  retrying may take in total, in milliseconds, or `:infinity`; the error is returned unchanged
  once `retryable?` rejects its reason or the next wait would outlast what is left of the budget.

  Blocks the calling process, so this is for a process with nothing else to do meanwhile —
  a worker resolving its own startup, not one that owes replies.
  """
  @spec run((-> result), (term() -> boolean()), timeout()) :: result when result: term()
  def run(fun, retryable?, budget \\ @retry_budget)

  def run(fun, retryable?, :infinity) when is_function(fun, 0) and is_function(retryable?, 1) do
    run(fun, retryable?, :infinity, 0)
  end

  def run(fun, retryable?, budget)
      when is_function(fun, 0) and is_function(retryable?, 1) and is_integer(budget) and budget >= 0 do
    run(fun, retryable?, System.monotonic_time(:millisecond) + budget, 0)
  end

  # Against a deadline rather than by subtracting the sleeps, so that the time `fun` itself
  # spends counts too. The broker calls it wraps carry their own timeouts of several seconds,
  # and charging only the sleeps would let a slow broker stretch a 3s budget into tens of
  # seconds — past the shutdown window the budget exists to stay inside.
  defp run(fun, retryable?, deadline, backoff) do
    case fun.() do
      {:error, reason} = error ->
        wait = next(backoff)

        if retryable?.(reason) and within_budget?(deadline, wait) do
          Process.sleep(wait)
          run(fun, retryable?, deadline, wait)
        else
          error
        end

      result ->
        result
    end
  end

  defp within_budget?(:infinity, _wait), do: true
  defp within_budget?(deadline, wait), do: System.monotonic_time(:millisecond) + wait <= deadline

  defp retryable?({code, _message}), do: code in @retryable_errors
  defp retryable?(reason), do: reason in @retryable_errors
end
