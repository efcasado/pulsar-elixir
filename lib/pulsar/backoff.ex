defmodule Pulsar.Backoff do
  @moduledoc false

  # Ceiling for a single wait in an unbounded retry, the kind `next/1` drives.
  @max_backoff 30_000

  # Maximum total wait in a bounded retry, the kind `run/3` drives.
  @retry_budget 3_000

  @spec next(non_neg_integer()) :: pos_integer()
  def next(0), do: :rand.uniform(100)

  def next(previous) do
    min(round(previous * 2), @max_backoff) + :rand.uniform(100)
  end

  @doc """
  Runs `fun`, retrying the failures `retryable?` accepts and backing off between attempts.

  `fun` returns `{:error, reason}` to fail and anything else to succeed. `budget` is how long
  retrying may take in total, in milliseconds; the error is returned unchanged once `retryable?`
  rejects its reason or the next wait would outlast what is left of the budget, so giving up
  reads the same as failing outright.

  Blocks the calling process, so this is for a process with nothing else to do meanwhile —
  a worker resolving its own startup, not one that owes replies.
  """
  @spec run((-> result), (term() -> boolean()), non_neg_integer()) :: result when result: term()
  def run(fun, retryable?, budget \\ @retry_budget) when is_function(fun, 0) and is_function(retryable?, 1) do
    run(fun, retryable?, budget, 0)
  end

  defp run(fun, retryable?, budget, backoff) do
    case fun.() do
      {:error, reason} = error ->
        wait = next(backoff)

        if retryable?.(reason) and wait <= budget do
          Process.sleep(wait)
          run(fun, retryable?, budget - wait, wait)
        else
          error
        end

      result ->
        result
    end
  end
end
