defmodule Pulsar.Backoff do
  @moduledoc false

  # Ceiling for a single wait in an unbounded retry, the kind `next/1` drives.
  @max_backoff 30_000

  # Maximum total wait in a bounded retry, the kind `run/3` drives.
  @retry_budget 3_000

  @retryable_errors [:connection_lost, :disconnected, :no_broker_available, :ServiceNotReady]

  # Pulsar.Topology.Resolver reports a broker's answer wrapped, and the reason that decides
  # retryability is the one inside.
  @resolver_wrappers [:lookup_failed, :partition_metadata_check_failed]

  @spec next(non_neg_integer()) :: pos_integer()
  def next(0), do: :rand.uniform(100)

  def next(previous) do
    min(round(previous * 2), @max_backoff) + :rand.uniform(100)
  end

  @doc """
  Runs a broker operation using Pulsar's standard retry policy and budget.

  A broker that is disconnected, not yet registered, or still reporting `ServiceNotReady`
  can recover without the caller changing anything. Other errors are returned immediately.

  """
  @spec run((-> result)) :: result when result: term()
  def run(fun) when is_function(fun, 0), do: run(fun, &retryable?/1, @retry_budget)

  @doc """
  Runs `fun`, retrying the failures `retryable?` accepts and backing off between attempts.

  `fun` returns `{:error, reason}` to fail and anything else to succeed. `budget` is how long
  retrying may take in total, in milliseconds, or `:infinity`; the error is returned unchanged
  once `retryable?` rejects its reason or the budget is exhausted.

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
        if retryable?.(reason) do
          retry(error, fun, retryable?, deadline, backoff)
        else
          error
        end

      result ->
        result
    end
  end

  defp retry(error, fun, retryable?, deadline, backoff) do
    wait = next(backoff)

    case retry_wait(deadline, wait) do
      :exhausted ->
        error

      retry_wait ->
        Process.sleep(retry_wait)
        run(fun, retryable?, deadline, wait)
    end
  end

  defp retry_wait(:infinity, wait), do: wait

  defp retry_wait(deadline, wait) do
    case deadline - System.monotonic_time(:millisecond) do
      remaining when remaining > 0 -> min(wait, remaining)
      _elapsed -> :exhausted
    end
  end

  @doc """
  How long to wait before retrying `reason`, for a caller that schedules rather than sleeps.

  `previous` is the last wait this caller used and `deadline` the monotonic millisecond it may
  keep trying until. Answers `{:retry, wait, next}` to try again in `wait` ms, carrying `next`
  into the following call, or `:give_up` once the reason is not retryable or the deadline has
  passed. Same policy and budget as `run/1`, without blocking the caller.

  This is what paces a worker that cannot start. Giving up at once would let its group restart
  it in a tight loop and exhaust the restart budget, which reads as a clean `:shutdown` and
  takes the whole resource down; spacing the attempts spends that budget slowly enough for a
  broker to come back.
  """
  @spec retry_in(term(), non_neg_integer(), integer() | :infinity) ::
          {:retry, pos_integer(), pos_integer()} | :give_up
  def retry_in(reason, previous, deadline) do
    if retryable?(reason) do
      wait = next(previous)

      case retry_wait(deadline, wait) do
        :exhausted -> :give_up
        retry_wait -> {:retry, retry_wait, wait}
      end
    else
      :give_up
    end
  end

  @doc """
  The monotonic millisecond a retry started now may keep trying until.
  """
  @spec deadline() :: integer()
  def deadline, do: System.monotonic_time(:millisecond) + @retry_budget

  @doc false
  @spec retryable?(term()) :: boolean()
  def retryable?({:resolver_failed, :exit, reason}), do: retryable_resolver_exit?(reason)
  def retryable?({wrapper, reason}) when wrapper in @resolver_wrappers, do: retryable?(reason)
  def retryable?({code, _message}), do: code in @retryable_errors
  def retryable?(reason), do: reason in @retryable_errors

  # A broker can disappear after the resolver selects its pid but before the call reaches it.
  # Keep the call context in the match so an arbitrary resolver exit with the same atom remains
  # a programming failure rather than an infinite retry.
  defp retryable_resolver_exit?({reason, {server, :call, _args}})
       when reason in [:noproc, :normal, :shutdown, :timeout] and server in [:gen_statem, GenServer], do: true

  defp retryable_resolver_exit?({{:shutdown, _reason}, {server, :call, _args}}) when server in [:gen_statem, GenServer],
    do: true

  defp retryable_resolver_exit?(_reason), do: false
end
