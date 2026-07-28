defmodule Pulsar.Backoff do
  @moduledoc false

  # Exponential backoff with jitter, between a first attempt of at most 100ms and a
  # ceiling. Jitter is added after the cap so simultaneous retries spread out rather
  # than arriving together.

  @spec next(non_neg_integer(), pos_integer()) :: pos_integer()
  def next(0, _max_backoff), do: :rand.uniform(100)

  def next(previous, max_backoff) do
    min(round(previous * 2), max_backoff) + :rand.uniform(100)
  end
end
