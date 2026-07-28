defmodule Pulsar.Backoff do
  @moduledoc false

  @max_backoff 30_000

  @spec next(non_neg_integer()) :: pos_integer()
  def next(0), do: :rand.uniform(100)

  def next(previous) do
    min(round(previous * 2), @max_backoff) + :rand.uniform(100)
  end
end
