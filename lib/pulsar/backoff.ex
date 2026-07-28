defmodule Pulsar.Backoff do
  @moduledoc false

  @spec next(non_neg_integer(), pos_integer()) :: pos_integer()
  def next(0, _max_backoff), do: :rand.uniform(100)

  def next(previous, max_backoff) do
    min(round(previous * 2), max_backoff) + :rand.uniform(100)
  end
end
