defmodule Pulsar.Topic do
  @moduledoc false

  # The naming convention for the individual partitions of a partitioned topic:
  # `"<base>-partition-<index>"`. Per-partition worker name bases follow the same convention.

  @separator "-partition-"

  @doc """
  Builds the partition name for `base` (a topic or resource name) at `index`.

      iex> Pulsar.Topic.partition("persistent://public/default/t", 3)
      "persistent://public/default/t-partition-3"
  """
  @spec partition(String.t() | atom(), non_neg_integer()) :: String.t()
  def partition(base, index), do: "#{base}#{@separator}#{index}"
end
