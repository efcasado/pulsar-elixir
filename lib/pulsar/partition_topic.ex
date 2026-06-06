defmodule Pulsar.PartitionTopic do
  @moduledoc """
  The naming convention for the individual partitions of a partitioned topic:
  `"<base>-partition-<index>"`.

  This is also used for the per-partition consumer/producer group names, which
  follow the same suffix convention.
  """

  @separator "-partition-"

  @doc """
  Builds the partition name for `base` (a topic or group name) at `index`.

      iex> Pulsar.PartitionTopic.name("persistent://public/default/t", 3)
      "persistent://public/default/t-partition-3"
  """
  @spec name(String.t(), non_neg_integer()) :: String.t()
  def name(base, index), do: "#{base}#{@separator}#{index}"

  @doc """
  Extracts the numeric partition index from a partition topic/group name.

      iex> Pulsar.PartitionTopic.index("persistent://public/default/t-partition-3")
      3
  """
  @spec index(String.t() | atom()) :: non_neg_integer()
  def index(partition_name) do
    partition_name
    |> to_string()
    |> String.split(@separator)
    |> List.last()
    |> String.to_integer()
  end
end
