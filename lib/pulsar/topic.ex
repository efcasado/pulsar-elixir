defmodule Pulsar.Topic do
  @moduledoc false

  # The naming convention for the individual partitions of a partitioned topic:
  # `"<base>-partition-<index>"`. The per-partition group names follow the same convention.

  @separator "-partition-"

  @doc """
  Builds the partition name for `base` (a topic or group name) at `index`.

      iex> Pulsar.Topic.partition("persistent://public/default/t", 3)
      "persistent://public/default/t-partition-3"
  """
  @spec partition(String.t(), non_neg_integer()) :: String.t()
  def partition(base, index), do: "#{base}#{@separator}#{index}"

  @doc """
  Extracts the numeric partition index from a partition topic/group name.

      iex> Pulsar.Topic.index("persistent://public/default/t-partition-3")
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

  @doc """
  Strips the partition suffix, returning the name a partition belongs to. A name without
  one is returned unchanged.

      iex> Pulsar.Topic.base("persistent://public/default/t-partition-3")
      "persistent://public/default/t"

      iex> Pulsar.Topic.base("persistent://public/default/t")
      "persistent://public/default/t"

  Only a trailing index is a partition suffix; the convention is not reserved, so a topic
  may contain the separator itself:

      iex> Pulsar.Topic.base("persistent://public/default/events-partition-archive-partition-0")
      "persistent://public/default/events-partition-archive"
  """
  @spec base(String.t() | atom()) :: String.t()
  def base(partition_name) do
    name = to_string(partition_name)

    case name |> String.split(@separator) |> Enum.split(-1) do
      {[_ | _] = prefix, [index]} -> if partition_index?(index), do: Enum.join(prefix, @separator), else: name
      _no_suffix -> name
    end
  end

  defp partition_index?(""), do: false
  defp partition_index?(index), do: index |> String.to_charlist() |> Enum.all?(&(&1 in ?0..?9))
end
