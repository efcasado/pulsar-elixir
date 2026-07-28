defmodule Pulsar.PartitionTopicTest do
  use ExUnit.Case, async: true

  doctest Pulsar.PartitionTopic

  describe "base/1" do
    test "is the inverse of name/2" do
      base = "persistent://public/default/orders"

      for index <- [0, 1, 9, 10, 42] do
        assert base |> Pulsar.PartitionTopic.name(index) |> Pulsar.PartitionTopic.base() == base
      end
    end

    test "leaves a group name's suffix convention intact" do
      assert Pulsar.PartitionTopic.base("orders-sub-partition-2") == "orders-sub"
    end
  end
end
