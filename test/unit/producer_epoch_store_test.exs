defmodule Pulsar.ProducerEpochStoreTest do
  use ExUnit.Case, async: true

  doctest Pulsar.ProducerEpochStore

  test "reads and writes are scoped to a client" do
    Pulsar.ProducerEpochStore.init(:epoch_a)
    Pulsar.ProducerEpochStore.init(:epoch_b)

    :ok = Pulsar.ProducerEpochStore.put(:epoch_a, "t", "p", :Exclusive, 7)

    assert Pulsar.ProducerEpochStore.get(:epoch_a, "t", "p", :Exclusive) == {:ok, 7}
    assert Pulsar.ProducerEpochStore.get(:epoch_b, "t", "p", :Exclusive) == :error
  end

  test "a client that was never started reports no epoch rather than raising" do
    assert Pulsar.ProducerEpochStore.get(:epoch_never, "t", "p", :Exclusive) == :error
    assert Pulsar.ProducerEpochStore.put(:epoch_never, "t", "p", :Exclusive, 1) == :error
  end
end
