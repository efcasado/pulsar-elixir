defmodule Pulsar.Producer.EpochStoreTest do
  use ExUnit.Case, async: true

  alias Pulsar.Producer.EpochStore

  doctest EpochStore

  test "reads and writes are scoped to a client" do
    EpochStore.init(:epoch_a)
    EpochStore.init(:epoch_b)

    :ok = EpochStore.put(:epoch_a, "t", "p", :Exclusive, 7)

    assert EpochStore.get(:epoch_a, "t", "p", :Exclusive) == {:ok, 7}
    assert EpochStore.get(:epoch_b, "t", "p", :Exclusive) == :error
  end

  test "a client that was never started reports no epoch rather than raising" do
    assert EpochStore.get(:epoch_never, "t", "p", :Exclusive) == :error
    assert EpochStore.put(:epoch_never, "t", "p", :Exclusive, 1) == :error
  end
end
