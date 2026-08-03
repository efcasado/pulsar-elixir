defmodule Pulsar.Producer.EpochStoreTest do
  use ExUnit.Case, async: true

  alias Pulsar.Producer.EpochStore

  test "reads and writes are scoped to a client" do
    EpochStore.init(:epoch_a)
    EpochStore.init(:epoch_b)

    :ok = EpochStore.put(:epoch_a, "t", "p", :exclusive, 7)

    assert EpochStore.get(:epoch_a, "t", "p", :exclusive) == {:ok, 7}
    assert EpochStore.get(:epoch_b, "t", "p", :exclusive) == :error
  end

  test "a client that was never started reports no epoch rather than raising" do
    assert EpochStore.get(:epoch_never, "t", "p", :exclusive) == :error
    assert EpochStore.put(:epoch_never, "t", "p", :exclusive, 1) == :error
  end

  test "deletes a stored epoch" do
    EpochStore.init(:epoch_delete)
    :ok = EpochStore.put(:epoch_delete, "t", "p", :exclusive, 7)

    assert EpochStore.delete(:epoch_delete, "t", "p", :exclusive) == :ok
    assert EpochStore.get(:epoch_delete, "t", "p", :exclusive) == :error
  end
end
