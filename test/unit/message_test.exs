defmodule Pulsar.MessageTest do
  use ExUnit.Case, async: true

  alias Pulsar.Message

  describe "valid?/1" do
    test "is true for a message with no validation error" do
      assert Message.valid?(%Message{payload: "hello"})
    end

    test "is false once a validation error is set" do
      refute Message.valid?(%Message{payload: <<255>>, validation_error: :checksum_mismatch})
    end

    test "is independent of chunk completeness" do
      incomplete = %Message{chunk_metadata: %{chunked: true, complete: false, error: :expired}}

      assert Message.valid?(incomplete)
      refute Message.complete?(incomplete)
    end
  end

  describe "num_broker_messages/1" do
    test "counts an invalid message as one permit" do
      assert Message.num_broker_messages(%Message{validation_error: :checksum_mismatch}) == 1
    end
  end
end
