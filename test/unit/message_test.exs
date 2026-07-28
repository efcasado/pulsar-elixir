defmodule Pulsar.MessageTest do
  use ExUnit.Case, async: true

  alias Pulsar.Message

  doctest Message

  # A corrupt frame's batch count was in the metadata that failed validation, so it cannot
  # be counted; crediting fewer permits than the broker charged stalls the consumer.
  test "num_broker_messages/1 counts an invalid message as one permit" do
    assert Message.num_broker_messages(%Message{validation_error: :checksum_mismatch}) == 1
  end
end
