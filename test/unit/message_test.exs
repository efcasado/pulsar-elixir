defmodule Pulsar.MessageTest do
  use ExUnit.Case, async: true

  alias Pulsar.Message

  doctest Message

  # A corrupt frame's batch count was in the metadata that failed validation, so it cannot
  # be counted; crediting fewer permits than the broker charged stalls the consumer.
  test "num_broker_messages/1 counts an invalid message as one permit" do
    assert Message.num_broker_messages(%Message{validation_error: :checksum_mismatch}) == 1
  end

  # The point of the accessors: the same question has one answer whether the broker delivered
  # the message on its own, inside a batch, or split across chunks.
  describe "accessors across delivery shapes" do
    defp metadata(overrides) do
      Map.merge(
        %{
          producer_name: "orders-api",
          publish_time: 1_700_000_000_000,
          partition_key: nil,
          ordering_key: nil,
          event_time: 0,
          properties: []
        },
        Map.new(overrides)
      )
    end

    test "reads a plain message from its metadata" do
      message = %Message{raw: %{metadata: metadata(partition_key: "user-1"), single_metadata: nil}}

      assert Message.producer_name(message) == "orders-api"
      assert Message.publish_time(message) == 1_700_000_000_000
      assert Message.key(message) == "user-1"
    end

    test "prefers a batch entry's own key, properties and event time" do
      message = %Message{
        raw: %{
          metadata: metadata(partition_key: "batch", properties: [%{key: "from", value: "batch"}]),
          single_metadata: %{
            partition_key: "entry",
            ordering_key: nil,
            event_time: 42,
            properties: [%{key: "from", value: "entry"}]
          }
        }
      }

      assert Message.key(message) == "entry"
      assert Message.event_time(message) == 42
      assert Message.properties(message) == %{"from" => "entry"}
      # producer and publish time are per broker message, so they stay on the outer metadata
      assert Message.producer_name(message) == "orders-api"
    end

    test "falls back to the carrying message for what a batch entry does not set" do
      message = %Message{
        raw: %{
          metadata: metadata(partition_key: "batch"),
          single_metadata: %{partition_key: nil, ordering_key: nil, event_time: 0, properties: []}
        }
      }

      assert Message.key(message) == "batch"
    end

    test "reads a chunked message as one message rather than a list" do
      chunk = metadata(partition_key: "user-9")
      message = %Message{raw: %{metadata: [chunk, chunk, chunk], single_metadata: []}}

      assert Message.producer_name(message) == "orders-api"
      assert Message.key(message) == "user-9"
      assert Message.properties(message) == %{}
    end

    test "answers nil rather than raising for an invalid message with no metadata" do
      message = %Message{validation_error: :checksum_mismatch, raw: %{metadata: nil, single_metadata: nil}}

      assert Message.producer_name(message) == nil
      assert Message.key(message) == nil
      assert Message.event_time(message) == nil
      assert Message.properties(message) == %{}
    end
  end
end
