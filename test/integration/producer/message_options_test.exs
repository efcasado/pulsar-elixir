defmodule Pulsar.Integration.Producer.MessageOptionsTest do
  use Pulsar.Test.Case, async: true

  @topic "persistent://public/default/producer-message-options-test"
  @consumer_callback Pulsar.Test.Support.DummyConsumer

  setup_all do
    :ok = System.create_topic(@topic)

    {:ok, group_pid} =
      Pulsar.Producer.start(@topic,
        client: @client,
        name: "message-options-producer"
      )

    :ok = Pulsar.Producer.await_ready(group_pid)

    {:ok, consumer_group} =
      Pulsar.Consumer.start(@topic, "message-options-sub", @consumer_callback, client: @client)

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [consumer] = Topology.workers(consumer_group)

    %{producer: group_pid, consumer: consumer}
  end

  test "carries the partition key it was sent with", %{producer: producer, consumer: consumer} do
    assert {:ok, _message_id} =
             Pulsar.Producer.send(producer, "payload with key", partition_key: "user-123", client: @client)

    message = wait_for_message(consumer, "payload with key")
    assert Pulsar.Message.key(message) == "user-123"
  end

  test "carries the ordering key it was sent with", %{producer: producer, consumer: consumer} do
    assert {:ok, _message_id} =
             Pulsar.Producer.send(producer, "payload with ordering key",
               ordering_key: "order-456",
               client: @client
             )

    message = wait_for_message(consumer, "payload with ordering key")
    assert Pulsar.Message.ordering_key(message) == "order-456"
  end

  test "carries the properties it was sent with", %{producer: producer, consumer: consumer} do
    properties = %{
      "trace_id" => "abc-123",
      "source" => "test-service",
      "version" => "1.0"
    }

    assert {:ok, _message_id} =
             Pulsar.Producer.send(producer, "payload with properties",
               properties: properties,
               client: @client
             )

    message = wait_for_message(consumer, "payload with properties")

    assert Pulsar.Message.properties(message) == properties
  end

  test "carries the event time it was sent with, as milliseconds", %{producer: producer, consumer: consumer} do
    event_time = DateTime.utc_now()
    event_time_ms = DateTime.to_unix(event_time, :millisecond)

    assert {:ok, _message_id} =
             Pulsar.Producer.send(producer, "payload with event time",
               event_time: event_time,
               client: @client
             )

    message = wait_for_message(consumer, "payload with event time")
    assert Pulsar.Message.event_time(message) == event_time_ms
  end

  test ":deliver_at_time asks the broker to hold it until then", %{producer: producer, consumer: consumer} do
    deliver_at = DateTime.shift(DateTime.utc_now(), second: 1)
    deliver_at_ms = DateTime.to_unix(deliver_at, :millisecond)

    assert {:ok, _message_id} =
             Pulsar.Producer.send(producer, "deliver_at payload",
               deliver_at_time: deliver_at,
               client: @client
             )

    message = wait_for_message(consumer, "deliver_at payload")
    assert message.raw.metadata.deliver_at_time == deliver_at_ms
  end

  test ":deliver_after resolves to a delivery time against the clock", %{producer: producer, consumer: consumer} do
    before_send = :erlang.system_time(:millisecond)

    assert {:ok, _message_id} =
             Pulsar.Producer.send(producer, "deliver_after payload",
               deliver_after: 1000,
               client: @client
             )

    message = wait_for_message(consumer, "deliver_after payload")

    assert_in_delta message.raw.metadata.deliver_at_time, before_send + 1000, 10
  end

  test "carries every option at once, none of them displacing another", %{producer: producer, consumer: consumer} do
    properties = %{
      "trace_id" => "xyz-789",
      "source" => "integration-test",
      "version" => "2.0"
    }

    event_time = DateTime.utc_now()
    event_time_ms = DateTime.to_unix(event_time, :millisecond)

    assert {:ok, _message_id} =
             Pulsar.Producer.send(producer, "complex payload",
               partition_key: "user-999",
               ordering_key: "order-888",
               properties: properties,
               event_time: event_time,
               client: @client
             )

    message = wait_for_message(consumer, "complex payload")

    assert Pulsar.Message.key(message) == "user-999"
    assert Pulsar.Message.ordering_key(message) == "order-888"
    assert Pulsar.Message.event_time(message) == event_time_ms

    assert Pulsar.Message.properties(message) == properties
  end

  defp wait_for_message(consumer, payload) do
    Utils.wait_for(fn ->
      consumer
      |> @consumer_callback.get_messages()
      |> Enum.any?(&(&1.payload == payload))
    end)

    consumer
    |> @consumer_callback.get_messages()
    |> Enum.find(&(&1.payload == payload))
  end
end
