defmodule Pulsar.Integration.Producer.MessageOptionsTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :producer_message_options_test_client
  @topic "persistent://public/default/producer-message-options-test"
  @consumer_callback Pulsar.Test.Support.DummyConsumer

  setup_all do
    broker = System.broker()

    :ok = System.create_topic(@topic)

    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    {:ok, group_pid} =
      Pulsar.start_producer(@topic,
        client: @client,
        name: "message-options-producer"
      )

    [producer] = Pulsar.get_producers(group_pid)

    :ok =
      Utils.wait_for(fn ->
        state = :sys.get_state(producer)
        state.producer_name != nil
      end)

    {:ok, _consumer_group} =
      Pulsar.start_consumer(@topic, "message-options-sub", @consumer_callback,
        client: @client,
        init_args: [notify_pid: self()]
      )

    consumer =
      receive do
        {:consumer_ready, pid} -> pid
      after
        5000 -> flunk("Timeout waiting for consumer to be ready")
      end

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)

    %{producer: group_pid, consumer: consumer}
  end

  test "send message with partition key", %{producer: producer, consumer: consumer} do
    assert {:ok, _message_id} =
             Pulsar.send(producer, "payload with key", partition_key: "user-123", client: @client)

    message = wait_for_message(consumer, "payload with key")
    assert message.metadata.partition_key == "user-123"
  end

  test "send message with ordering key", %{producer: producer, consumer: consumer} do
    assert {:ok, _message_id} =
             Pulsar.send(producer, "payload with ordering key",
               ordering_key: "order-456",
               client: @client
             )

    message = wait_for_message(consumer, "payload with ordering key")
    assert message.metadata.ordering_key == "order-456"
  end

  test "send message with properties map", %{producer: producer, consumer: consumer} do
    properties = %{
      "trace_id" => "abc-123",
      "source" => "test-service",
      "version" => "1.0"
    }

    assert {:ok, _message_id} =
             Pulsar.send(producer, "payload with properties",
               properties: properties,
               client: @client
             )

    message = wait_for_message(consumer, "payload with properties")

    received_properties =
      Map.new(message.metadata.properties, fn %{key: k, value: v} -> {k, v} end)

    assert received_properties == properties
  end

  test "send message with event time", %{producer: producer, consumer: consumer} do
    event_time = DateTime.utc_now()
    event_time_ms = DateTime.to_unix(event_time, :millisecond)

    assert {:ok, _message_id} =
             Pulsar.send(producer, "payload with event time",
               event_time: event_time,
               client: @client
             )

    message = wait_for_message(consumer, "payload with event time")
    assert message.metadata.event_time == event_time_ms
  end

  test "send message with deliver_at_time option", %{producer: producer, consumer: consumer} do
    deliver_at = DateTime.add(DateTime.utc_now(), 1, :second)
    deliver_at_ms = DateTime.to_unix(deliver_at, :millisecond)

    assert {:ok, _message_id} =
             Pulsar.send(producer, "deliver_at payload",
               deliver_at_time: deliver_at,
               client: @client
             )

    message = wait_for_message(consumer, "deliver_at payload")
    assert message.metadata.deliver_at_time == deliver_at_ms
  end

  test "send message with deliver_after option", %{producer: producer, consumer: consumer} do
    before_send = :erlang.system_time(:millisecond)

    assert {:ok, _message_id} =
             Pulsar.send(producer, "deliver_after payload",
               deliver_after: 1000,
               client: @client
             )

    message = wait_for_message(consumer, "deliver_after payload")

    assert_in_delta message.metadata.deliver_at_time, before_send + 1000, 10
  end

  test "send message with all options combined", %{producer: producer, consumer: consumer} do
    properties = %{
      "trace_id" => "xyz-789",
      "source" => "integration-test",
      "version" => "2.0"
    }

    event_time = DateTime.utc_now()
    event_time_ms = DateTime.to_unix(event_time, :millisecond)

    assert {:ok, _message_id} =
             Pulsar.send(producer, "complex payload",
               partition_key: "user-999",
               ordering_key: "order-888",
               properties: properties,
               event_time: event_time,
               client: @client
             )

    message = wait_for_message(consumer, "complex payload")

    assert message.metadata.partition_key == "user-999"
    assert message.metadata.ordering_key == "order-888"
    assert message.metadata.event_time == event_time_ms

    received_properties =
      Map.new(message.metadata.properties, fn %{key: k, value: v} -> {k, v} end)

    assert received_properties == properties
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
