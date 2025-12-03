defmodule Pulsar.Integration.Producer.MessageOptionsTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :producer_message_options_test_client
  @topic "persistent://public/default/producer-message-options-test"

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

    on_exit(fn ->
      Pulsar.stop_producer(group_pid)
      Pulsar.Client.stop(@client)
    end)

    %{producer: group_pid}
  end

  test "send message with partition key", %{producer: producer} do
    assert {:ok, _message_id} =
             Pulsar.send(producer, "payload with key", key: "user-123", client: @client)
  end

  test "send message with ordering key", %{producer: producer} do
    assert {:ok, _message_id} =
             Pulsar.send(producer, "payload with ordering key", ordering_key: "order-456", client: @client)
  end

  test "send message with properties map", %{producer: producer} do
    properties = %{
      "trace_id" => "abc-123",
      "source" => "test-service",
      "version" => "1.0"
    }

    assert {:ok, _message_id} =
             Pulsar.send(producer, "payload with properties", properties: properties, client: @client)
  end

  test "send message with event time ", %{producer: producer} do
    event_time = DateTime.utc_now()

    assert {:ok, _message_id} =
             Pulsar.send(producer, "payload with event time", event_time: event_time, client: @client)

    event_time_ms = :erlang.system_time(:millisecond) - 60_000

    assert {:ok, _message_id} =
             Pulsar.send(producer, "payload with event time ms", event_time: event_time_ms, client: @client)
  end

  test "send message with deliver options", %{producer: producer} do
    deliver_at = DateTime.add(DateTime.utc_now(), 5, :second)

    assert {:ok, _message_id} =
             Pulsar.send(producer, "delayed payload", deliver_at_time: deliver_at, client: @client)

    assert {:ok, _message_id} =
             Pulsar.send(producer, "delayed payload relative", deliver_after: 5000, client: @client)
  end

  test "send message with all options combined", %{producer: producer} do
    properties = %{
      "trace_id" => "xyz-789",
      "source" => "integration-test",
      "version" => "2.0"
    }

    event_time = DateTime.utc_now()
    deliver_at = DateTime.add(DateTime.utc_now(), 10, :second)

    assert {:ok, _message_id} =
             Pulsar.send(producer, "complex payload",
               key: "user-999",
               ordering_key: "order-888",
               properties: properties,
               event_time: event_time,
               deliver_at_time: deliver_at,
               client: @client
             )
  end
end
