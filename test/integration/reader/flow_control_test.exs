defmodule Pulsar.Integration.Reader.FlowControlTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :reader_flow_control_test_client
  @topic "persistent://public/default/reader-flow-control-test"
  @num_messages 20

  setup_all do
    broker = System.broker()

    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    {:ok, _producer_pid} =
      Pulsar.start_producer(
        @topic,
        client: @client,
        name: :reader_flow_control_test_producer
      )

    for i <- 1..@num_messages do
      Pulsar.send(:reader_flow_control_test_producer, "Message #{i}", client: @client)
    end

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)

    :ok
  end

  setup [:telemetry_listen]

  @tag telemetry_listen: [[:pulsar, :consumer, :flow_control, :stop]]
  test "small flow_permits triggers refills" do
    messages =
      @topic
      |> Pulsar.Reader.stream(client: @client, flow_permits: 5, timeout: 100)
      |> Enum.to_list()

    assert length(messages) == @num_messages

    consumer_id = hd(messages).command.consumer_id
    consumer_stats = Map.fetch!(Utils.collect_flow_stats(), consumer_id)

    assert consumer_stats.event_count == 5
    assert consumer_stats.requested_total == @num_messages + 5
  end

  @tag telemetry_listen: [[:pulsar, :consumer, :flow_control, :stop]]
  test "flow_permits of 1 triggers refill on every message" do
    messages =
      @topic
      |> Pulsar.Reader.stream(client: @client, flow_permits: 1, timeout: 100)
      |> Enum.to_list()

    assert length(messages) == @num_messages

    consumer_id = hd(messages).command.consumer_id
    consumer_stats = Map.fetch!(Utils.collect_flow_stats(), consumer_id)

    assert consumer_stats.event_count == @num_messages + 1
    assert consumer_stats.requested_total == @num_messages + 1
  end

  @tag telemetry_listen: [[:pulsar, :consumer, :flow_control, :stop]]
  test "large flow_permits requires only initial request" do
    messages =
      @topic
      |> Pulsar.Reader.stream(client: @client, flow_permits: 1000, timeout: 100)
      |> Enum.to_list()

    assert length(messages) == @num_messages

    consumer_id = hd(messages).command.consumer_id
    consumer_stats = Map.fetch!(Utils.collect_flow_stats(), consumer_id)

    assert consumer_stats.event_count == 1
    assert consumer_stats.requested_total == 1000
  end
end
