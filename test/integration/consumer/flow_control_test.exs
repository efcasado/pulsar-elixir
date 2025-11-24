defmodule Pulsar.Integration.Consumer.FlowControlTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :flow_control_test_client
  @topic "persistent://public/default/flow-control"
  @consumer_callback Pulsar.Test.Support.DummyConsumer
  @messages [
    {"key1", "Message 1"},
    {"key2", "Message 2"},
    {"key3", "Message 3"},
    {"key4", "Message 4"},
    {"key5", "Message 5"},
    {"key6", "Message 6"}
  ]

  setup_all do
    broker = System.broker()

    {:ok, client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    {:ok, producer_pid} =
      Pulsar.start_producer(
        @topic,
        client: @client,
        name: :flow_control_producer
      )

    for {key, payload} <- @messages do
      Pulsar.send(:flow_control_producer, payload, partition_key: key, client: @client)
    end

    on_exit(fn ->
      if Process.alive?(producer_pid), do: Pulsar.stop_producer(producer_pid)
      if Process.alive?(client_pid), do: Supervisor.stop(client_pid, :normal)
    end)

    {:ok, expected_count: Enum.count(@messages)}
  end

  setup [:telemetry_listen]

  @tag telemetry_listen: [[:pulsar, :consumer, :flow_control, :stop]]
  test "tiny permits with zero threshold triggers refill on every message", %{
    expected_count: expected_count
  } do
    {:ok, consumer_group} =
      Pulsar.start_consumer(
        @topic,
        "tiny-permits",
        @consumer_callback,
        subscription_options(1, 1, 0, 1)
      )

    [consumer] = Pulsar.get_consumers(consumer_group)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == expected_count
    end)

    consumer_id = consumer |> :sys.get_state() |> Map.get(:consumer_id)
    stats = Utils.collect_flow_stats()

    # 1 initial + 6 refills (one per message) = 7 total events
    # Each event requests 1 permit, so requested_total should be 7
    assert %{^consumer_id => %{event_count: 7, requested_total: 7}} = stats
  end

  @tag telemetry_listen: [[:pulsar, :consumer, :flow_control, :stop]]
  test "threshold triggers refill when outstanding permits drop below threshold", %{
    expected_count: expected_count
  } do
    {:ok, consumer_group} =
      Pulsar.start_consumer(
        @topic,
        "threshold-test",
        @consumer_callback,
        subscription_options(1, 5, 3, 4)
      )

    [consumer] = Pulsar.get_consumers(consumer_group)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == expected_count
    end)

    consumer_id = consumer |> :sys.get_state() |> Map.get(:consumer_id)
    stats = Utils.collect_flow_stats()

    # Initial: 5 permits
    # After 2 messages: 3 permits remaining (within threshold of 3) -> refill 4 = 7 permits
    # After 4 messages: 3 permits remaining (within threshold of 3) -> no more messages
    # Expected: 3 events (initial + 2 refill)
    assert %{^consumer_id => %{event_count: 3, requested_total: 13}} = stats
  end

  test "manual flow control with zero initial permits", %{expected_count: expected_count} do
    {:ok, consumer_group} =
      Pulsar.start_consumer(
        @topic,
        "manual-flow",
        @consumer_callback,
        subscription_options(1, 0, 0, 0)
      )

    [consumer] = Pulsar.get_consumers(consumer_group)

    # Initially, no messages should be received
    Process.sleep(500)
    assert @consumer_callback.count_messages(consumer) == 0

    # Manually request 3 messages
    :ok = Pulsar.Consumer.send_flow(consumer, 3)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == 3
    end)

    # Request remaining messages
    :ok = Pulsar.Consumer.send_flow(consumer, expected_count - 3)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == expected_count
    end)

    assert @consumer_callback.count_messages(consumer) == expected_count
  end

  defp subscription_options(count, initial, threshold, refill) do
    [
      client: @client,
      initial_position: :earliest,
      consumer_count: count,
      flow_initial: initial,
      flow_threshold: threshold,
      flow_refill: refill,
      startup_delay_ms: 0,
      startup_jitter_ms: 0
    ]
  end
end
