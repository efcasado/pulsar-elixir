defmodule Pulsar.Integration.Consumer.FlowControlTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils
  alias Pulsar.Topology

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

    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    {:ok, _producer_pid} =
      Pulsar.Producer.start(
        @topic,
        client: @client,
        name: :flow_control_producer
      )

    for {key, payload} <- @messages do
      Utils.wait_for(
        fn -> Pulsar.Producer.send(:flow_control_producer, payload, partition_key: key, client: @client) end,
        until: &match?({:ok, _message_id}, &1)
      )
    end

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)

    {:ok, expected_count: Enum.count(@messages)}
  end

  setup [:telemetry_listen]

  @tag telemetry_listen: [[:pulsar, :consumer, :flow_control, :stop]]
  test "tiny permits with zero threshold triggers refill on every message", %{
    expected_count: expected_count
  } do
    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        @topic,
        "tiny-permits",
        @consumer_callback,
        subscription_options(1, 1, 0, 1)
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [consumer] = Topology.workers(consumer_group)

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
      Pulsar.Consumer.start(
        @topic,
        "threshold-test",
        @consumer_callback,
        subscription_options(1, 5, 3, 4)
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [consumer] = Topology.workers(consumer_group)

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
      Pulsar.Consumer.start(
        @topic,
        "manual-flow",
        @consumer_callback,
        subscription_options(1, 0, 0, 0)
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [consumer] = Topology.workers(consumer_group)

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

  test "granting permits through the group pid reaches its workers" do
    {:ok, consumer_group} =
      Pulsar.Consumer.start(@topic, "group-flow", @consumer_callback, subscription_options(2, 0, 0, 0))

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    workers = Topology.workers(consumer_group)
    assert length(workers) == 2

    # The pid start/1 returns is a supervisor, which cannot answer the worker's call.
    assert :ok = Pulsar.Consumer.send_flow(consumer_group, 2)

    assert Process.alive?(consumer_group)
    assert Enum.all?(workers, &Process.alive?/1)

    Utils.wait_for(fn -> Enum.sum(Enum.map(workers, &@consumer_callback.count_messages/1)) > 0 end)
    assert Enum.sum(Enum.map(workers, &@consumer_callback.count_messages/1)) > 0

    Pulsar.Consumer.stop(consumer_group)
  end

  defp subscription_options(count, initial, threshold, refill) do
    [
      client: @client,
      initial_position: :earliest,
      consumer_count: count,
      flow_policy: if(initial == 0, do: {Pulsar.Test.Support.Flow, :never, []}, else: :auto),
      flow_initial: initial,
      flow_threshold: threshold,
      flow_refill: refill
    ]
  end
end
