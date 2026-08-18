defmodule Pulsar.Integration.Consumer.FlowControlTest do
  use Pulsar.Test.Case, async: true

  @topic "persistent://public/default/flow-control"
  @flow_control [:pulsar, :consumer, :flow_control, :stop]
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
    Utils.seed_topic(@topic, @messages, client: @client)

    {:ok, expected_count: length(@messages)}
  end

  @tag telemetry_listen: [@flow_control]
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

    # One permit up front and one back per message consumed, so seven grants of one.
    for _grant <- 1..7, do: assert_granted(consumer_id, 1)
    refute_granted(consumer_id)
  end

  @tag telemetry_listen: [@flow_control]
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

    # Five up front, then a refill of four each time the window falls to the threshold of three.
    assert_granted(consumer_id, 5)
    assert_granted(consumer_id, 4)
    assert_granted(consumer_id, 4)
    refute_granted(consumer_id)
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

    Pulsar.Consumer.stop(consumer_group)
  end

  # Every test listening for this event is sent every consumer's, so the id picks out ours.
  defp assert_granted(consumer_id, permits) do
    assert_receive {:telemetry_event,
                    %{
                      event: @flow_control,
                      metadata: %{consumer_id: ^consumer_id, permits_requested: ^permits}
                    }}
  end

  defp refute_granted(consumer_id) do
    refute_receive {:telemetry_event, %{event: @flow_control, metadata: %{consumer_id: ^consumer_id}}}
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
