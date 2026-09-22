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
  test "a window of one is topped up on every message", %{
    expected_count: expected_count
  } do
    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        @topic,
        "tiny-permits",
        @consumer_callback,
        subscription_options(1, 0, 1)
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [consumer] = Topology.workers(consumer_group)

    for _message <- 1..expected_count, do: assert_receive({:consumer, ^consumer, _message})

    consumer_id = consumer |> :sys.get_state() |> Map.get(:consumer_id)

    # One permit up front and one back per message consumed, so seven grants of one.
    for _grant <- 1..7, do: assert_granted(consumer_id, 1)
    refute_granted(consumer_id)
  end

  @tag telemetry_listen: [@flow_control]
  test "is topped up once the window falls to its threshold", %{
    expected_count: expected_count
  } do
    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        @topic,
        "threshold-test",
        @consumer_callback,
        subscription_options(5, 3, 4)
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [consumer] = Topology.workers(consumer_group)

    for _message <- 1..expected_count, do: assert_receive({:consumer, ^consumer, _message})

    consumer_id = consumer |> :sys.get_state() |> Map.get(:consumer_id)

    # Five up front, then a refill of four each time the window falls to the threshold of three.
    assert_granted(consumer_id, 5)
    assert_granted(consumer_id, 4)
    assert_granted(consumer_id, 4)
    refute_granted(consumer_id)
  end

  test "grants nothing until the caller asks, when it starts with no window", %{expected_count: expected_count} do
    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        @topic,
        "manual-flow",
        @consumer_callback,
        subscription_options(0, 0, 0)
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [consumer] = Topology.workers(consumer_group)

    refute_receive {:consumer, ^consumer, _message}, 500

    :ok = Pulsar.Consumer.send_flow(consumer, 3)

    for _message <- 1..3, do: assert_receive({:consumer, ^consumer, _message})
    refute_receive {:consumer, ^consumer, _message}

    :ok = Pulsar.Consumer.send_flow(consumer, expected_count - 3)

    for _message <- 1..(expected_count - 3), do: assert_receive({:consumer, ^consumer, _message})
  end

  test "granting permits through the root pid reaches its worker" do
    {:ok, consumer_root} =
      Pulsar.Consumer.start(@topic, "root-flow", @consumer_callback, subscription_options(0, 0, 0))

    :ok = Pulsar.Consumer.await_ready(consumer_root)
    [worker] = Topology.workers(consumer_root)

    # The pid start/1 returns is a supervisor, which cannot answer the worker's call.
    assert :ok = Pulsar.Consumer.send_flow(consumer_root, 2)

    assert Process.alive?(consumer_root)
    assert Process.alive?(worker)

    assert_receive {:consumer, ^worker, _message}

    Pulsar.Consumer.stop(consumer_root)
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

  defp subscription_options(initial, threshold, refill) do
    [
      client: @client,
      initial_position: :earliest,
      flow_policy: if(initial == 0, do: {Pulsar.Test.Support.Flow, :never, []}, else: :auto),
      flow_initial: initial,
      flow_threshold: threshold,
      flow_refill: refill,
      init_args: [forward_to: self()]
    ]
  end
end
