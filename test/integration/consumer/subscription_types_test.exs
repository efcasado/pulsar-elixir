defmodule Pulsar.Integration.Consumer.SubscriptionTypesTest do
  use Pulsar.Test.Case, async: true

  @topic "persistent://public/default/subscription-types-test"
  @consumer_callback Pulsar.Test.Support.DummyConsumer
  @messages [
    {"key1", "Message 1 for key1"},
    {"key2", "Message 1 for key2"},
    {"key1", "Message 2 for key1"},
    {"key2", "Message 2 for key2"},
    {"key3", "Message 1 for key3"},
    {"key4", "Message 1 for key4"}
  ]

  setup_all do
    Utils.seed_topic(@topic, @messages, client: @client)

    {:ok, expected_count: length(@messages)}
  end

  test ":shared hands each consumer a share of the messages", %{expected_count: expected_count} do
    {:ok, shared_group} =
      Pulsar.Consumer.start(
        @topic,
        "shared",
        @consumer_callback,
        manual_flow_options(:shared, 2)
      )

    :ok = Pulsar.Consumer.await_ready(shared_group)
    [consumer1, consumer2] = Topology.workers(shared_group)

    # A permit each per round, so the broker has exactly one message to give each. Left to
    # itself, whichever consumer drained the backlog first would take all of it.
    rounds = div(expected_count, 2)

    for _round <- 1..rounds do
      :ok = Pulsar.Consumer.send_flow(consumer1, 1)
      :ok = Pulsar.Consumer.send_flow(consumer2, 1)

      assert_receive {:consumer, ^consumer1, _message}
      assert_receive {:consumer, ^consumer2, _message}
    end

    refute_receive {:consumer, _pid, _message}
  end

  test ":key_shared gives each consumer a set of keys no other one sees", %{expected_count: expected_count} do
    {:ok, key_shared_group} =
      Pulsar.Consumer.start(
        @topic,
        "key-shared",
        @consumer_callback,
        manual_flow_options(:key_shared, 2)
      )

    :ok = Pulsar.Consumer.await_ready(key_shared_group)
    [consumer1, consumer2] = Topology.workers(key_shared_group)

    # Granted only once both have subscribed, so the broker has split the hash range before it
    # dispatches anything. Earlier, and the first consumer takes keys that later belong to the
    # second.
    :ok = Pulsar.Consumer.send_flow(consumer1, expected_count)
    :ok = Pulsar.Consumer.send_flow(consumer2, expected_count)

    delivered = receive_messages(expected_count)
    messages1 = Map.get(delivered, consumer1, [])
    messages2 = Map.get(delivered, consumer2, [])

    assert length(messages1) + length(messages2) == expected_count

    extract_keys = fn messages ->
      messages
      |> Enum.map(&Pulsar.Message.key(&1))
      |> Enum.filter(&(&1 != nil))
      |> MapSet.new()
    end

    keys1 = extract_keys.(messages1)
    keys2 = extract_keys.(messages2)
    key_overlap = MapSet.intersection(keys1, keys2)
    assert MapSet.size(key_overlap) == 0
  end

  test ":failover delivers to one consumer and leaves the rest standing by", %{expected_count: expected_count} do
    {:ok, failover_group} =
      Pulsar.Consumer.start(
        @topic,
        "failover",
        @consumer_callback,
        subscription_options(:failover, 2)
      )

    :ok = Pulsar.Consumer.await_ready(failover_group)
    [consumer1, consumer2] = Topology.workers(failover_group)

    delivered = receive_messages(expected_count)

    assert [{active, messages}] = Map.to_list(delivered)
    assert length(messages) == expected_count
    assert active in [consumer1, consumer2]

    assert_receive {:consumer_active, ^active, true}
    refute_receive {:consumer_active, _pid, true}
  end

  test ":exclusive delivers everything to the one consumer it admits", %{expected_count: expected_count} do
    {:ok, exclusive_group} =
      Pulsar.Consumer.start(
        @topic,
        "exclusive",
        @consumer_callback,
        subscription_options(:exclusive, 1)
      )

    :ok = Pulsar.Consumer.await_ready(exclusive_group)
    [consumer] = Topology.workers(exclusive_group)

    for _message <- 1..expected_count, do: assert_receive({:consumer, ^consumer, _message})
    refute_receive {:consumer, ^consumer, _message}
  end

  test "exclusive subscription refuses a consumer count above one" do
    assert_raise ArgumentError, ~r/admits a single consumer/, fn ->
      Pulsar.Consumer.start(@topic, "exclusive-multi", @consumer_callback, subscription_options(:exclusive, 2))
    end
  end

  test "a second consumer of an exclusive subscription stops rather than waiting for it", %{broker: broker} do
    subscription = "exclusive-contended"
    contender_client = Utils.start_isolated_client(:exclusive_contender, broker)

    {:ok, holder} = Pulsar.Consumer.start(@topic, subscription, @consumer_callback, subscription_options(:exclusive, 1))
    :ok = Pulsar.Consumer.await_ready(holder, client: @client)

    {:ok, contender} =
      Pulsar.Consumer.start(
        @topic,
        subscription,
        @consumer_callback,
        [name: "exclusive-contender"] ++
          Keyword.put(subscription_options(:exclusive, 1), :client, contender_client)
      )

    ref = Process.monitor(contender)

    assert_receive {:DOWN, ^ref, :process, ^contender, :shutdown}, 10_000
    refute contender in Pulsar.Client.consumers(contender_client)

    assert Process.alive?(holder)
  end

  # Deliveries from the consumers of one subscription arrive interleaved, so they are grouped
  # by the worker that got each one.
  defp receive_messages(count) do
    for_result =
      for _message <- 1..count do
        assert_receive {:consumer, pid, message}
        {pid, message}
      end

    Enum.group_by(for_result, &elem(&1, 0), &elem(&1, 1))
  end

  defp subscription_options(type, count) do
    [
      client: @client,
      subscription_type: type,
      initial_position: :earliest,
      consumer_count: count,
      flow_initial: 1,
      flow_threshold: 0,
      flow_refill: 1,
      init_args: [forward_to: self()]
    ]
  end

  # Manual flow control granting nothing on subscribe, so the test can grant permits
  # explicitly once all consumers are subscribed.
  defp manual_flow_options(type, count) do
    [
      client: @client,
      subscription_type: type,
      initial_position: :earliest,
      consumer_count: count,
      flow_policy: {Pulsar.Test.Support.Flow, :never, []},
      flow_initial: 0,
      init_args: [forward_to: self()]
    ]
  end
end
