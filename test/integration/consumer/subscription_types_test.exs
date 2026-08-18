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

    # With manual flow control, grant one permit to each consumer per round so
    # the broker dispatches exactly one message to each. This makes the Shared
    # distribution deterministic (verifying round-robin) instead of racing on
    # which consumer drains the pre-produced backlog first.
    rounds = div(expected_count, 2)

    for round <- 1..rounds do
      :ok = Pulsar.Consumer.send_flow(consumer1, 1)
      :ok = Pulsar.Consumer.send_flow(consumer2, 1)

      Utils.wait_for(fn ->
        @consumer_callback.count_messages(consumer1) == round and
          @consumer_callback.count_messages(consumer2) == round
      end)
    end

    assert @consumer_callback.count_messages(consumer1) == rounds
    assert @consumer_callback.count_messages(consumer2) == rounds
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

    # With manual flow control, only grant permits once BOTH consumers are
    # subscribed, so Key_Shared hash ranges are split between them before any
    # message is dispatched. Otherwise the first consumer can drain backlog for
    # keys that later belong to the second consumer's range, causing key overlap.
    :ok = Pulsar.Consumer.send_flow(consumer1, expected_count)
    :ok = Pulsar.Consumer.send_flow(consumer2, expected_count)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer1) +
        @consumer_callback.count_messages(consumer2) ==
        expected_count
    end)

    messages1 = @consumer_callback.get_messages(consumer1)
    messages2 = @consumer_callback.get_messages(consumer2)

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

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer1) +
        @consumer_callback.count_messages(consumer2) == expected_count
    end)

    count1 = @consumer_callback.count_messages(consumer1)
    count2 = @consumer_callback.count_messages(consumer2)

    assert count1 + count2 == expected_count

    assert (count1 == expected_count and count2 == 0) or
             (count1 == 0 and count2 == expected_count)

    {active_consumer, passive_consumer} =
      if count1 == expected_count, do: {consumer1, consumer2}, else: {consumer2, consumer1}

    assert @consumer_callback.active?(active_consumer) == true
    assert @consumer_callback.active?(passive_consumer) == false
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

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == expected_count
    end)

    count = @consumer_callback.count_messages(consumer)
    assert count == expected_count
  end

  # An :exclusive subscription admits one consumer, so the workers past the first are refused
  # and stop instead of restarting against a slot that will not free up. The one that got the
  # subscription keeps running. Use :failover if the others should stand by.
  test "exclusive subscription keeps only the consumer that got the subscription" do
    {:ok, exclusive_multi_group} =
      Pulsar.Consumer.start(
        @topic,
        "exclusive-multi",
        @consumer_callback,
        subscription_options(:exclusive, 2)
      )

    assert Process.alive?(exclusive_multi_group)

    assert [_worker] =
             Utils.wait_for(fn -> Topology.workers(exclusive_multi_group) end,
               until: &match?([_worker], &1)
             )
  end

  defp subscription_options(type, count) do
    [
      client: @client,
      subscription_type: type,
      initial_position: :earliest,
      consumer_count: count,
      flow_initial: 1,
      flow_threshold: 0,
      flow_refill: 1
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
      flow_initial: 0
    ]
  end
end
