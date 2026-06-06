defmodule Pulsar.Integration.Consumer.SubscriptionTypesTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :subscription_types_test_client
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
        name: :subscription_types_producer
      )

    for {key, payload} <- @messages do
      Pulsar.send(:subscription_types_producer, payload, partition_key: key, client: @client)
    end

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)

    {:ok, expected_count: Enum.count(@messages)}
  end

  test "shared subscription distributes messages across consumers", %{expected_count: expected_count} do
    {:ok, _shared_group} =
      Pulsar.start_consumer(
        @topic,
        "shared",
        @consumer_callback,
        manual_flow_options(:Shared, 2)
      )

    [consumer1, consumer2] = Utils.wait_for_consumer_ready(2)

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

  test "key_shared subscription partitions by key", %{expected_count: expected_count} do
    {:ok, _key_shared_group} =
      Pulsar.start_consumer(
        @topic,
        "key-shared",
        @consumer_callback,
        manual_flow_options(:Key_Shared, 2)
      )

    [consumer1, consumer2] = Utils.wait_for_consumer_ready(2)

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

    # Extract partition keys to verify no key overlap
    extract_keys = fn messages ->
      messages
      |> Enum.map(& &1.metadata.partition_key)
      |> Enum.filter(&(&1 != nil))
      |> MapSet.new()
    end

    keys1 = extract_keys.(messages1)
    keys2 = extract_keys.(messages2)
    key_overlap = MapSet.intersection(keys1, keys2)
    assert MapSet.size(key_overlap) == 0
  end

  test "failover subscription uses single active consumer", %{expected_count: expected_count} do
    {:ok, _failover_group} =
      Pulsar.start_consumer(
        @topic,
        "failover",
        @consumer_callback,
        subscription_options(:Failover, 2)
      )

    [consumer1, consumer2] = Utils.wait_for_consumer_ready(2)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer1) +
        @consumer_callback.count_messages(consumer2) == expected_count
    end)

    count1 = @consumer_callback.count_messages(consumer1)
    count2 = @consumer_callback.count_messages(consumer2)

    assert count1 + count2 == expected_count

    assert (count1 == expected_count and count2 == 0) or
             (count1 == 0 and count2 == expected_count)
  end

  test "exclusive subscription receives all messages", %{expected_count: expected_count} do
    {:ok, _exclusive_group} =
      Pulsar.start_consumer(
        @topic,
        "exclusive",
        @consumer_callback,
        subscription_options(:Exclusive, 1)
      )

    [consumer] = Utils.wait_for_consumer_ready(1)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer) == expected_count
    end)

    count = @consumer_callback.count_messages(consumer)
    assert count == expected_count
  end

  test "exclusive subscription fails with multiple consumers" do
    {:ok, exclusive_multi_group} =
      Pulsar.start_consumer(
        @topic,
        "exclusive-multi",
        @consumer_callback,
        subscription_options(:Exclusive, 2)
      )

    Utils.wait_for(fn -> not Process.alive?(exclusive_multi_group) end)

    assert Process.alive?(exclusive_multi_group) == false
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
      init_args: [notify_pid: self()]
    ]
  end

  # Manual flow control (flow_initial: 0 disables automatic permits), so the
  # test can grant permits explicitly once all consumers are subscribed.
  defp manual_flow_options(type, count) do
    [
      client: @client,
      subscription_type: type,
      initial_position: :earliest,
      consumer_count: count,
      flow_initial: 0,
      init_args: [notify_pid: self()]
    ]
  end
end
