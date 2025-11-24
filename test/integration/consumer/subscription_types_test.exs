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

    {:ok, client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    {:ok, producer_pid} =
      Pulsar.start_producer(
        @topic,
        client: @client,
        name: :subscription_types_producer,
        startup_delay_ms: 0,
        startup_jitter_ms: 0
      )

    for {key, payload} <- @messages do
      Pulsar.send(:subscription_types_producer, payload, partition_key: key, client: @client)
    end

    on_exit(fn ->
      if Process.alive?(producer_pid), do: Pulsar.stop_producer(producer_pid)
      if Process.alive?(client_pid), do: Supervisor.stop(client_pid, :normal)
    end)

    {:ok, expected_count: Enum.count(@messages)}
  end

  test "shared subscription distributes messages across consumers", %{expected_count: expected_count} do
    {:ok, _shared_group} =
      Pulsar.start_consumer(
        @topic,
        "shared",
        @consumer_callback,
        subscription_options(:Shared, 2)
      )

    [consumer1, consumer2] = wait_for_consumer_ready(2)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer1) +
        @consumer_callback.count_messages(consumer2) ==
        expected_count
    end)

    count1 = @consumer_callback.count_messages(consumer1)
    count2 = @consumer_callback.count_messages(consumer2)

    assert count1 + count2 == expected_count
    assert count1 > 0
    assert count2 > 0
  end

  test "key_shared subscription partitions by key", %{expected_count: expected_count} do
    opts = subscription_options(:Key_Shared, 2)

    {:ok, _key_shared_group} =
      Pulsar.start_consumer(
        @topic,
        "key-shared",
        @consumer_callback,
        opts
      )

    [consumer1, consumer2] = wait_for_consumer_ready(2)

    Utils.wait_for(fn ->
      @consumer_callback.count_messages(consumer1) +
        @consumer_callback.count_messages(consumer2) ==
        expected_count
    end)

    messages1 = @consumer_callback.get_messages(consumer1)
    messages2 = @consumer_callback.get_messages(consumer2)
    count1 = length(messages1)
    count2 = length(messages2)

    assert count1 + count2 == expected_count
    assert count1 > 0
    assert count2 > 0

    # Extract partition keys to verify no key overlap
    extract_keys = fn messages ->
      messages
      |> Enum.map(& &1.partition_key)
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

    [consumer1, consumer2] = wait_for_consumer_ready(2)

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

    [consumer] = wait_for_consumer_ready(1)

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
      startup_delay_ms: 0,
      startup_jitter_ms: 0,
      flow_initial: 1,
      flow_threshold: 0,
      flow_refill: 1,
      init_args: [notify_pid: self()]
    ]
  end

  def wait_for_consumer_ready(count, timeout \\ 5000) do
    Enum.map(1..count, fn _ ->
      receive do
        {:consumer_ready, pid} -> pid
      after
        timeout -> flunk("Timeout waiting for consumer to be ready")
      end
    end)
  end
end
