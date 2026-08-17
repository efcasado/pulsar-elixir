defmodule Pulsar.Integration.Consumer.StopTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :consumer_stop_test_client
  @declared_client :consumer_stop_test_declared_client
  @topic "persistent://public/default/consumer-stop-test"

  setup_all do
    broker = System.broker()
    {:ok, _client} = Pulsar.Client.start_link(name: @client, host: broker.service_url)
    on_exit(fn -> Pulsar.Client.stop(@client) end)
    :ok
  end

  test "stopping by pid removes the consumer rather than restarting it" do
    topic = @topic <> "-pid"
    consumer = start_consumer(topic, "pid-sub")
    Utils.wait_for_consumer_ready(1)

    assert Pulsar.Consumer.stop(consumer, client: @client) == :ok

    refute_receive {:consumer_ready, _pid}, 3_000
    assert_removed(consumer, topic, "pid-sub")
    assert Pulsar.Consumer.stop(consumer, client: @client) == {:error, :not_found}
  end

  test "stopping by pid without naming a client removes it just the same" do
    topic = @topic <> "-no-client"
    consumer = start_consumer(topic, "no-client-sub")

    assert Pulsar.Consumer.stop(consumer) == :ok

    assert_removed(consumer, topic, "no-client-sub")
  end

  test "stopping by registered name removes it just the same" do
    topic = @topic <> "-name"
    consumer = start_consumer(topic, "name-sub")

    assert Pulsar.Consumer.stop(name(topic, "name-sub"), client: @client) == :ok

    assert_removed(consumer, topic, "name-sub")
    assert Pulsar.Consumer.stop(name(topic, "name-sub"), client: @client) == {:error, :not_found}
  end

  test "stopping a partitioned consumer takes every partition with it" do
    topic = @topic <> "-partitioned"
    :ok = System.create_topic(topic, 3)
    consumer = start_consumer(topic, "partitioned-sub", create_topic?: false)

    workers = Pulsar.Topology.workers(consumer)
    assert length(workers) == 3

    assert Pulsar.Consumer.stop(consumer, client: @client) == :ok

    for worker <- workers, do: refute(Process.alive?(worker), "a partition worker outlived its consumer")
    assert_removed(consumer, topic, "partitioned-sub")
  end

  test "leaves the other consumers on the same client alone" do
    kept = start_consumer(@topic <> "-kept", "kept-sub")
    doomed = start_consumer(@topic <> "-doomed", "doomed-sub")

    assert Pulsar.Consumer.stop(doomed, client: @client) == :ok

    assert kept in Pulsar.Client.consumers(@client)
    assert lookup(@topic <> "-kept", "kept-sub") == {:ok, kept}
    assert Pulsar.Consumer.stop(kept, client: @client) == :ok
  end

  test "releases the subscription on the broker, so an exclusive one can be taken again" do
    topic = @topic <> "-exclusive"
    consumer = start_consumer(topic, "exclusive-sub", subscription_type: :exclusive)

    assert Pulsar.Consumer.stop(consumer, client: @client) == :ok

    replacement = start_consumer(topic, "exclusive-sub", create_topic?: false, subscription_type: :exclusive)

    assert Pulsar.Consumer.stop(replacement, client: @client) == :ok
  end

  test "a consumer declared on the client is not brought back either" do
    topic = @topic <> "-declared"
    :ok = System.create_topic(topic)
    broker = System.broker()

    {:ok, _client} =
      Pulsar.Client.start_link(
        name: @declared_client,
        host: broker.service_url,
        consumers: [
          [
            topic: topic,
            subscription_name: "declared-sub",
            callback_module: DummyConsumer,
            init_args: [notify_pid: self()]
          ]
        ]
      )

    on_exit(fn -> Pulsar.Client.stop(@declared_client) end)

    :ok = Pulsar.Consumer.await_ready(name(topic, "declared-sub"), client: @declared_client)
    Utils.wait_for_consumer_ready(1)

    assert Pulsar.Consumer.stop(name(topic, "declared-sub"), client: @declared_client) == :ok

    refute_receive {:consumer_ready, _pid}, 3_000
    assert Pulsar.Client.consumers(@declared_client) == []
  end

  ## Helpers

  defp start_consumer(topic, subscription, opts \\ []) do
    {create_topic?, opts} = Keyword.pop(opts, :create_topic?, true)
    if create_topic?, do: :ok = System.create_topic(topic)

    opts = Keyword.merge([client: @client, init_args: [notify_pid: self()]], opts)

    {:ok, consumer} = Pulsar.Consumer.start(topic, subscription, DummyConsumer, opts)
    :ok = Pulsar.Consumer.await_ready(consumer, client: Keyword.fetch!(opts, :client))

    consumer
  end

  defp assert_removed(consumer, topic, subscription) do
    refute Process.alive?(consumer)
    refute consumer in Pulsar.Client.consumers(@client)

    Utils.wait_for(fn -> lookup(topic, subscription) end,
      until: &(&1 == {:error, :not_found}),
      timeout: 2_000,
      description: "#{name(topic, subscription)} to be unregistered"
    )
  end

  defp name(topic, subscription), do: "#{topic}-#{subscription}"

  defp lookup(topic, subscription), do: Pulsar.Client.lookup(:consumers, name(topic, subscription), @client)
end
