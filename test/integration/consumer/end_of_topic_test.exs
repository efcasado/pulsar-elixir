defmodule Pulsar.Integration.Consumer.EndOfTopicTest do
  use Pulsar.Test.Case, async: true

  alias Pulsar.Test.Support.DummyConsumer

  @topic "persistent://public/default/consumer-end-of-topic-test"
  @messages ["msg-1", "msg-2", "msg-3"]

  test "tells a consumer that has drained a terminated topic" do
    topic = @topic <> "-drained"
    consumer = start_consumer(topic, "drained-sub")
    [worker] = Topology.workers(consumer)

    drain()
    :ok = System.terminate_topic(topic)

    assert_receive {:consumer_end_of_topic, ^worker}, 10_000
    assert Process.alive?(worker)
  end

  test "leaves the consumer and its connection alive" do
    topic = @topic <> "-alive"
    consumer = start_consumer(topic, "alive-sub")

    drain()
    :ok = System.terminate_topic(topic)
    assert_receive {:consumer_end_of_topic, _worker}, 10_000

    # A terminated topic still admits a new subscription, which a downed connection could not.
    :ok = Pulsar.Consumer.stop(consumer, client: @client)
    start_consumer(topic, "alive-replay-sub", create_topic?: false, seed?: false)

    drain()
  end

  test "lets a callback finish its transient worker without stopping the resource" do
    topic = @topic <> "-worker-finished"

    consumer =
      start_consumer(topic, "worker-finished-sub", init_args: [finish_at_end_of_topic: true])

    [worker] = Topology.workers(consumer)
    assert_receive {:consumer_started, ^worker, _context}
    worker_ref = Process.monitor(worker)

    drain()
    :ok = System.terminate_topic(topic)

    assert_receive {:consumer_end_of_topic, ^worker}, 10_000
    assert_receive {:DOWN, ^worker_ref, :process, ^worker, :normal}, 5_000
    assert_receive {:consumer_terminated, ^worker, :normal}, 5_000

    assert Process.alive?(consumer)
    assert Topology.workers(consumer) == []
    refute_receive {:consumer_started, _replacement, _context}, 500
  end

  # A callback stop finishes only its worker, so this one tells Pulsar.Consumer.stop/2 instead -
  # which is what the whole consumer coming down proves, and terminate/2 running with it.
  test "lets a callback have its consumer stopped once the topic is done" do
    topic = @topic <> "-stopping"
    consumer = start_consumer(topic, "stopping-sub", init_args: [stop_at_end_of_topic: true])
    [worker] = Topology.workers(consumer)

    drain()

    ref = Process.monitor(worker)
    :ok = System.terminate_topic(topic)

    assert_receive {:consumer_end_of_topic, ^worker}, 10_000
    assert_receive {:DOWN, ^ref, :process, ^worker, :shutdown}, 5_000
    assert_receive {:consumer_terminated, ^worker, :shutdown}, 5_000
  end

  # Regression for #198: what is stopped stays stopped, and leaves nothing registered behind.
  test "leaves a consumer that was stopped at the end of the topic stopped" do
    topic = @topic <> "-stays-stopped"
    consumer = start_consumer(topic, "stays-stopped-sub", init_args: [stop_at_end_of_topic: true])
    [worker] = Topology.workers(consumer)

    drain()

    ref = Process.monitor(consumer)
    :ok = System.terminate_topic(topic)

    assert_receive {:consumer_end_of_topic, ^worker}, 10_000

    assert_receive {:DOWN, ^ref, :process, ^consumer, :shutdown}, 10_000
    refute consumer in Pulsar.Client.consumers(@client)
  end

  # One partition reaching its end is that partition's news. The consumer goes on serving the
  # others until something decides it is finished, which is the caller's decision to make.
  test "keeps draining the other partitions when one of them reaches its end" do
    topic = @topic <> "-partitioned-stopping"
    :ok = System.create_topic(topic, 2)

    consumer = start_consumer(topic, "partitioned-stopping-sub", create_topic?: false)

    partitions = Map.new(Topology.partitions(consumer))
    ending = Map.fetch!(partitions, 0)
    surviving = Map.fetch!(partitions, 1)

    drain()

    :ok = System.terminate_topic(topic <> "-partition-0")
    assert_receive {:consumer_end_of_topic, ^ending}, 10_000

    assert Process.alive?(consumer)
    Utils.seed_topic(topic <> "-partition-1", ["after-the-other-ended"], client: @client)
    assert_receive {:consumer, ^surviving, %Pulsar.Message{payload: "after-the-other-ended"}}, 10_000

    consumer_ref = Process.monitor(consumer)
    :ok = Pulsar.Consumer.stop(consumer, client: @client)

    assert_receive {:DOWN, ^consumer_ref, :process, ^consumer, _reason}, 10_000
  end

  test "tells every consumer on a shared subscription" do
    topic = @topic <> "-shared"
    :ok = System.create_topic(topic)
    Utils.seed_topic(topic, @messages, client: @client)

    workers = start_subscription_consumers(topic, "shared-sub", :shared, 3)

    drain()
    :ok = System.terminate_topic(topic)

    for worker <- workers, do: assert_receive({:consumer_end_of_topic, ^worker}, 10_000)
  end

  test "tells the passive consumer of a failover subscription too" do
    topic = @topic <> "-failover"
    :ok = System.create_topic(topic)
    Utils.seed_topic(topic, @messages, client: @client)

    workers = start_subscription_consumers(topic, "failover-sub", :failover, 2)

    drain()
    :ok = System.terminate_topic(topic)

    for worker <- workers, do: assert_receive({:consumer_end_of_topic, ^worker}, 10_000)
  end

  test "tells each partition's consumer separately" do
    topic = @topic <> "-partitioned"
    :ok = System.create_topic(topic, 3)
    consumer = start_consumer(topic, "partitioned-sub", create_topic?: false)
    workers = Topology.workers(consumer)
    assert length(workers) == 3

    drain()
    :ok = System.terminate_topic(topic, partitioned?: true)

    for worker <- workers, do: assert_receive({:consumer_end_of_topic, ^worker}, 10_000)
  end

  ## Helpers

  defp start_consumer(topic, subscription, opts \\ []) do
    {create_topic?, opts} = Keyword.pop(opts, :create_topic?, true)
    {seed?, opts} = Keyword.pop(opts, :seed?, true)
    if create_topic?, do: :ok = System.create_topic(topic)
    if seed?, do: Utils.seed_topic(topic, @messages, client: @client)

    {init_args, opts} = Keyword.pop(opts, :init_args, [])
    opts = Keyword.merge([client: @client, initial_position: :earliest], opts)
    opts = Keyword.put(opts, :init_args, [forward_to: self()] ++ init_args)

    {:ok, consumer} = Pulsar.Consumer.start(topic, subscription, DummyConsumer, opts)
    :ok = Pulsar.Consumer.await_ready(consumer, client: @client)

    consumer
  end

  defp start_subscription_consumers(topic, subscription, type, count) do
    for index <- 1..count do
      root =
        start_consumer(topic, subscription,
          create_topic?: false,
          seed?: false,
          name: "#{subscription}-#{index}",
          subscription_type: type
        )

      [worker] = Topology.workers(root)
      worker
    end
  end

  defp drain do
    for payload <- @messages do
      assert_receive {:consumer, _worker, %Pulsar.Message{payload: ^payload}}, 10_000
    end
  end
end
