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

  test "lets the callback stop the worker" do
    topic = @topic <> "-stopping"
    consumer = start_consumer(topic, "stopping-sub", init_args: [stop_at_end_of_topic: true])
    [worker] = Topology.workers(consumer)

    drain()

    ref = Process.monitor(worker)
    :ok = System.terminate_topic(topic)

    assert_receive {:consumer_end_of_topic, ^worker}, 10_000
    assert_receive {:DOWN, ^ref, :process, ^worker, :shutdown}, 5_000

    # The parent terminates a stopped worker rather than it exiting, so the worker has to trap
    # exits for its own terminate/2 - and the callback's - to run at all.
    assert_receive {:consumer_terminated, ^worker, _reason}, 5_000
  end

  # Regression for #198.
  test "leaves a consumer that stopped at the end of the topic stopped" do
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

  test "keeps draining the other partitions when one of them stops at its end" do
    topic = @topic <> "-partitioned-stopping"
    :ok = System.create_topic(topic, 2)

    consumer =
      start_consumer(topic, "partitioned-stopping-sub",
        create_topic?: false,
        init_args: [stop_at_end_of_topic: true]
      )

    groups = Map.new(Topology.groups(consumer))
    [ending] = partition_workers(groups, 0)
    [surviving] = partition_workers(groups, 1)

    drain()

    ending_ref = Process.monitor(ending)
    :ok = System.terminate_topic(topic <> "-partition-0")
    assert_receive {:DOWN, ^ending_ref, :process, ^ending, :shutdown}, 10_000

    assert Process.alive?(consumer)
    Utils.seed_topic(topic <> "-partition-1", ["after-the-other-ended"], client: @client)
    assert_receive {:consumer, ^surviving, %Pulsar.Message{payload: "after-the-other-ended"}}, 10_000

    consumer_ref = Process.monitor(consumer)
    :ok = System.terminate_topic(topic <> "-partition-1")

    assert_receive {:DOWN, ^consumer_ref, :process, ^consumer, :shutdown}, 10_000
  end

  test "tells every consumer on a shared subscription" do
    topic = @topic <> "-shared"
    consumer = start_consumer(topic, "shared-sub", consumer_count: 3, subscription_type: :shared)
    workers = Topology.workers(consumer)
    assert length(workers) == 3

    drain()
    :ok = System.terminate_topic(topic)

    for worker <- workers, do: assert_receive({:consumer_end_of_topic, ^worker}, 10_000)
  end

  test "tells the passive consumer of a failover subscription too" do
    topic = @topic <> "-failover"
    consumer = start_consumer(topic, "failover-sub", consumer_count: 2, subscription_type: :failover)
    workers = Topology.workers(consumer)
    assert length(workers) == 2

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

  defp partition_workers(groups, index) do
    for {_id, worker, :worker, _modules} <- Supervisor.which_children(Map.fetch!(groups, index)), do: worker
  end

  defp drain do
    for payload <- @messages do
      assert_receive {:consumer, _worker, %Pulsar.Message{payload: ^payload}}, 10_000
    end
  end
end
