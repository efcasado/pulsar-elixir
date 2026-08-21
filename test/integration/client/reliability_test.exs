defmodule Pulsar.Integration.Client.ReliabilityTest do
  use Pulsar.Test.Case, async: true

  alias Pulsar.Test.Support.DummyConsumer

  @topic "persistent://public/default/reliability-test-topic"
  @consumer_callback DummyConsumer

  setup_all do
    :ok = System.create_topic(@topic)
  end

  test "producer recovers from broker crash" do
    {:ok, group_pid} = Pulsar.Producer.start(@topic, producer_options())

    assert_worker_restarts(Pulsar.Producer, group_pid, fn producer ->
      Process.exit(broker(producer), :kill)
    end)
  end

  test "producer recovers from broker-initiated topic unload" do
    # NOTE: add support the ExtensibleLoadManager config in the future
    # It adds a reassignment url to skip topic lookup
    # See: https://github.com/apache/pulsar/blob/master/pip/pip-307.md
    topic = "persistent://public/default/producer-unload-test"

    {:ok, group_pid} = Pulsar.Producer.start(topic, producer_options())

    assert_worker_restarts(Pulsar.Producer, group_pid, fn _producer ->
      :ok = System.unload_topic(topic)
    end)

    Pulsar.Producer.stop(group_pid)
  end

  test "consumer recovers from broker crash" do
    {:ok, group_pid} =
      Pulsar.Consumer.start(
        @topic,
        "broker-crash",
        @consumer_callback,
        subscription_options()
      )

    assert_worker_restarts(Pulsar.Consumer, group_pid, fn consumer ->
      Process.exit(broker(consumer), :kill)
    end)
  end

  test "consumer recovers from broker-initiated topic unload" do
    {:ok, group_pid} =
      Pulsar.Consumer.start(
        @topic,
        "topic-unload",
        DummyConsumer,
        subscription_options()
      )

    assert_worker_restarts(Pulsar.Consumer, group_pid, fn _consumer ->
      :ok = System.unload_topic(@topic)
    end)
  end

  # A broker that reconnects does not die: it stays up and exits its workers to make them
  # subscribe again. Both trap exits, so that signal arrives as a message they have to answer -
  # a worker that outlives it holds a subscription the broker has forgotten.
  for {facade, callback} <- [{Pulsar.Consumer, DummyConsumer}, {Pulsar.Producer, nil}] do
    test "#{inspect(facade)} restarts a worker its broker exits to reconnect" do
      {:ok, group} = start_resource(unquote(facade), unquote(callback), "reconnect-exit")

      assert_worker_restarts(unquote(facade), group, fn worker ->
        send(worker, {:EXIT, broker(worker), :broker_disconnected})
      end)
    end
  end

  defp start_resource(Pulsar.Consumer, callback, subscription) do
    Pulsar.Consumer.start(@topic, subscription, callback, subscription_options())
  end

  defp start_resource(Pulsar.Producer, _callback, name) do
    Pulsar.Producer.start(@topic, Keyword.put(producer_options(), :name, name))
  end

  # A ready worker has already resolved its broker, which is what the restart below needs.
  defp assert_worker_restarts(facade, group, restart) do
    :ok = facade.await_ready(group)
    [before] = Topology.workers(group)

    ref = Process.monitor(before)
    restart.(before)
    assert_receive {:DOWN, ^ref, :process, ^before, _reason}

    [after_restart] =
      Utils.wait_for(fn -> Topology.workers(group) end,
        until: &match?([worker] when worker != before, &1),
        description: "replacement topology worker to start"
      )

    refute Process.alive?(before)
    assert Process.alive?(group)
    assert Process.alive?(after_restart)
    assert before != after_restart
  end

  defp broker(worker) do
    %{broker_pid: broker} = :sys.get_state(worker)
    broker
  end

  defp subscription_options do
    [
      client: @client
    ]
  end

  defp producer_options do
    [
      client: @client
    ]
  end
end
