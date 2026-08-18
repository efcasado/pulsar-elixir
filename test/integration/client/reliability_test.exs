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

    assert_worker_restarts(group_pid, fn producer ->
      broker =
        Utils.wait_for(fn -> broker(producer) end,
          until: &is_pid/1,
          description: "producer to connect to a broker"
        )

      Process.exit(broker, :kill)
    end)
  end

  test "producer recovers from broker-initiated topic unload" do
    # NOTE: add support the ExtensibleLoadManager config in the future
    # It adds a reassignment url to skip topic lookup
    # See: https://github.com/apache/pulsar/blob/master/pip/pip-307.md
    topic = "persistent://public/default/producer-unload-test"

    {:ok, group_pid} = Pulsar.Producer.start(topic, producer_options())

    assert_worker_restarts(group_pid, fn producer ->
      Utils.wait_for(fn -> :sys.get_state(producer).ready end)
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

    assert_worker_restarts(group_pid, fn consumer ->
      broker =
        Utils.wait_for(fn -> broker(consumer) end,
          until: &is_pid/1,
          description: "consumer to connect to a broker"
        )

      Process.exit(broker, :kill)
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

    assert_worker_restarts(group_pid, fn _consumer ->
      :ok = System.unload_topic(@topic)
    end)
  end

  defp assert_worker_restarts(group, restart) do
    [before] =
      Utils.wait_for(fn -> Topology.workers(group) end,
        until: &match?([_worker], &1),
        description: "topology worker to start"
      )

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
    case :sys.get_state(worker) do
      %{broker_pid: broker} when is_pid(broker) -> broker
      _state -> nil
    end
  catch
    :exit, _reason -> nil
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
