defmodule Pulsar.Integration.Client.EscalationTest do
  use ExUnit.Case, async: false

  alias Pulsar.Client
  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Topology

  @moduletag :integration

  @missing "persistent://public/default/escalation-test-missing"
  @real "persistent://public/default/escalation-test-real"

  defp host_tree(children, intensity \\ []) do
    start_supervised!(
      %{
        id: {:host_tree, :erlang.unique_integer([:positive])},
        start: {Supervisor, :start_link, [children, [strategy: :one_for_one] ++ intensity]},
        type: :supervisor
      },
      restart: :temporary
    )
  end

  defp declared_client(name, intensity) do
    consumer = [
      topic: @missing,
      subscription_name: "escalation-declared",
      callback_module: DummyConsumer,
      force_create_topic: false
    ]

    host_tree([{Client, name: name, host: System.broker().service_url, consumers: [consumer]}], intensity)
  end

  # A cascade takes about five seconds, most of it a client reconnecting, so a host allowing
  # three restarts in five never fills its budget. One in sixty fills on the second cascade.
  @tag timeout: 180_000
  test "a declared resource that cannot run reaches a host whose window outlasts a cascade" do
    host = declared_client(:escalation_wide, max_restarts: 1, max_seconds: 60)
    ref = Process.monitor(host)

    assert_receive {:DOWN, ^ref, :process, ^host, :shutdown}, 150_000
  end

  test "a declared resource that cannot run only rebuilds a host on OTP's own window" do
    host = declared_client(:escalation_default, max_restarts: 3, max_seconds: 5)
    ref = Process.monitor(host)

    refute_receive {:DOWN, ^ref, :process, ^host, _reason}, 20_000
  end

  # A worker waiting to start still reads its mailbox, so it is not killed after the timeout.
  test "a consumer still in its startup delay stops promptly" do
    host_tree([{Client, name: :escalation_delay, host: System.broker().service_url}])

    topic = "persistent://public/default/escalation-test-delayed"
    :ok = System.create_topic(topic)

    {:ok, consumer} =
      Pulsar.Consumer.start(topic, "escalation-delay", DummyConsumer,
        client: :escalation_delay,
        startup_delay_ms: 30_000
      )

    # The topology-only barrier returns once the worker exists, without waiting for its delayed
    # subscription to become ready.
    :ok = Topology.await_ready(consumer, 10_000)
    [worker] = Topology.workers(consumer)

    ref = Process.monitor(worker)

    started = :erlang.monotonic_time(:millisecond)
    :ok = Pulsar.Consumer.stop(consumer, client: :escalation_delay)
    assert_receive {:DOWN, ^ref, :process, ^worker, _reason}, 15_000
    elapsed = :erlang.monotonic_time(:millisecond) - started

    assert elapsed < 4_000, "took #{elapsed}ms; a worker asleep is killed after the 5000ms timeout"
  end

  # Bootstrap only recreates declared resources, so once the branch is rebuilt without this one
  # nothing is left failing and the climb ends at the client.
  test "a resource started at runtime takes its siblings, and stops at the client" do
    :ok = System.create_topic(@real)
    host_tree([{Client, name: :escalation_runtime, host: System.broker().service_url}])
    client = Process.whereis(:escalation_runtime)

    {:ok, healthy} = Pulsar.Consumer.start(@real, "escalation-healthy", DummyConsumer, client: :escalation_runtime)
    :ok = Pulsar.Consumer.await_ready(healthy)

    healthy_ref = Process.monitor(healthy)
    client_ref = Process.monitor(client)

    {:ok, doomed} =
      Pulsar.Consumer.start(@missing, "escalation-runtime", DummyConsumer,
        client: :escalation_runtime,
        force_create_topic: false
      )

    doomed_ref = Process.monitor(doomed)

    assert_receive {:DOWN, ^doomed_ref, :process, ^doomed, _reason}, 30_000
    assert_receive {:DOWN, ^healthy_ref, :process, ^healthy, _reason}, 30_000
    refute_receive {:DOWN, ^client_ref, :process, ^client, _reason}, 5_000

    assert Client.consumers(:escalation_runtime) == []
  end
end
