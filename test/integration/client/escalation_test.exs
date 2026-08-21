defmodule Pulsar.Integration.Client.EscalationTest do
  use ExUnit.Case, async: false

  alias Pulsar.Client
  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System

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

  # One cascade takes about five seconds, most of it a client reconnecting and bootstrapping, so
  # a host allowing three restarts in five of them never fills its budget. One restart in sixty
  # fills on the second cascade, and waits for two rather than four of them to do it.
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
