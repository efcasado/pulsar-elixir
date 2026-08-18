defmodule Pulsar.Integration.Topology.ResolverTest do
  use Pulsar.Test.Case, async: true

  alias Pulsar.Topology.Resolver

  @topic "persistent://public/default/topology-resolver-test-topic"
  @lookup [:pulsar, :topology, :resolver, :lookup_topic, :stop]

  setup_all do
    :ok = System.create_topic(@topic)
  end

  @tag telemetry_listen: [@lookup]
  test "resolves a topic to the broker serving it" do
    assert {:ok, broker_pid} = Resolver.lookup_topic(@topic, client: @client)
    assert Process.alive?(broker_pid)

    assert %{success_count: 1, failure_count: 0, total_count: 1} =
             Utils.collect_stats(@lookup, client: @client)
  end

  @tag telemetry_listen: [@lookup]
  test "reports a topic whose tenant or namespace does not exist" do
    assert {:error, _reason} = Resolver.lookup_topic("persistent://fake/fake/fake", client: @client)
    assert {:error, _reason} = Resolver.lookup_topic("persistent://public/fake/fake", client: @client)

    assert %{success_count: 0, failure_count: 2, total_count: 2} =
             Utils.collect_stats(@lookup, client: @client)
  end
end
