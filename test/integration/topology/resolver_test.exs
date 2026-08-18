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

    # Every test listening for this event is sent every client's, so the client picks out ours.
    assert_receive {:telemetry_event, %{event: @lookup, metadata: %{client: @client, success: true}}}
    refute_receive {:telemetry_event, %{event: @lookup, metadata: %{client: @client}}}
  end

  @tag telemetry_listen: [@lookup]
  test "reports a topic whose tenant or namespace does not exist" do
    assert {:error, _reason} = Resolver.lookup_topic("persistent://fake/fake/fake", client: @client)
    assert {:error, _reason} = Resolver.lookup_topic("persistent://public/fake/fake", client: @client)

    for _lookup <- 1..2 do
      assert_receive {:telemetry_event, %{event: @lookup, metadata: %{client: @client, success: false}}}
    end

    refute_receive {:telemetry_event, %{event: @lookup, metadata: %{client: @client}}}
  end
end
