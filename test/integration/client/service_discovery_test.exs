defmodule Pulsar.Integration.Client.ServiceDiscoveryTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.ServiceDiscovery
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  require Logger

  @moduletag :integration
  @client :service_discovery_test_client
  @topic "persistent://public/default/service-discovery-test-topic"

  setup_all do
    broker = System.broker()

    System.create_topic(@topic)

    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)
  end

  setup [:telemetry_listen]

  @tag telemetry_listen: [[:pulsar, :service_discovery, :lookup_topic, :stop]]
  test "topic lookup retrieves broker PID" do
    assert {:ok, broker_pid} = ServiceDiscovery.lookup_topic(@topic, client: @client)
    assert Process.alive?(broker_pid)

    stats = Utils.collect_lookup_stats(client: @client)
    assert %{success_count: 1, failure_count: 0, total_count: 1} = stats
  end

  @tag telemetry_listen: [[:pulsar, :service_discovery, :lookup_topic, :stop]]
  test "topic lookup fails for non-existing topics" do
    result1 = ServiceDiscovery.lookup_topic("persistent://fake/fake/fake", client: @client)
    assert {:error, _reason} = result1

    result2 = ServiceDiscovery.lookup_topic("persistent://public/fake/fake", client: @client)
    assert {:error, _reason} = result2

    stats = Utils.collect_lookup_stats(client: @client)
    assert %{success_count: 0, failure_count: 2, total_count: 2} = stats
  end
end
