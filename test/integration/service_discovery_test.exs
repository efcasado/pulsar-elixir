defmodule Pulsar.Integration.ServiceDiscoveryTest do
  use ExUnit.Case
  import TelemetryTest

  require Logger

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils
  alias Pulsar.ServiceDiscovery

  @moduletag :integration
  @topic "persistent://public/default/service-discovery-test-topic"

  setup do
    broker = System.broker()

    config = [
      host: broker.service_url
    ]

    {:ok, app_pid} = Pulsar.start(config)

    on_exit(fn ->
      Process.exit(app_pid, :shutdown)
      Utils.wait_for(fn -> not Process.alive?(app_pid) end)
    end)
  end

  setup [:telemetry_listen]

  describe "Topic Lookup" do
    @tag telemetry_listen: [[:pulsar, :service_discovery, :lookup_topic, :stop]]
    test "topic lookup retrieves broker PID" do
      topic = @topic <> "-#{:erlang.unique_integer([:positive])}"

      {:ok, broker_pid} = ServiceDiscovery.lookup_topic(topic)

      assert Process.alive?(broker_pid)

      # Verify 1 lookup with success result
      stats = Utils.collect_lookup_stats()
      assert %{success_count: 1, failure_count: 0, total_count: 1} = stats
    end
  end
end
