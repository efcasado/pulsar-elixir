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
      stats = lookup_stats()
      assert %{success_count: 1, failure_count: 0, total_count: 1} = stats
    end
  end

  # Helper function to collect and aggregate lookup statistics from telemetry events
  defp lookup_stats do
    collect_lookup_events([]) |> aggregate_lookup_stats()
  end

  defp collect_lookup_events(acc) do
    receive do
      {:telemetry_event,
       %{
         event: [:pulsar, :service_discovery, :lookup_topic, :stop],
         measurements: measurements,
         metadata: metadata
       }} ->
        event = Map.merge(measurements, metadata)
        collect_lookup_events([event | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp aggregate_lookup_stats(events) do
    %{
      total_count: length(events),
      success_count: Enum.count(events, &(&1.result == :success)),
      failure_count: Enum.count(events, &(&1.result == :failure))
    }
  end
end
