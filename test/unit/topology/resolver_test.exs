defmodule Pulsar.Topology.ResolverTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.Topology.Resolver

  @topic "persistent://public/default/never-looked-up"

  setup [:telemetry_listen]

  describe "with no broker to ask" do
    # Both take whatever Pulsar.Client.random_broker/1 returns, which is nil while a client is
    # down or restarting. The worker subscribe paths match on an error tuple, so an exit there
    # would bypass their handling entirely.
    @tag telemetry_listen: [:pulsar, :service_discovery, :partition_count, :stop]
    test "partition_count/2 reports it rather than exiting" do
      assert Resolver.partition_count(@topic, client: :never_started) ==
               {:error, :no_broker_available}

      assert_receive {:telemetry_event,
                      %{
                        metadata: %{
                          topic: @topic,
                          client: :never_started,
                          success: false,
                          error: :no_broker_available
                        }
                      }}
    end

    test "lookup_topic/2 reports it rather than exiting" do
      assert Resolver.lookup_topic(@topic, client: :never_started) ==
               {:error, :no_broker_available}
    end
  end
end
