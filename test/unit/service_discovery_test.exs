defmodule Pulsar.ServiceDiscoveryTest do
  use ExUnit.Case, async: true

  alias Pulsar.ServiceDiscovery

  @topic "persistent://public/default/never-looked-up"

  describe "with no broker to ask" do
    # Both take whatever Pulsar.Client.random_broker/1 returns, which is nil while a client is
    # down or restarting. The worker subscribe paths match on an error tuple, so an exit there
    # would bypass their handling entirely.
    test "partition_count/2 reports it rather than exiting" do
      assert ServiceDiscovery.partition_count(@topic, client: :never_started) ==
               {:error, :no_broker_available}
    end

    test "lookup_topic/2 reports it rather than exiting" do
      assert ServiceDiscovery.lookup_topic(@topic, client: :never_started) ==
               {:error, :no_broker_available}
    end
  end
end
