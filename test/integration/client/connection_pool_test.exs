defmodule Pulsar.Integration.Client.ConnectionPoolTest do
  use ExUnit.Case, async: true

  alias Pulsar.Broker
  alias Pulsar.Broker.Pool, as: BrokerPool
  alias Pulsar.Client
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration

  @client __MODULE__
  @topic "persistent://public/default/connection-pool-test"

  setup_all do
    broker = System.broker()
    :ok = System.create_topic(@topic)

    {:ok, _client} =
      Client.start_link(
        name: @client,
        host: broker.service_url,
        connections_per_broker: 2
      )

    on_exit(fn -> Client.stop(@client) end)

    {:ok, broker: broker}
  end

  test "every connection in a broker pool completes its handshake and carries requests", %{broker: broker} do
    [{pool, _value}] = Registry.lookup(Client.broker_registry(@client), broker.service_url)
    connections = BrokerPool.connections(pool)

    assert length(connections) == 2

    Utils.wait_for(
      fn -> Enum.map(connections, &Broker.get_max_message_size/1) end,
      until: &Enum.all?(&1, fn size -> is_integer(size) and size > 0 end),
      description: "every pooled broker connection to complete its handshake"
    )

    for connection <- connections do
      assert {:ok, %{response: :Success, partitions: 0}} =
               Broker.partitioned_topic_metadata(connection, @topic)
    end
  end
end
