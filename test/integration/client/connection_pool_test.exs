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

    assert {:ok, selected} = Client.lookup_broker(broker.service_url, client: @client)
    assert selected in BrokerPool.connections(pool)

    for connection <- connections do
      assert {:ok, %{response: :Success, partitions: 0}} =
               Broker.partitioned_topic_metadata(connection, @topic)
    end
  end

  test "logical producers on the same topic are assigned connections round-robin" do
    {:ok, first} =
      Pulsar.Producer.start(topic: @topic, name: :first_pooled_producer, client: @client)

    :ok = Pulsar.Producer.await_ready(first)

    {:ok, second} =
      Pulsar.Producer.start(topic: @topic, name: :second_pooled_producer, client: @client)

    :ok = Pulsar.Producer.await_ready(second)

    on_exit(fn ->
      Pulsar.Producer.stop(first)
      Pulsar.Producer.stop(second)
    end)

    registration_counts =
      @client
      |> Client.broker_registry()
      |> Registry.select([{{:"$1", :"$2", :"$3"}, [], [:"$2"]}])
      |> Enum.flat_map(&BrokerPool.connections/1)
      |> Enum.map(&Broker.get_producers/1)
      |> Enum.map(&map_size/1)
      |> Enum.reject(&(&1 == 0))
      |> Enum.sort()

    assert registration_counts == [1, 1]
  end
end
