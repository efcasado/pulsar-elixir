defmodule Pulsar.Integration.Client.ConnectionPoolTest do
  use ExUnit.Case, async: true

  alias Pulsar.Broker
  alias Pulsar.Broker.Pool, as: BrokerPool
  alias Pulsar.Client
  alias Pulsar.Test.Support.System

  @moduletag :integration

  @client __MODULE__
  @topic "persistent://public/default/connection-pool-test"

  setup_all do
    broker = System.broker()
    :ok = System.create_topic(@topic)

    {:ok, broker: broker}
  end

  setup %{broker: broker} do
    handler = make_ref()
    :ok = :telemetry.attach(handler, [:pulsar, :connection, :connected], &__MODULE__.connected/4, self())
    on_exit(fn -> :telemetry.detach(handler) end)

    start_supervised!({Client, name: @client, host: broker.service_url, connections_per_broker: 2})

    :ok
  end

  def connected(_event, _measurements, metadata, test_pid), do: send(test_pid, {:broker_connected, metadata})

  test "every connection in a broker pool completes its handshake and carries requests", %{broker: broker} do
    [{pool, _value}] = Registry.lookup(Client.broker_registry(@client), broker.service_url)
    connections = BrokerPool.connections(pool)

    assert length(connections) == 2

    for connection <- connections do
      assert_receive {:broker_connected, %{broker_pid: ^connection, max_message_size: size}}
      assert is_integer(size) and size > 0
      assert Broker.get_max_message_size(connection) == size
    end

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
