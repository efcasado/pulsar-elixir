defmodule Pulsar.Integration.Consumer.SchemaTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils
  alias Pulsar.Topology

  @moduletag :integration
  @client :consumer_schema_test_client

  setup_all do
    broker = System.broker()
    {:ok, _} = Pulsar.Client.start_link(name: @client, host: broker.service_url)
    on_exit(fn -> Pulsar.Client.stop(@client) end)
  end

  test "consumer successfully registers schema with broker" do
    topic = "persistent://public/default/consumer-schema-registration-test-#{:erlang.unique_integer([:positive])}"

    producer_pid = start_producer(topic, schema: [type: :String])
    consumer_pid = start_consumer(topic, "schema-sub", schema: [type: :String])

    state = :sys.get_state(consumer_pid)
    assert %{schema: schema} = state
    assert schema.type == :String

    # Verify messages can be sent and received
    {:ok, _} = Pulsar.Producer.send(producer_pid, "test message")
    Utils.wait_for(fn -> DummyConsumer.count_messages(consumer_pid) >= 1 end)

    [message] = DummyConsumer.get_messages(consumer_pid)
    assert message.payload == "test message"
  end

  test "consumer can subscribe without schema" do
    topic = "persistent://public/default/consumer-no-schema-test-#{:erlang.unique_integer([:positive])}"

    producer_pid = start_producer(topic, schema: [type: :String])
    consumer_pid = start_consumer(topic, "no-schema-sub")

    state = :sys.get_state(consumer_pid)
    assert state.schema == nil

    {:ok, _} = Pulsar.Producer.send(producer_pid, "test message")
    Utils.wait_for(fn -> DummyConsumer.count_messages(consumer_pid) >= 1 end)

    [message] = DummyConsumer.get_messages(consumer_pid)
    assert message.payload == "test message"
  end

  test "incompatible schema types are rejected" do
    topic = "persistent://public/default/consumer-schema-compat-test-#{:erlang.unique_integer([:positive])}"

    start_producer(topic, schema: [type: :String])

    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        topic,
        "incompatible-sub",
        DummyConsumer,
        client: @client,
        schema: [type: :Int32]
      )

    :ok = Utils.wait_for(fn -> Topology.status(consumer_group) == {:ready, :non_partitioned} end)
    :ok = Utils.wait_for(fn -> Topology.workers(consumer_group) == [] end)

    assert Process.alive?(consumer_group)
    assert consumer_group in Pulsar.Client.consumers(@client)
    assert {:error, :no_consumers_available} = Pulsar.Consumer.send_flow(consumer_group, 1)

    assert :ok = Pulsar.Consumer.stop(consumer_group, client: @client)
    :ok = Utils.wait_for(fn -> not Process.alive?(consumer_group) end)
    refute consumer_group in Pulsar.Client.consumers(@client)
  end

  defp start_producer(topic, opts) do
    {:ok, pid} = Pulsar.Producer.start(topic, Keyword.merge([client: @client], opts))

    Utils.wait_for(fn -> Topology.workers(pid) end,
      until: fn
        [producer] -> :sys.get_state(producer).ready
        _workers -> false
      end
    )

    pid
  end

  defp start_consumer(topic, sub_name, opts \\ []) do
    {:ok, _} =
      Pulsar.Consumer.start(
        topic,
        sub_name,
        DummyConsumer,
        Keyword.merge(
          [
            client: @client,
            initial_position: :earliest,
            init_args: [notify_pid: self()]
          ],
          opts
        )
      )

    [pid] = Utils.wait_for_consumer_ready(1)
    pid
  end
end
