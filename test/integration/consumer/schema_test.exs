defmodule Pulsar.Integration.Consumer.SchemaTest do
  use Pulsar.Test.Case, async: true

  alias Pulsar.Test.Support.DummyConsumer

  test "subscribes with a schema and reads what was published under it" do
    topic = "persistent://public/default/consumer-schema-registration-test-#{:erlang.unique_integer([:positive])}"

    producer_pid = start_producer(topic, schema: [type: :String])
    consumer_pid = start_consumer(topic, "schema-sub", schema: [type: :String])

    state = :sys.get_state(consumer_pid)
    assert %{schema: schema} = state
    assert schema.type == :String

    {:ok, _} = Pulsar.Producer.send(producer_pid, "test message")

    assert_receive {:consumer, ^consumer_pid, %{payload: "test message"}}
  end

  test "subscribes without one, and reads a schema-carrying topic anyway" do
    topic = "persistent://public/default/consumer-no-schema-test-#{:erlang.unique_integer([:positive])}"

    producer_pid = start_producer(topic, schema: [type: :String])
    consumer_pid = start_consumer(topic, "no-schema-sub")

    state = :sys.get_state(consumer_pid)
    assert state.schema == nil

    {:ok, _} = Pulsar.Producer.send(producer_pid, "test message")

    assert_receive {:consumer, ^consumer_pid, %{payload: "test message"}}
  end

  test "a consumer whose schema the topic will not accept never becomes ready" do
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

    :ok = Topology.await_ready(consumer_group, 1_000)
    Utils.wait_for(fn -> Topology.workers(consumer_group) == [] end)

    assert Pulsar.Consumer.await_ready(consumer_group, timeout: 0) ==
             {:error, :timeout}

    assert Process.alive?(consumer_group)
    assert consumer_group in Pulsar.Client.consumers(@client)
    assert {:error, :no_consumers_available} = Pulsar.Consumer.send_flow(consumer_group, 1)

    ref = Process.monitor(consumer_group)
    assert :ok = Pulsar.Consumer.stop(consumer_group, client: @client)
    assert_receive {:DOWN, ^ref, :process, ^consumer_group, _reason}
    refute consumer_group in Pulsar.Client.consumers(@client)
  end

  defp start_producer(topic, opts) do
    {:ok, pid} = Pulsar.Producer.start(topic, Keyword.merge([client: @client], opts))

    :ok = Pulsar.Producer.await_ready(pid)

    pid
  end

  defp start_consumer(topic, sub_name, opts \\ []) do
    {:ok, group} =
      Pulsar.Consumer.start(
        topic,
        sub_name,
        DummyConsumer,
        Keyword.merge([client: @client, initial_position: :earliest, init_args: [forward_to: self()]], opts)
      )

    :ok = Pulsar.Consumer.await_ready(group)
    [pid] = Topology.workers(group)
    pid
  end
end
