defmodule Pulsar.Integration.Producer.SchemaTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils
  alias Pulsar.Topology

  @moduletag :integration
  @client :producer_schema_test_client

  setup_all do
    broker = System.broker()
    {:ok, _} = Pulsar.Client.start_link(name: @client, host: broker.service_url)
    on_exit(fn -> Pulsar.Client.stop(@client) end)
  end

  test "json schema" do
    topic = "persistent://public/default/producer-schema-test-json"
    :ok = System.create_topic(topic)
    json_definition = %{type: "record", name: "TestRecord", fields: [%{name: "name", type: "string"}]}

    producer_pid = start_producer(topic, schema: [type: :Json, definition: json_definition, name: "test-json-schema"])
    state = get_producer_state(producer_pid)
    assert %{schema: schema} = state
    assert schema.type == :Json

    consumer_pid = start_consumer(topic, "json-schema-sub")
    assert_send(producer_pid, consumer_pid, ~s({"name": "test"}))
  end

  test "avro schema" do
    topic = "persistent://public/default/producer-schema-test-avro"
    :ok = System.create_topic(topic)

    avro_definition = %{
      type: "record",
      name: "User",
      namespace: "com.example",
      fields: [
        %{name: "id", type: "int"},
        %{name: "name", type: "string"}
      ]
    }

    producer_pid = start_producer(topic, schema: [type: :Avro, definition: avro_definition, name: "test-avro-schema"])
    state = get_producer_state(producer_pid)
    assert %{schema: schema} = state
    assert schema.type == :Avro

    consumer_pid = start_consumer(topic, "avro-schema-sub")
    # Pre-encoded Avro binary: id=21 (zigzag: 42), name="test" (length=4, zigzag: 8)
    assert_send(producer_pid, consumer_pid, <<0, 42, 8, "test">>)
  end

  defmodule TestSchemaStruct do
    @moduledoc false
    defstruct [:type, :name, :fields]
  end

  test "json schema with struct definition" do
    topic = "persistent://public/default/producer-schema-test-json-struct"
    :ok = System.create_topic(topic)

    struct_def = %TestSchemaStruct{
      type: "record",
      name: "TestRecord",
      fields: [%{name: "name", type: "string"}]
    }

    producer_pid =
      start_producer(topic, schema: [type: :Json, definition: struct_def, name: "test-json-struct-schema"])

    state = get_producer_state(producer_pid)
    assert %{schema: schema} = state
    assert schema.type == :Json
    assert is_binary(schema.definition)
    assert String.contains?(schema.definition, "TestRecord")

    consumer_pid = start_consumer(topic, "json-schema-struct-sub")
    assert_send(producer_pid, consumer_pid, ~s({"name": "test"}))
  end

  test "batched messages work with schema" do
    topic = "persistent://public/default/schema-with-batching-test"
    :ok = System.create_topic(topic)

    producer_pid =
      start_producer(topic, schema: [type: :String], batch_enabled: true, batch_size: 3, flush_interval: 100)

    state = get_producer_state(producer_pid)
    assert %{schema: schema, batch_enabled: true} = state
    assert schema.type == :String

    consumer_pid = start_consumer(topic, "batch-schema-sub")
    messages = ["msg-1", "msg-2", "msg-3"]

    results =
      messages |> Enum.map(&Task.async(fn -> Pulsar.Producer.send(producer_pid, &1) end)) |> Task.await_many(10_000)

    assert Enum.all?(results, &match?({:ok, _}, &1))

    Utils.wait_for(fn -> DummyConsumer.count_messages(consumer_pid) >= 3 end)
    received = consumer_pid |> DummyConsumer.get_messages() |> Enum.map(& &1.payload)
    assert Enum.all?(messages, &(&1 in received))
  end

  test "we get schema version from broker" do
    topic = "persistent://public/default/producer-schema-version-test"
    :ok = System.create_topic(topic)

    producer_pid = start_producer(topic, schema: [type: :String])
    state = get_producer_state(producer_pid)

    assert %{schema: schema, schema_version: version} = state
    assert schema.type == :String
    assert is_binary(version), "expected schema_version to be binary, got: #{inspect(version)}"
  end

  test "message metadata includes schema version" do
    topic = "persistent://public/default/producer-schema-msg-version-test"
    :ok = System.create_topic(topic)

    producer_pid = start_producer(topic, schema: [type: :String])
    consumer_pid = start_consumer(topic, "schema-version-sub")

    {:ok, _} = Pulsar.Producer.send(producer_pid, "test message")
    Utils.wait_for(fn -> DummyConsumer.count_messages(consumer_pid) >= 1 end)

    [message] = DummyConsumer.get_messages(consumer_pid)
    assert message.metadata.schema_version
  end

  test "incompatible schema types are rejected on same topic" do
    topic = "persistent://public/default/producer-schema-compat-test"
    :ok = System.create_topic(topic)

    _producer1 = start_producer(topic, schema: [type: :String], name: "compat-test-producer-1")

    {:ok, producer_group} =
      Pulsar.Producer.start(topic,
        client: @client,
        schema: [type: :Int32],
        name: "compat-test-producer-2"
      )

    :ok = Topology.await_ready(producer_group, 1_000)
    :ok = Utils.wait_for(fn -> Topology.workers(producer_group) == [] end)

    assert Pulsar.Producer.await_ready(producer_group, timeout: 0) ==
             {:error, :timeout}

    assert Process.alive?(producer_group)
    assert producer_group in Pulsar.Client.producers(@client)
    assert {:error, :no_producers_available} = Pulsar.Producer.send(producer_group, "message")

    assert :ok = Pulsar.Producer.stop(producer_group, client: @client)
    :ok = Utils.wait_for(fn -> not Process.alive?(producer_group) end)
    refute producer_group in Pulsar.Client.producers(@client)
  end

  test "compatible schema changes produce different versions" do
    topic = "persistent://public/default/producer-schema-evolution-test"
    :ok = System.create_topic(topic)

    producer1 =
      start_producer(topic,
        schema: [
          type: :Json,
          definition: %{
            type: "record",
            name: "User",
            fields: [%{name: "name", type: "string"}]
          },
          name: "user-schema"
        ]
      )

    state1 = get_producer_state(producer1)
    version1 = state1.schema_version

    Pulsar.Producer.stop(producer1)

    # Evolved schema with additional optional field
    producer2 =
      start_producer(topic,
        schema: [
          type: :Json,
          definition: %{
            type: "record",
            name: "User",
            fields: [
              %{name: "name", type: "string"},
              %{name: "age", type: ["null", "int"], default: nil}
            ]
          },
          name: "user-schema"
        ]
      )

    state2 = get_producer_state(producer2)
    version2 = state2.schema_version

    assert version1 != version2
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

  defp start_consumer(topic, sub_name) do
    {:ok, _} =
      Pulsar.Consumer.start(topic, sub_name, DummyConsumer,
        client: @client,
        initial_position: :earliest,
        init_args: [notify_pid: self()]
      )

    [pid] = Utils.wait_for_consumer_ready(1)
    pid
  end

  defp get_producer_state(producer_pid) do
    [producer] = Utils.wait_for(fn -> Topology.workers(producer_pid) end, until: &match?([_], &1))
    :sys.get_state(producer)
  end

  defp assert_send(producer_pid, consumer_pid, payload) do
    {:ok, _} = Pulsar.Producer.send(producer_pid, payload)
    Utils.wait_for(fn -> DummyConsumer.count_messages(consumer_pid) >= 1 end)
    assert [%{payload: ^payload}] = DummyConsumer.get_messages(consumer_pid)
  end
end
