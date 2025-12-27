defmodule Pulsar.Integration.Producer.SchemaTest do
  use ExUnit.Case, async: true

  alias Pulsar.Schema
  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :producer_schema_test_client

  defp start_producer(topic, opts) do
    {:ok, pid} = Pulsar.start_producer(topic, Keyword.merge([client: @client], opts))
    Utils.wait_for_producer_ready(pid)
    pid
  end

  defp start_consumer(topic, sub_name) do
    {:ok, _} =
      Pulsar.start_consumer(topic, sub_name, DummyConsumer,
        client: @client,
        initial_position: :earliest,
        init_args: [notify_pid: self()]
      )

    [pid] = Utils.wait_for_consumer_ready(1)
    pid
  end

  defp get_producer_state(producer_pid) do
    [producer] = Pulsar.get_producers(producer_pid)
    :sys.get_state(producer)
  end

  defp assert_send(producer_pid, consumer_pid, payload) do
    {:ok, _} = Pulsar.send(producer_pid, payload)
    Utils.wait_for(fn -> DummyConsumer.count_messages(consumer_pid) >= 1 end)
    assert [%{payload: ^payload}] = DummyConsumer.get_messages(consumer_pid)
  end

  setup_all do
    broker = System.broker()
    {:ok, _} = Pulsar.Client.start_link(name: @client, host: broker.service_url)
    on_exit(fn -> Pulsar.Client.stop(@client) end)
  end

  describe "producer with schemas" do
    test "json schema" do
      topic = "persistent://public/default/producer-schema-test-json"
      :ok = System.create_topic(topic)
      json_definition = Jason.encode!(%{type: "record", name: "TestRecord", fields: [%{name: "name", type: "string"}]})
      schema = Schema.new!(type: :Json, definition: json_definition, name: "test-json-schema")

      producer_pid = start_producer(topic, schema: schema)
      assert %{schema: %Schema{type: :Json}} = get_producer_state(producer_pid)

      consumer_pid = start_consumer(topic, "json-schema-sub")
      assert_send(producer_pid, consumer_pid, ~s({"name": "test"}))
    end

    test "avro schema" do
      topic = "persistent://public/default/producer-schema-test-avro"
      :ok = System.create_topic(topic)

      avro_definition =
        Jason.encode!(%{
          type: "record",
          name: "User",
          namespace: "com.example",
          fields: [
            %{name: "id", type: "int"},
            %{name: "name", type: "string"}
          ]
        })

      schema = Schema.new!(type: :Avro, definition: avro_definition, name: "test-avro-schema")

      producer_pid = start_producer(topic, schema: schema)
      assert %{schema: %Schema{type: :Avro}} = get_producer_state(producer_pid)

      consumer_pid = start_consumer(topic, "avro-schema-sub")
      # Pre-encoded Avro binary: id=21 (zigzag: 42), name="test" (length=4, zigzag: 8)
      assert_send(producer_pid, consumer_pid, <<0, 42, 8, "test">>)
    end

    test "batched messages work with schema" do
      topic = "persistent://public/default/schema-with-batching-test"
      :ok = System.create_topic(topic)
      schema = Schema.new!(type: :String)

      producer_pid = start_producer(topic, schema: schema, batch_enabled: true, batch_size: 3, flush_interval: 100)
      assert %{schema: %Schema{type: :String}, batch_enabled: true} = get_producer_state(producer_pid)

      consumer_pid = start_consumer(topic, "batch-schema-sub")
      messages = ["msg-1", "msg-2", "msg-3"]

      results = messages |> Enum.map(&Task.async(fn -> Pulsar.send(producer_pid, &1) end)) |> Task.await_many(10_000)
      assert Enum.all?(results, &match?({:ok, _}, &1))

      Utils.wait_for(fn -> DummyConsumer.count_messages(consumer_pid) >= 3 end)
      received = consumer_pid |> DummyConsumer.get_messages() |> Enum.map(& &1.payload)
      assert Enum.all?(messages, &(&1 in received))
    end
  end

  describe "schemas" do
    test "we get schema version from broker" do
      topic = "persistent://public/default/producer-schema-version-test"
      :ok = System.create_topic(topic)
      schema = Schema.new!(type: :String)

      producer_pid = start_producer(topic, schema: schema)
      state = get_producer_state(producer_pid)

      assert %{schema: %Schema{type: :String}, schema_version: version} = state
      assert is_binary(version), "expected schema_version to be binary, got: #{inspect(version)}"
    end

    test "message metadata includes schema version" do
      topic = "persistent://public/default/producer-schema-msg-version-test"
      :ok = System.create_topic(topic)
      schema = Schema.new!(type: :String)

      producer_pid = start_producer(topic, schema: schema)
      consumer_pid = start_consumer(topic, "schema-version-sub")

      {:ok, _} = Pulsar.send(producer_pid, "test message")
      Utils.wait_for(fn -> DummyConsumer.count_messages(consumer_pid) >= 1 end)

      [message] = DummyConsumer.get_messages(consumer_pid)
      assert message.metadata.schema_version
    end

    test "incompatible schema types are rejected on same topic" do
      topic = "persistent://public/default/producer-schema-compat-test"
      :ok = System.create_topic(topic)

      string_schema = Schema.new!(type: :String)
      _producer1 = start_producer(topic, schema: string_schema, name: "compat-test-producer-1")

      int_schema = Schema.new!(type: :Int32)

      {:ok, producer_group} =
        Pulsar.start_producer(topic,
          client: @client,
          schema: int_schema,
          name: "compat-test-producer-2"
        )

      [producer_pid] = Pulsar.get_producers(producer_group)
      ref = Process.monitor(producer_pid)
      assert_receive {:DOWN, ^ref, :process, ^producer_pid, _reason}, 5_000
      assert Pulsar.get_producers(producer_group) == []
    end

    test "compatible schema changes produce different versions" do
      topic = "persistent://public/default/producer-schema-evolution-test"
      :ok = System.create_topic(topic)

      schema_v1 =
        Schema.new!(
          type: :Json,
          definition:
            Jason.encode!(%{
              type: "record",
              name: "User",
              fields: [%{name: "name", type: "string"}]
            }),
          name: "user-schema"
        )

      producer1 = start_producer(topic, schema: schema_v1)
      state1 = get_producer_state(producer1)
      version1 = state1.schema_version

      Pulsar.stop_producer(producer1)

      # Evolved schema with additional optional field
      schema_v2 =
        Schema.new!(
          type: :Json,
          definition:
            Jason.encode!(%{
              type: "record",
              name: "User",
              fields: [
                %{name: "name", type: "string"},
                %{name: "age", type: ["null", "int"], default: nil}
              ]
            }),
          name: "user-schema"
        )

      producer2 = start_producer(topic, schema: schema_v2)
      state2 = get_producer_state(producer2)
      version2 = state2.schema_version

      assert version1 != version2
    end
  end
end
