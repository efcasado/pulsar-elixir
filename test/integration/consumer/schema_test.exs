defmodule Pulsar.Integration.Consumer.SchemaTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :consumer_schema_test_client

  setup_all do
    broker = System.broker()
    {:ok, _} = Pulsar.Client.start_link(name: @client, host: broker.service_url)
    on_exit(fn -> Pulsar.Client.stop(@client) end)
  end

  describe "consumer schema registration" do
    test "consumer successfully registers schema with broker" do
      topic = "persistent://public/default/consumer-schema-registration-test-#{:erlang.unique_integer([:positive])}"

      producer_pid = start_producer(topic, schema: [type: :String])
      consumer_pid = start_consumer(topic, "schema-sub", schema: [type: :String])

      state = :sys.get_state(consumer_pid)
      assert %{schema: schema} = state
      assert schema.type == :String

      # Verify messages can be sent and received
      {:ok, _} = Pulsar.send(producer_pid, "test message")
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

      {:ok, _} = Pulsar.send(producer_pid, "test message")
      Utils.wait_for(fn -> DummyConsumer.count_messages(consumer_pid) >= 1 end)

      [message] = DummyConsumer.get_messages(consumer_pid)
      assert message.payload == "test message"
    end
  end

  describe "schema compatibility validation" do
    test "incompatible schema types are rejected" do
      topic = "persistent://public/default/consumer-schema-compat-test-#{:erlang.unique_integer([:positive])}"

      start_producer(topic, schema: [type: :String])

      {:ok, consumer_group} =
        Pulsar.start_consumer(
          topic,
          "incompatible-sub",
          DummyConsumer,
          client: @client,
          schema: [type: :Int32]
        )

      ref = Process.monitor(consumer_group)
      assert_receive {:DOWN, ^ref, :process, ^consumer_group, _reason}, 10_000
    end
  end

  defp start_producer(topic, opts) do
    {:ok, pid} = Pulsar.start_producer(topic, Keyword.merge([client: @client], opts))
    Utils.wait_for_producer_ready(pid)
    pid
  end

  defp start_consumer(topic, sub_name, opts \\ []) do
    {:ok, _} =
      Pulsar.start_consumer(
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
