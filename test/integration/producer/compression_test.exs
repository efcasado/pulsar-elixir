defmodule Pulsar.Integration.Producer.CompressionTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.DummyConsumer
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  require Logger

  @moduletag :integration
  @client :producer_compression_test_client
  @topic "persistent://public/default/producer-compression-test"

  setup_all do
    broker = System.broker()

    :ok = System.create_topic(@topic)

    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)
  end

  test "produce and consume compressed messages successfully" do
    {:ok, _} =
      Pulsar.start_producer(
        @topic,
        producer_options("none", :NONE)
      )

    {:ok, _} =
      Pulsar.start_producer(
        @topic,
        producer_options("lz4", :LZ4)
      )

    {:ok, _} =
      Pulsar.start_producer(
        @topic,
        producer_options("zlib", :ZLIB)
      )

    {:ok, _} =
      Pulsar.start_producer(
        @topic,
        producer_options("zstd", :ZSTD)
      )

    {:ok, _} =
      Pulsar.start_producer(
        @topic,
        producer_options("snappy", :SNAPPY)
      )

    {:ok, consumer_pid} =
      Pulsar.start_consumer(
        @topic,
        "compression-test",
        DummyConsumer,
        subscription_options()
      )

    [consumer] = Pulsar.get_consumers(consumer_pid)
    Utils.wait_for(fn -> Process.alive?(consumer) end)

    # Wait for consumer to be ready to receive messages
    Utils.wait_for(fn ->
      state = :sys.get_state(consumer)
      state.flow_outstanding_permits > 0
    end)

    {:ok, _} = Pulsar.send("none", "Hello, world!", client: @client)
    {:ok, _} = Pulsar.send("lz4", "Hello, world!", client: @client)
    {:ok, _} = Pulsar.send("zstd", "Hello, world!", client: @client)
    {:ok, _} = Pulsar.send("zlib", "Hello, world!", client: @client)
    {:ok, _} = Pulsar.send("snappy", "Hello, world!", client: @client)

    Utils.wait_for(fn ->
      DummyConsumer.count_messages(consumer) == 5
    end)

    all_decoded? =
      consumer
      |> DummyConsumer.get_messages()
      |> Enum.all?(fn message -> message.payload == "Hello, world!" end)

    assert all_decoded?
  end

  defp producer_options(name, compression) do
    [
      client: @client,
      name: name,
      compression: compression
    ]
  end

  defp subscription_options do
    [
      client: @client
    ]
  end
end
