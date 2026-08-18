defmodule Pulsar.Integration.Producer.CompressionTest do
  use Pulsar.Test.Case, async: true

  alias Pulsar.Test.Support.DummyConsumer

  @topic "persistent://public/default/producer-compression-test"

  setup_all do
    :ok = System.create_topic(@topic)
  end

  test "a consumer decodes what every codec produced, without being told which" do
    {:ok, _} =
      Pulsar.Producer.start(
        @topic,
        producer_options("none", :none)
      )

    {:ok, _} =
      Pulsar.Producer.start(
        @topic,
        producer_options("lz4", :lz4)
      )

    {:ok, _} =
      Pulsar.Producer.start(
        @topic,
        producer_options("zlib", :zlib)
      )

    {:ok, _} =
      Pulsar.Producer.start(
        @topic,
        producer_options("zstd", :zstd)
      )

    {:ok, _} =
      Pulsar.Producer.start(
        @topic,
        producer_options("snappy", :snappy)
      )

    {:ok, consumer_pid} =
      Pulsar.Consumer.start(
        @topic,
        "compression-test",
        DummyConsumer,
        subscription_options()
      )

    :ok = Pulsar.Consumer.await_ready(consumer_pid)
    [consumer] = Topology.workers(consumer_pid)

    {:ok, _} = Pulsar.Producer.send("none", "Hello, world!", client: @client)
    {:ok, _} = Pulsar.Producer.send("lz4", "Hello, world!", client: @client)
    {:ok, _} = Pulsar.Producer.send("zstd", "Hello, world!", client: @client)
    {:ok, _} = Pulsar.Producer.send("zlib", "Hello, world!", client: @client)
    {:ok, _} = Pulsar.Producer.send("snappy", "Hello, world!", client: @client)

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
