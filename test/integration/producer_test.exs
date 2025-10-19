defmodule Pulsar.Integration.ProducerTest do
  use ExUnit.Case
  import TelemetryTest

  require Logger

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @topic "persistent://public/default/producer-test-topic"

  setup do
    broker = System.broker()

    config = [
      host: broker.service_url
    ]

    {:ok, app_pid} = Pulsar.start(config)

    on_exit(fn ->
      Process.exit(app_pid, :shutdown)
      Utils.wait_for(fn -> not Process.alive?(app_pid) end)
    end)
  end

  setup [:telemetry_listen]

  describe "Producer Lifecycle" do
    @tag telemetry_listen: [[:pulsar, :producer, :opened, :stop]]
    test "create producer successfully" do
      topic = @topic <> "-#{:erlang.unique_integer([:positive])}"
      System.create_topic(topic)

      assert {:ok, producer} = Pulsar.start_producer(topic: topic)
      assert Process.alive?(producer)

      # Wait a bit to ensure telemetry events are emitted
      Process.sleep(100)

      stats = Utils.collect_producer_opened_stats()
      assert %{success_count: 1, failure_count: 0, total_count: 1} = stats
    end

    @tag telemetry_listen: [[:pulsar, :producer, :opened, :stop]]
    test "producer creation fails for non-existing topics" do
      result1 = Pulsar.start_producer(topic: "persistent://fake/fake/fake")
      assert {:error, _reason} = result1

      stats = Utils.collect_producer_opened_stats()
      assert %{success_count: 0, failure_count: 1, total_count: 1} = stats
    end
  end
end
