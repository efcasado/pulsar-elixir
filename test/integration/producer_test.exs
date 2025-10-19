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
      # Create topic first
      topic = @topic <> "-#{:erlang.unique_integer([:positive])}"
      System.create_topic(topic)

      {:ok, producer} = Pulsar.start_producer(topic: topic)
      assert Process.alive?(producer)

      # Wait a bit to ensure telemetry events are emitted
      Process.sleep(100)

      # Verify telemetry using collected stats
      stats = Utils.collect_producer_opened_stats()
      assert %{success_count: 1, failure_count: 0, total_count: 1} = stats
    end
  end
end
