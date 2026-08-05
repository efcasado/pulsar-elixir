defmodule Pulsar.Test.Support.ProducerState do
  @moduledoc """
  A producer worker's state, for tests that drive `Pulsar.Producer.Worker` directly rather than
  through a supervised producer.

  The defaults describe a ready producer that batches nothing and chunks nothing, so a test only
  overrides what it is about:

      ProducerState.new(broker, batch_enabled: true, batch_size: 10)

  Its topic and producer name are unique per call, so tests running together cannot be taken for
  one another in a log line or a telemetry event. A test that asserts on either passes its own.
  """

  alias Pulsar.Producer.Worker

  @spec new(pid(), keyword() | map()) :: Worker.t()
  def new(broker, overrides \\ []) do
    struct(Worker, Map.merge(defaults(broker), Map.new(overrides)))
  end

  defp defaults(broker) do
    unique = System.unique_integer([:positive])
    topic = "persistent://public/default/producer-state-#{unique}"

    %{
      topic: topic,
      base_topic: topic,
      producer_id: unique,
      producer_name: "producer-#{unique}",
      broker_pid: broker,
      ready: true,
      compression: :none,
      chunking_enabled: false,
      max_message_size: 5_242_880,
      broker_max_message_size: 5_242_880,
      batch_enabled: false,
      batch_size: 100,
      batch_builder: :default,
      # Long enough that nothing flushes on a timer: these tests flush by filling the batch.
      flush_interval: 30_000,
      send_timeout: 30_000,
      max_pending_messages: 1000
    }
  end
end
