defmodule Pulsar.Integration.Consumer.BatchAckTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :consumer_batch_ack_test_client
  @topic "persistent://public/default/consumer-batch-ack-test"
  @batch ["msg-1", "msg-2", "msg-3", "msg-4", "msg-5"]

  # Leaves part of a batch unacknowledged, the way a consumer dying mid-entry would.
  defmodule PartialAckConsumer do
    @moduledoc false
    use Pulsar.Consumer.Callback

    def init(opts, _context) do
      notify_pid = Keyword.fetch!(opts, :notify_pid)
      send(notify_pid, {:consumer_ready, self()})

      {:ok, %{notify_pid: notify_pid, ack: Keyword.fetch!(opts, :ack)}}
    end

    def handle_message(message, state) do
      send(state.notify_pid, {:received, message.payload})

      if state.ack == :all or message.payload in state.ack do
        {:ok, state}
      else
        {:noreply, state}
      end
    end
  end

  setup_all do
    broker = System.broker()
    {:ok, _} = Pulsar.Client.start_link(name: @client, host: broker.service_url)
    on_exit(fn -> Pulsar.Client.stop(@client) end)
    :ok
  end

  test "acking one message of a batch does not acknowledge the rest of its entry" do
    topic = @topic <> "-partial"
    subscription = "partial-sub"
    :ok = produce_one_batch(topic, "partial")

    consumer = start_consumer(topic, subscription, ack: ["msg-1"])
    for payload <- @batch, do: assert_receive({:received, ^payload}, 10_000)
    :ok = Pulsar.Consumer.stop(consumer, client: @client)

    # The entry is redelivered whole, so the acked message comes back with its siblings.
    _consumer = start_consumer(topic, subscription, ack: :all)
    for payload <- @batch, do: assert_receive({:received, ^payload}, 10_000)
  end

  test "batch index acks redeliver only the messages that were not acked" do
    topic = @topic <> "-index"
    subscription = "index-sub"
    :ok = produce_one_batch(topic, "index")

    consumer = start_consumer(topic, subscription, [ack: ["msg-1"]], batch_index_ack_enabled: true)
    for payload <- @batch, do: assert_receive({:received, ^payload}, 10_000)
    :ok = Pulsar.Consumer.stop(consumer, client: @client)

    # The broker was told which message the ack was for, so it does not come back.
    _consumer = start_consumer(topic, subscription, [ack: :all], batch_index_ack_enabled: true)
    for payload <- tl(@batch), do: assert_receive({:received, ^payload}, 10_000)
    refute_receive {:received, "msg-1"}, 2_000
  end

  test "acking every message of a batch acknowledges its entry" do
    topic = @topic <> "-complete"
    subscription = "complete-sub"
    :ok = produce_one_batch(topic, "complete")

    consumer = start_consumer(topic, subscription, ack: :all)
    for payload <- @batch, do: assert_receive({:received, ^payload}, 10_000)
    :ok = Pulsar.Consumer.stop(consumer, client: @client)

    _consumer = start_consumer(topic, subscription, ack: :all)
    refute_receive {:received, _payload}, 3_000
  end

  ## Helpers

  # One batch, so all five messages share a single entry.
  defp produce_one_batch(topic, name) do
    :ok = System.create_topic(topic)

    {:ok, producer} =
      Pulsar.Producer.start(topic,
        client: @client,
        name: "#{name}-producer",
        batch_enabled: true,
        batch_size: length(@batch),
        flush_interval: 30_000
      )

    :ok = Pulsar.Producer.await_ready(producer)

    results =
      @batch
      |> Enum.map(fn payload -> Task.async(fn -> Pulsar.Producer.send(producer, payload) end) end)
      |> Task.await_many(10_000)

    assert Enum.all?(results, &match?({:ok, _}, &1))

    :ok
  end

  defp start_consumer(topic, subscription, init_args, consumer_opts \\ []) do
    {:ok, consumer} =
      Pulsar.Consumer.start(
        topic,
        subscription,
        PartialAckConsumer,
        [
          client: @client,
          initial_position: :earliest,
          init_args: [notify_pid: self()] ++ init_args
        ] ++ consumer_opts
      )

    Utils.wait_for_consumer_ready(1)

    consumer
  end
end
