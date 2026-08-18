defmodule Pulsar.Integration.Consumer.CumulativeAckTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :consumer_cumulative_ack_test_client
  @topic "persistent://public/default/consumer-cumulative-ack-test"
  @messages ["msg-1", "msg-2", "msg-3", "msg-4", "msg-5"]

  # Acks only the messages it is told to, so the rest are left for the cursor to pass over.
  defmodule SelectiveAckConsumer do
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

  test "one ack acknowledges the messages before it that were never acked" do
    topic = @topic <> "-through"
    subscription = "through-sub"
    :ok = produce(topic, "through")

    consumer = start_consumer(topic, subscription, ack: ["msg-3"])
    for payload <- @messages, do: assert_receive({:received, ^payload}, 10_000)
    :ok = Pulsar.Consumer.stop(consumer, client: @client)

    # msg-1 and msg-2 were only ever deferred, but the ack of msg-3 moved the cursor past them.
    _consumer = start_consumer(topic, subscription, ack: :all)
    for payload <- ["msg-4", "msg-5"], do: assert_receive({:received, ^payload}, 10_000)
    refute_receive {:received, "msg-1"}, 2_000
    refute_receive {:received, "msg-2"}, 2_000
  end

  test "leaves the messages after the cursor to be redelivered" do
    topic = @topic <> "-remainder"
    subscription = "remainder-sub"
    :ok = produce(topic, "remainder")

    consumer = start_consumer(topic, subscription, ack: ["msg-2"])
    for payload <- @messages, do: assert_receive({:received, ^payload}, 10_000)
    :ok = Pulsar.Consumer.stop(consumer, client: @client)

    _consumer = start_consumer(topic, subscription, ack: :all)
    for payload <- ["msg-3", "msg-4", "msg-5"], do: assert_receive({:received, ^payload}, 10_000)
  end

  test "acking every message leaves the subscription caught up" do
    topic = @topic <> "-complete"
    subscription = "complete-sub"
    :ok = produce(topic, "complete")

    consumer = start_consumer(topic, subscription, ack: :all)
    for payload <- @messages, do: assert_receive({:received, ^payload}, 10_000)
    :ok = Pulsar.Consumer.stop(consumer, client: @client)

    _consumer = start_consumer(topic, subscription, ack: :all)
    refute_receive {:received, _payload}, 3_000
  end

  # The cursor names entries, so a batch is where cumulative acking can overshoot.
  test "does not acknowledge the messages batched after the one that was acked" do
    topic = @topic <> "-batch"
    subscription = "batch-sub"
    :ok = produce_one_batch(topic, "batch")

    consumer = start_consumer(topic, subscription, ack: ["msg-2"])
    for payload <- @messages, do: assert_receive({:received, ^payload}, 10_000)
    :ok = Pulsar.Consumer.stop(consumer, client: @client)

    # msg-3 onwards were never processed, so the entry has to come back — and it comes back
    # whole, since an entry is the unit of redelivery.
    _consumer = start_consumer(topic, subscription, ack: :all)
    for payload <- @messages, do: assert_receive({:received, ^payload}, 10_000)
  end

  test "refuses a subscription the broker would reject the acknowledgement on" do
    assert_raise ArgumentError, ~r/no single cursor/, fn ->
      Pulsar.Consumer.start(@topic, "shared-sub", SelectiveAckConsumer,
        client: @client,
        ack_type: :cumulative,
        subscription_type: :shared,
        init_args: [notify_pid: self(), ack: :all]
      )
    end
  end

  ## Helpers

  # Unbatched, so each message is its own entry and the cursor has somewhere to stop.
  defp produce(topic, name) do
    :ok = System.create_topic(topic)

    {:ok, producer} = Pulsar.Producer.start(topic, client: @client, name: "#{name}-producer")
    :ok = Pulsar.Producer.await_ready(producer)

    for payload <- @messages do
      assert {:ok, _message_id} = Pulsar.Producer.send(producer, payload)
    end

    :ok
  end

  # One batch, so all five messages share a single entry.
  defp produce_one_batch(topic, name) do
    :ok = System.create_topic(topic)

    {:ok, producer} =
      Pulsar.Producer.start(topic,
        client: @client,
        name: "#{name}-producer",
        batch_enabled: true,
        batch_size: length(@messages),
        flush_interval: 30_000
      )

    :ok = Pulsar.Producer.await_ready(producer)

    results =
      @messages
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
        SelectiveAckConsumer,
        [
          client: @client,
          ack_type: :cumulative,
          subscription_type: :exclusive,
          initial_position: :earliest,
          init_args: [notify_pid: self()] ++ init_args
        ] ++ consumer_opts
      )

    Utils.wait_for_consumer_ready(1)

    consumer
  end
end
