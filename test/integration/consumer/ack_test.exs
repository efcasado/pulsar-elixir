defmodule Pulsar.Integration.Consumer.AckTest do
  use Pulsar.Test.Case, async: true

  alias Pulsar.Topology

  @messages ["ack-1", "ack-2", "ack-3"]

  # Holds every message unacknowledged and reports who it came from, so the test owns the
  # acknowledgement and can answer from whatever worker or name it likes.
  defmodule ManualAckConsumer do
    @moduledoc false
    use Pulsar.Consumer.Callback

    def init(opts, _context), do: {:ok, %{notify_pid: Keyword.fetch!(opts, :notify_pid)}}

    def handle_message(message, state) do
      send(state.notify_pid, {:received, self(), message.message_id, message.payload})
      {:noreply, state}
    end
  end

  test "acks a message through the consumer's name, and the subscription advances" do
    topic = subject("by-name")
    subscription = "ack-by-name"
    :ok = System.create_topic(topic)
    :ok = System.produce_messages(topic, @messages)

    {:ok, consumer} =
      Pulsar.Consumer.start(topic, subscription, ManualAckConsumer, start_options("ack-by-name"))

    :ok = Pulsar.Consumer.await_ready(consumer)

    for payload <- @messages do
      assert_receive({:received, _worker, message_id, ^payload}, 10_000)
      assert Pulsar.Consumer.ack("ack-by-name", message_id, client: @client) == :ok
    end

    :ok = Pulsar.Consumer.stop(consumer, client: @client)

    # Successfully acked messages are off the subscription; a refused or dropped ack would
    # bring their messages back to the one that runs next.
    {:ok, _consumer} =
      Pulsar.Consumer.start(topic, subscription, ManualAckConsumer, start_options("ack-by-name"))

    refute_receive {:received, _worker, _id, _payload}, 3_000
  end

  # An ack carries the worker's consumer id, but the broker applies it to the subscription
  # that worker holds, so it does not matter which consumer of the subscription delivered the
  # message: every acknowledgement here is sent through the other consumer's name, for a
  # message its own worker never saw.
  test "acks a message delivered to the other consumer of the same subscription" do
    topic = subject("cross-consumer")
    subscription = "ack-cross-consumer"
    :ok = System.create_topic(topic)
    :ok = System.produce_messages(topic, @messages)

    {:ok, consumer_a} =
      Pulsar.Consumer.start(topic, subscription, ManualAckConsumer, start_options("cross-a"))

    {:ok, consumer_b} =
      Pulsar.Consumer.start(topic, subscription, ManualAckConsumer, start_options("cross-b"))

    :ok = Pulsar.Consumer.await_ready(consumer_a)
    :ok = Pulsar.Consumer.await_ready(consumer_b)

    workers_a = MapSet.new(Topology.workers(consumer_a))

    for payload <- @messages do
      assert_receive({:received, worker, message_id, ^payload}, 10_000)

      other_name = if worker in workers_a, do: "cross-b", else: "cross-a"
      assert Pulsar.Consumer.ack(other_name, message_id, client: @client) == :ok
    end

    :ok = Pulsar.Consumer.stop(consumer_a, client: @client)
    :ok = Pulsar.Consumer.stop(consumer_b, client: @client)

    {:ok, _consumer} =
      Pulsar.Consumer.start(topic, subscription, ManualAckConsumer, start_options("cross-a"))

    refute_receive {:received, _worker, _id, _payload}, 3_000
  end

  test "nacks a message through the consumer's name, and it comes back" do
    topic = subject("by-name-nack")
    :ok = System.create_topic(topic)
    :ok = System.produce_messages(topic, ["ack-1"])

    {:ok, consumer} =
      Pulsar.Consumer.start(topic, "ack-by-name-nack", ManualAckConsumer,
        name: "ack-by-name-nack",
        client: @client,
        redelivery_interval: 200,
        init_args: [notify_pid: self()]
      )

    :ok = Pulsar.Consumer.await_ready(consumer)
    assert_receive({:received, _worker, message_id, _payload}, 10_000)

    assert Pulsar.Consumer.nack("ack-by-name-nack", message_id, client: @client) == :ok
    assert_receive({:received, _worker, ^message_id, _payload}, 10_000)

    :ok = Pulsar.Consumer.stop(consumer, client: @client)
  end

  # One group behind the name is one subscription; a name cannot say which of several
  # subscription ledgers an id came from, so it is refused instead of guessed at.
  test "refuses a name that spans the subscriptions of a partitioned consumer" do
    topic = subject("partitioned")
    :ok = System.create_topic(topic, 3)
    :ok = System.produce_messages(topic, @messages)

    {:ok, consumer} =
      Pulsar.Consumer.start(topic, "ack-partitioned", ManualAckConsumer,
        name: "ack-partitioned",
        client: @client,
        init_args: [notify_pid: self()]
      )

    :ok = Pulsar.Consumer.await_ready(consumer)
    assert_receive({:received, _worker, message_id, _payload}, 10_000)

    assert Pulsar.Consumer.ack("ack-partitioned", message_id, client: @client) ==
             {:error, :ambiguous_worker}
  end

  # The other partition's worker holds a different topic's subscription, so this
  # acknowledgement goes out against a subscription that knows nothing of this ledger and
  # entry: it is dropped, and the message comes back. That is the behaviour a refused name
  # exists to avoid.
  test "an acknowledgement through the wrong partition's worker does not count" do
    topic = subject("cross-partition")
    subscription = "ack-cross-partition"
    :ok = System.create_topic(topic, 3)
    :ok = System.produce_messages(topic, @messages)
    {:ok, consumer} = start_partitioned(topic, subscription)

    assert_receive({:received, worker, message_id, payload}, 10_000)
    from_partition = partition_index(worker)
    assert from_partition

    other_worker =
      Enum.find(Topology.workers(consumer), &(&1 != worker and partition_index(&1) != nil))

    assert Pulsar.Consumer.ack(other_worker, message_id, client: @client) == :ok

    :ok = Pulsar.Consumer.stop(consumer, client: @client)
    {:ok, _consumer} = start_partitioned(topic, subscription)

    assert_receive {:received, _worker, _id, ^payload}, 10_000
  end

  ## Helpers

  defp subject(name), do: "persistent://public/default/consumer-ack-#{name}"

  defp start_options(name) do
    [
      name: name,
      client: @client,
      subscription_type: :shared,
      initial_position: :earliest,
      init_args: [notify_pid: self()]
    ]
  end

  defp start_partitioned(topic, subscription) do
    {:ok, consumer} =
      Pulsar.Consumer.start(topic, subscription, ManualAckConsumer,
        name: "ack-cross-partition",
        client: @client,
        initial_position: :earliest,
        init_args: [notify_pid: self()]
      )

    :ok = Pulsar.Consumer.await_ready(consumer)
    {:ok, consumer}
  end

  defp partition_index(worker) do
    case String.split(Pulsar.Consumer.topic(worker), "-partition-") do
      [_base, index] -> String.to_integer(index)
      _no_partition -> nil
    end
  end
end
