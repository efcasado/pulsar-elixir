defmodule Pulsar.Producer.BatchTest do
  @moduledoc false
  use ExUnit.Case, async: true

  import Pulsar.Test.Support.BrokerStub, only: [published: 0]

  alias Pulsar.Producer.Worker
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.Test.Support.BrokerStub

  @at_time 1_900_000_000_000

  setup do
    broker = start_supervised!({BrokerStub, self()})

    %{state: producer_state(broker)}
  end

  describe "the entry a batch is published as" do
    test "carries the key its messages share", ctx do
      flush(ctx.state, [[partition_key: "tenant-1"], [partition_key: "tenant-1"]])

      assert [entry] = published()
      assert entry.metadata.partition_key == "tenant-1"
    end

    test "carries the ordering key its messages share", ctx do
      flush(ctx.state, [[ordering_key: "order-1"], [ordering_key: "order-1"]])

      assert [entry] = published()
      assert entry.metadata.ordering_key == "order-1"
    end

    test "carries no key when its messages have none", ctx do
      flush(ctx.state, [[], []])

      assert [entry] = published()
      assert entry.metadata.partition_key == nil
      assert entry.metadata.ordering_key == nil
    end

    # What Java and Go do. Only key-based batching would give the rest of the entry their own.
    test "carries the first message's key when they differ", ctx do
      flush(ctx.state, [[partition_key: "tenant-1"], [partition_key: "tenant-2"]])

      assert [entry] = published()
      assert entry.metadata.partition_key == "tenant-1"
    end

    test "leaves each message its own key", ctx do
      flush(ctx.state, [[partition_key: "tenant-1"], [partition_key: "tenant-2"]])

      assert [entry] = published()
      assert keys_in_batch(entry.payload) == ["tenant-1", "tenant-2"]
    end
  end

  describe "a delayed message on a batching producer" do
    test "carries the delay the caller asked for", ctx do
      send_message(ctx.state, "later", deliver_at_time: @at_time)

      assert [sent] = published()
      assert sent.metadata.deliver_at_time == @at_time
    end

    test "goes out on its own, so the delay applies to it alone", ctx do
      send_message(ctx.state, "later", deliver_at_time: @at_time)

      # Batch framing would have wrapped the payload in a SingleMessageMetadata.
      assert [sent] = published()
      assert sent.payload == "later"
    end

    test "resolves :deliver_after against the clock", ctx do
      before = System.system_time(:millisecond)
      send_message(ctx.state, "later", deliver_after: 60_000)

      assert [sent] = published()
      assert sent.metadata.deliver_at_time >= before + 60_000
      assert sent.metadata.deliver_at_time <= System.system_time(:millisecond) + 60_000
    end

    test "publishes what was already batched first, so it does not overtake it", ctx do
      {:noreply, state} = send_message(ctx.state, "first", [])
      {:noreply, state} = send_message(state, "second", [])
      assert [] == published(), "batched messages wait for a flush"

      send_message(state, "later", deliver_after: 60_000)

      assert [batch, delayed] = published()
      assert batch.metadata.num_messages_in_batch == 2
      assert batch.metadata.deliver_at_time == nil
      assert delayed.metadata.deliver_at_time
    end

    test "takes a sequence id after the batch it flushed", ctx do
      {:noreply, state} = send_message(ctx.state, "first", [])
      {:noreply, state} = send_message(state, "second", [])

      {:noreply, state} = send_message(state, "later", deliver_after: 60_000)

      assert [batch, delayed] = published()
      assert {batch.command.sequence_id, batch.command.highest_sequence_id} == {1, 2}
      assert delayed.command.sequence_id == 3
      assert state.sequence_id == 3
    end

    test "leaves the batch alone when no delay was asked for", ctx do
      {:noreply, state} = send_message(ctx.state, "a", properties: %{"k" => "v"})

      assert [] == published()
      assert state.batched == 1
    end
  end

  describe "a delayed message on a producer that is not batching" do
    test "carries the delay, as it always did", ctx do
      send_message(%{ctx.state | batch_enabled: false}, "later", deliver_at_time: @at_time)

      assert [sent] = published()
      assert sent.metadata.deliver_at_time == @at_time
    end
  end

  ## Helpers

  defp send_message(state, payload, opts) do
    Worker.handle_call({:send_message, payload, opts}, {self(), make_ref()}, state)
  end

  # Fills the batch so the last send flushes it.
  defp flush(state, sends) do
    Enum.reduce(sends, %{state | batch_size: length(sends)}, fn opts, acc ->
      {:noreply, next} = send_message(acc, "payload", opts)
      next
    end)
  end

  defp keys_in_batch(payload), do: keys_in_batch(payload, [])

  defp keys_in_batch(<<>>, acc), do: Enum.reverse(acc)

  defp keys_in_batch(<<size::32, metadata::bytes-size(size), rest::binary>>, acc) do
    single = Binary.SingleMessageMetadata.decode(metadata)
    payload_size = single.payload_size
    <<_payload::bytes-size(^payload_size), tail::binary>> = rest

    keys_in_batch(tail, [single.partition_key | acc])
  end

  defp producer_state(broker) do
    struct(Worker, %{
      topic: "persistent://public/default/orders",
      base_topic: "persistent://public/default/orders",
      producer_id: 1,
      producer_name: "orders-api",
      broker_pid: broker,
      ready: true,
      compression: :none,
      chunking_enabled: false,
      max_message_size: 5_242_880,
      batch_enabled: true,
      batch_size: 10,
      flush_interval: 30_000
    })
  end
end
