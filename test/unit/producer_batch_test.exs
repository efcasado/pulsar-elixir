defmodule Pulsar.Producer.BatchTest do
  @moduledoc false
  use ExUnit.Case, async: true

  import Pulsar.Test.Support.BrokerStub, only: [published: 0]

  alias Pulsar.Producer.Worker
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.Test.Support.BrokerStub
  alias Pulsar.Test.Support.ProducerState

  @at_time 1_900_000_000_000

  setup do
    broker = start_supervised!({BrokerStub, self()})

    %{state: ProducerState.new(broker, batch_enabled: true, batch_size: 10)}
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

  describe "key-based batching" do
    test "publishes one entry per key, each carrying its own", ctx do
      flush(key_based(ctx.state), [
        [partition_key: "tenant-1"],
        [partition_key: "tenant-2"],
        [partition_key: "tenant-1"]
      ])

      assert [first, second] = published()
      assert first.metadata.partition_key == "tenant-1"
      assert second.metadata.partition_key == "tenant-2"

      assert first.metadata.num_messages_in_batch == 2
      assert second.metadata.num_messages_in_batch == 1
    end

    test "orders entries by the key that appeared first", ctx do
      flush(key_based(ctx.state), [[partition_key: "b"], [partition_key: "a"]])

      assert [first, second] = published()
      assert {first.metadata.partition_key, second.metadata.partition_key} == {"b", "a"}
    end

    test "gives each entry a sequence id range of its own", ctx do
      flush(key_based(ctx.state), [
        [partition_key: "tenant-1"],
        [partition_key: "tenant-2"],
        [partition_key: "tenant-1"]
      ])

      assert [first, second] = published()
      assert {first.command.sequence_id, first.command.highest_sequence_id} == {1, 2}
      assert {second.command.sequence_id, second.command.highest_sequence_id} == {3, 3}
    end

    test "groups on the ordering key when a message carries one", ctx do
      flush(key_based(ctx.state), [
        [ordering_key: "shared", partition_key: "tenant-1"],
        [ordering_key: "shared", partition_key: "tenant-2"]
      ])

      assert [entry] = published()
      assert entry.metadata.ordering_key == "shared"
      assert entry.metadata.num_messages_in_batch == 2
    end

    test "keeps keyless messages together in one entry", ctx do
      flush(key_based(ctx.state), [[], [partition_key: "tenant-1"], []])

      assert [keyless, keyed] = published()
      assert keyless.metadata.partition_key == nil
      assert keyless.metadata.num_messages_in_batch == 2
      assert keyed.metadata.partition_key == "tenant-1"
    end

    test "leaves the whole batch as one entry when it is off", ctx do
      flush(ctx.state, [[partition_key: "tenant-1"], [partition_key: "tenant-2"]])

      assert [entry] = published()
      assert entry.metadata.num_messages_in_batch == 2
    end

    # "msg-1" was sent before "msg-2" and is published after it.
    test "keeps order within a key, not across them", ctx do
      flush(key_based(ctx.state), [[partition_key: "a"], [partition_key: "b"], [partition_key: "a"]])

      assert [first, second] = published()
      assert payloads_in_batch(first.payload) == ["msg-0", "msg-2"]
      assert payloads_in_batch(second.payload) == ["msg-1"]
    end

    test "an entry the broker refuses fails its own callers and leaves the rest alone", ctx do
      # The second entry published is the one carrying "b".
      broker = start_supervised!({BrokerStub, {self(), [1]}}, id: :refusing_broker)
      state = %{key_based(ctx.state) | broker_pid: broker, batch_size: 3}

      froms = Enum.map(0..2, fn _ -> {self(), make_ref()} end)

      state =
        [froms, ["a", "b", "a"], 0..2]
        |> Enum.zip()
        |> Enum.reduce(state, fn {from, key, index}, acc ->
          {:noreply, next} = Worker.handle_call({:send_message, "msg-#{index}", [partition_key: key]}, from, acc)
          next
        end)

      assert [entry] = published()
      assert payloads_in_batch(entry.payload) == ["msg-0", "msg-2"]

      # Ids 1 and 2 went to the entry that landed; the refused one claimed none.
      assert state.sequence_id == 2
      assert map_size(state.pending_frames) == 1

      # Only "b" hears about it. The others are still waiting on the receipt for their entry.
      [{_, a_ref}, {_, b_ref}, _] = froms
      assert_received {^b_ref, {:error, :message_too_large}}
      refute_received {^a_ref, _reply}
    end

    test "keeps the ids of a refused entry for the next one, having sent nothing", ctx do
      broker = start_supervised!({BrokerStub, {self(), [0]}}, id: :refusing_first)
      state = %{key_based(ctx.state) | broker_pid: broker}

      state = flush(state, [[partition_key: "a"], [partition_key: "b"]])

      # The refused entry claimed nothing, so the one after it starts where it would have.
      assert [entry] = published()
      assert entry.command.sequence_id == 1
      assert state.sequence_id == 1
      assert map_size(state.pending_frames) == 1
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

  defp key_based(state), do: %{state | batch_builder: :key_based}

  # Fills the batch so the last send flushes it.
  defp flush(state, sends) do
    sends
    |> Enum.with_index()
    |> Enum.reduce(%{state | batch_size: length(sends)}, fn {opts, index}, acc ->
      {:noreply, next} = send_message(acc, "msg-#{index}", opts)
      next
    end)
  end

  defp keys_in_batch(payload), do: Enum.map(messages_in_batch(payload), &elem(&1, 0))

  defp payloads_in_batch(payload), do: Enum.map(messages_in_batch(payload), &elem(&1, 1))

  defp messages_in_batch(batch), do: messages_in_batch(batch, [])

  defp messages_in_batch(<<>>, acc), do: Enum.reverse(acc)

  defp messages_in_batch(<<size::32, metadata::bytes-size(size), rest::binary>>, acc) do
    single = Binary.SingleMessageMetadata.decode(metadata)
    payload_size = single.payload_size
    <<payload::bytes-size(^payload_size), tail::binary>> = rest

    messages_in_batch(tail, [{single.partition_key, payload} | acc])
  end
end
