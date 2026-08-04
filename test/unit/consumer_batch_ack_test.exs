defmodule Pulsar.Consumer.BatchAckTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Pulsar.Consumer.Ack
  alias Pulsar.Consumer.Worker
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

  defmodule Callback do
    @moduledoc false
    use Pulsar.Consumer.Callback

    def init(answers, _context), do: {:ok, answers}

    def handle_message(message, answers) do
      # The worker is driven inline, so this lands in the test process.
      send(self(), {:delivered, message.payload})

      case Map.get(answers, message.payload, :ack) do
        :ack -> {:ok, answers}
        :defer -> {:noreply, answers}
        :nack -> {:error, :rejected, answers}
      end
    end
  end

  @topic "persistent://public/default/orders"
  @ledger 7
  @entry 42

  describe "acking a batched message" do
    test "acknowledges the entry once, after the last message in it is acked" do
      deliver(worker_state(%{}), ["a", "b", "c"])

      assert [ack] = acks()
      assert [message_id] = ack.message_id
      assert {@ledger, @entry} == {message_id.ledgerId, message_id.entryId}

      assert message_id.batch_index == -1
    end

    test "leaves the entry unacknowledged while any message in it is still outstanding" do
      deliver(worker_state(%{"b" => :defer}), ["a", "b", "c"])

      assert [] == acks()
    end

    test "does not acknowledge the entry when a message in it is nacked" do
      deliver(worker_state(%{"b" => :nack}), ["a", "b", "c"])

      assert [] == acks()
    end

    test "completes the entry when a deferred ack arrives later" do
      state = deliver(worker_state(%{"b" => :defer}), ["a", "b", "c"])
      assert [] == acks()

      {:reply, :ok, _state} = Worker.handle_call({:ack, [batch_id(1)]}, self(), state)

      assert [ack] = acks()
      assert [%{batch_index: -1}] = ack.message_id
    end

    test "counts a redelivered entry from scratch, having dropped what a nack invalidated" do
      state = deliver(worker_state(%{"c" => :nack}), ["a", "b", "c"])
      assert [] == acks()

      state = deliver(%{state | callback_state: %{"c" => :ack}}, ["a", "b", "c"])

      assert [ack] = acks()
      assert [%{batch_index: -1}] = ack.message_id
      assert state.acks.acked == %{}
    end

    test "forgets the entry once it has been acknowledged" do
      state = deliver(worker_state(%{}), ["a", "b", "c"])

      assert state.acks.acked == %{}
    end
  end

  describe "nacking a batched message" do
    test "collapses ids to the entry the broker would redeliver" do
      state = deliver(worker_state(%{"a" => :nack, "c" => :nack}, redelivery_interval: 1000), ["a", "b", "c"])

      assert [%{batch_index: -1} = id] = MapSet.to_list(state.acks.nacked)
      assert {@ledger, @entry} == {id.ledgerId, id.entryId}
    end
  end

  describe "acking a message that was not batched" do
    test "acknowledges it on its own" do
      deliver_unbatched(worker_state(%{}), "solo")

      assert [ack] = acks()
      assert [%{ledgerId: @ledger, entryId: @entry, batch_index: -1}] = ack.message_id
    end

    test "is unaffected by an entry still being counted off" do
      state = deliver(worker_state(%{"b" => :defer}), ["a", "b", "c"])
      assert [] == acks()

      deliver_unbatched(state, "solo", entry: @entry + 1)

      assert [ack] = acks()
      assert [%{entryId: 43}] = ack.message_id
    end
  end

  describe "batch index acking" do
    test "reports what is still outstanding in the entry as each message is acked" do
      deliver(worker_state(%{}, batch_index_ack_enabled: true), ["a", "b", "c"])

      # Set bits are the messages still owed, clearing as the acks come in.
      assert [first, second, third] = Enum.map(acks(), fn ack -> hd(ack.message_id) end)
      assert first.ack_set == [0b110]
      assert second.ack_set == [0b100]
      assert third.ack_set == []

      assert first.batch_size == 3
      assert third.batch_size == nil
    end

    test "holds the entry back instead when it is turned off" do
      deliver(worker_state(%{}), ["a", "b", "c"])

      assert [ack] = acks()
      assert [%{ack_set: []}] = ack.message_id
    end

    test "spans several words for a batch wider than one, writing int64 as signed" do
      payloads = Enum.map(1..70, &"msg-#{&1}")
      # Everything but message 0 is deferred, so one ack goes out reporting the other 69.
      answers = Map.new(tl(payloads), &{&1, :defer})

      deliver(worker_state(answers, batch_index_ack_enabled: true), payloads)

      assert [ack] = acks()
      assert [%{ack_set: [low, high], batch_size: 70}] = ack.message_id

      # Bits 1..63 of the first word, which sets the sign bit, and 64..69 of the second.
      assert low == -2
      assert high == 0b111111
    end
  end

  describe "a redelivered entry" do
    test "skips the messages it reports as already acknowledged" do
      state = worker_state(%{})

      # Bits set are the messages still outstanding: "a" and "c" were acked before.
      deliver(state, ["a", "b", "c"], ack_set: [0b010])

      assert [received] = delivered_payloads()
      assert received == "b"
    end

    test "still spends a permit on each message the broker sent" do
      state = %{worker_state(%{}) | flow_initial: 100, flow_threshold: 0, flow_outstanding_permits: 100}

      new_state = deliver(state, ["a", "b", "c"], ack_set: [0b010])

      # One message was delivered, but the broker charged for the whole entry.
      assert new_state.flow_outstanding_permits == 97
    end

    test "delivers everything when the entry carries no set" do
      deliver(worker_state(%{}), ["a", "b", "c"])

      assert delivered_payloads() == ["a", "b", "c"]
    end
  end

  describe "an entry the broker will not deliver in full" do
    test "still completes when part of it was acknowledged before" do
      # Bits set are the messages still owed, so "a" was acked by whoever held the entry before.
      state = deliver(worker_state(%{}), ["a", "b", "c"], ack_set: [0b110])

      assert delivered_payloads() == ["b", "c"]

      # Acking what arrived has to finish the entry: nothing will ever deliver "a" again.
      assert [ack] = acks()
      assert [%{batch_index: -1}] = ack.message_id
      assert state.acks.acked == %{}
    end

    test "still completes when part of it was compacted away" do
      state = deliver(worker_state(%{}), ["a", "b", "c"], compacted_out: [1])

      assert delivered_payloads() == ["a", "c"]

      assert [ack] = acks()
      assert [%{batch_index: -1}] = ack.message_id
      assert state.acks.acked == %{}
    end

    test "charges a permit for every message the broker sent, delivered or not" do
      state = %{worker_state(%{}) | flow_initial: 100, flow_threshold: 0, flow_outstanding_permits: 100}

      new_state = deliver(state, ["a", "b", "c"], compacted_out: [1], ack_set: [0b011])

      assert delivered_payloads() == ["a"]
      assert new_state.flow_outstanding_permits == 97
    end
  end

  ## Helpers

  defp worker_state(answers, opts \\ []) do
    {batch_index_ack_enabled, opts} = Keyword.pop(opts, :batch_index_ack_enabled, false)

    struct(
      Worker,
      [
        acks: Ack.new(batch_index_ack_enabled),
        topic: @topic,
        base_topic: @topic,
        subscription_name: "order-service",
        subscription_type: :shared,
        callback_module: Callback,
        callback_state: answers,
        broker_pid: self(),
        consumer_id: 1,
        flow_initial: 0
      ] ++ opts
    )
  end

  # Broker commands are casts, so they arrive in the test process mailbox.
  defp deliver(state, payloads, opts \\ []) do
    command = %Binary.CommandMessage{
      consumer_id: 1,
      message_id: message_id(),
      redelivery_count: 0,
      ack_set: Keyword.get(opts, :ack_set, [])
    }

    metadata = %Binary.MessageMetadata{
      producer_name: "orders-api",
      sequence_id: 0,
      publish_time: 0,
      compression: :NONE,
      num_messages_in_batch: length(payloads)
    }

    compacted_out = Keyword.get(opts, :compacted_out, [])

    payload =
      payloads
      |> Enum.with_index()
      |> Enum.map(fn {payload, index} -> encode_single_message(payload, index in compacted_out) end)
      |> :erlang.iolist_to_binary()

    {:noreply, new_state} = Worker.handle_info({:broker_message, {command, metadata, payload, nil}}, state)
    new_state
  end

  defp deliver_unbatched(state, payload, opts \\ []) do
    entry = Keyword.get(opts, :entry, @entry)
    command = %Binary.CommandMessage{consumer_id: 1, message_id: message_id(entry), redelivery_count: 0}

    # No batch framing, so unwrapping falls back to the payload as it stands.
    metadata = %Binary.MessageMetadata{
      producer_name: "orders-api",
      sequence_id: 0,
      publish_time: 0,
      compression: :NONE,
      num_messages_in_batch: 1
    }

    {:noreply, new_state} = Worker.handle_info({:broker_message, {command, metadata, payload, nil}}, state)
    new_state
  end

  defp message_id(entry \\ @entry) do
    %Binary.MessageIdData{ledgerId: @ledger, entryId: entry, partition: -1, batch_index: -1}
  end

  defp batch_id(index) do
    %{message_id() | batch_index: index, batch_size: 3}
  end

  defp encode_single_message(payload, compacted_out \\ false) do
    metadata =
      Binary.SingleMessageMetadata.encode(%Binary.SingleMessageMetadata{
        payload_size: byte_size(payload),
        compacted_out: compacted_out
      })

    <<byte_size(metadata)::32, metadata::binary, payload::binary>>
  end

  defp acks do
    Enum.filter(receive_commands(), &match?(%Binary.CommandAck{}, &1))
  end

  defp delivered_payloads(acc \\ []) do
    receive do
      {:delivered, payload} -> delivered_payloads([payload | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp receive_commands(acc \\ []) do
    receive do
      {:"$gen_cast", {:send_command, command}} -> receive_commands([command | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
