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

  # Stands in for the dead letter producer. `Pulsar.Producer.send/3` routes a bare pid through
  # `Topology.kind/1`, which calls anything that is not a topology or group supervisor a worker,
  # so answering `{:send_message, ...}` is all it takes to be one.
  defmodule DeadLetterProducer do
    @moduledoc false
    use GenServer

    def start_link({refuse, notify_pid}), do: GenServer.start_link(__MODULE__, {refuse, notify_pid})

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_cast({:send_message, payload, _opts, from}, {refuse, notify_pid} = state) do
      if payload in refuse do
        GenServer.reply(from, {:error, :message_too_large})
      else
        send(notify_pid, {:diverted, payload})
        GenServer.reply(from, {:ok, %Binary.MessageIdData{ledgerId: 9, entryId: 1, partition: -1}})
      end

      {:noreply, state}
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

    test "starts a tally nothing will finish when a completed entry is acked again" do
      state = deliver(worker_state(%{}), ["a", "b", "c"])
      assert [_ack] = acks()

      {:reply, :ok, state} = Worker.handle_call({:ack, [batch_id(1)]}, self(), state)

      # The entry is gone, so the second ack counts one message of three off an empty tally.
      assert [] == acks()
      assert map_size(state.acks.acked) == 1
    end
  end

  describe "nacking a batched message" do
    test "does not queue a nack for a redelivery that will never be requested" do
      state = deliver(worker_state(%{"b" => :nack}), ["a", "b", "c"])

      # :trigger_redelivery is only scheduled when an interval is configured, so anything
      # recorded here would sit forever with nothing to drain it.
      assert {[], _acks} = Ack.take_nacked(state.acks)
    end

    test "collapses ids to the entry the broker would redeliver" do
      state = deliver(worker_state(%{"a" => :nack, "c" => :nack}, redelivery_interval: 1000), ["a", "b", "c"])

      assert [%{batch_index: -1} = id] = MapSet.to_list(state.acks.nacked)
      assert {@ledger, @entry} == {id.ledgerId, id.entryId}
    end

    test "collapses ids handed to Pulsar.Consumer.nack/2 to their entry" do
      state = worker_state(%{}, redelivery_interval: 1000)

      {:reply, :ok, state} = Worker.handle_call({:nack, [batch_id(0), batch_id(2)]}, self(), state)

      assert [%{batch_index: -1} = id] = MapSet.to_list(state.acks.nacked)
      assert {@ledger, @entry} == {id.ledgerId, id.entryId}
    end

    test "keeps the entry's tally when nothing will ask the nacked message back" do
      state = deliver(worker_state(%{"b" => :nack}), ["a", "b", "c"])

      assert [] == acks()
      assert map_size(state.acks.acked) == 1

      # "a" and "c" were acked before the nack, so acking "b" out of band completes the entry.
      {:reply, :ok, state} = Worker.handle_call({:ack, [batch_id(1)]}, self(), state)

      assert [ack] = acks()
      assert [%{batch_index: -1}] = ack.message_id
      assert state.acks.acked == %{}
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

    test "reads a set that spans words, and the sign the broker wrote it with" do
      payloads = Enum.map(0..69, &"msg-#{&1}")

      # Bits 1..63 of the first word, which sets its sign bit, and 64..69 of the second: only
      # "msg-0" was acked before.
      state = deliver(worker_state(%{}), payloads, ack_set: [-2, 0b111111])

      assert delivered_payloads() == tl(payloads)

      assert [ack] = acks()
      assert [%{batch_index: -1}] = ack.message_id
      assert state.acks.acked == %{}
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

    test "keeps its full width under batch index acking, so it can still complete" do
      state = worker_state(%{}, batch_index_ack_enabled: true)

      # "a" was acked by whoever held the entry before, so only "b" and "c" are delivered.
      state = deliver(state, ["a", "b", "c"], ack_set: [0b110])

      assert delivered_payloads() == ["b", "c"]

      # batch_size comes from the entry's own metadata, not from how much of it was delivered,
      # so counting "a" off and acking the two that arrived still reaches the whole entry.
      assert [first, second, third] = Enum.map(acks(), fn ack -> hd(ack.message_id) end)
      assert {first.ack_set, first.batch_size} == {[0b110], 3}
      assert {second.ack_set, second.batch_size} == {[0b100], 3}
      assert third.batch_index == -1
      assert state.acks.acked == %{}
    end

    test "still completes when part of it was compacted away" do
      state = deliver(worker_state(%{}), ["a", "b", "c"], compacted_out: [1])

      assert delivered_payloads() == ["a", "c"]

      assert [ack] = acks()
      assert [%{batch_index: -1}] = ack.message_id
      assert state.acks.acked == %{}
    end

    test "is acknowledged when it has nothing left to deliver at all" do
      state = deliver(worker_state(%{}), ["a", "b", "c"], compacted_out: [0, 1, 2])

      assert delivered_payloads() == []

      # No callback runs, so nothing else could ever acknowledge this entry.
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

  describe "diverting a batch to the dead letter topic" do
    test "does not acknowledge the entry when one message of it fails to divert" do
      state = diverting_state(refuse: ["b"])

      state = deliver(state, ["a", "b", "c"], redelivery_count: 1)

      # "a" and "c" reached the dead letter topic, but "b" did not, so the entry is still owed.
      assert diverted_payloads() == ["a", "c"]
      assert [] == acks()

      # It is asked for again, so its tally goes: the entry comes back whole and has to answer
      # for "a" and "c" a second time before it can be acknowledged.
      assert {[%{batch_index: -1}], _acks} = Ack.take_nacked(state.acks)
      assert state.acks.acked == %{}
    end

    test "acknowledges the entry once every message of it has been diverted" do
      state = diverting_state(refuse: [])

      state = deliver(state, ["a", "b", "c"], redelivery_count: 1)

      assert diverted_payloads() == ["a", "b", "c"]
      assert [ack] = acks()
      assert [%{batch_index: -1}] = ack.message_id
      assert state.acks.acked == %{}
    end
  end

  ## Helpers

  # A consumer whose dead letter producer is the stub above, past its redelivery limit.
  # DeadLetter.producer/1 looks for a `{:dead_letter, topic}` child reported as a supervisor.
  defp diverting_state(opts) do
    child = %{
      id: {:dead_letter, "dlq"},
      start: {DeadLetterProducer, :start_link, [{Keyword.fetch!(opts, :refuse), self()}]},
      type: :supervisor
    }

    root =
      start_supervised!(%{
        id: :dead_letter_root,
        start: {Supervisor, :start_link, [[child], [strategy: :one_for_one]]}
      })

    # A failed divert is nacked, so the consumer needs an interval for it to come back at all.
    %{
      worker_state(%{}, redelivery_interval: 1000)
      | dead_letter_root: root,
        dead_letter_topic: "dlq",
        max_redelivery: 1
    }
  end

  defp diverted_payloads(acc \\ []) do
    receive do
      {:diverted, payload} -> diverted_payloads([payload | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp worker_state(answers, opts \\ []) do
    {ack_opts, opts} = Keyword.split(opts, [:batch_index_ack_enabled])

    struct(
      Worker,
      [
        acks: Ack.new(ack_opts),
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
      redelivery_count: Keyword.get(opts, :redelivery_count, 0),
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

  defp encode_single_message(payload, compacted_out) do
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
