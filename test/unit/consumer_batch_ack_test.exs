defmodule Pulsar.Consumer.BatchAckTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Pulsar.Consumer.Ack
  alias Pulsar.Consumer.Worker
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.Test.Support.Flow

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
        {:stop, reason} -> {:stop, reason, answers}
      end
    end

    def handle_invalid_message(message, answers) do
      send(self(), {:delivered, message.payload})

      case Map.get(answers, message.payload, :ack) do
        :ack -> {:ok, answers}
        :defer -> {:noreply, answers}
        :nack -> {:error, :rejected_invalid_message, answers}
        {:stop, reason} -> {:stop, reason, answers}
      end
    end
  end

  # Stands in for the dead letter producer. `Pulsar.Producer.send/3` routes a bare pid through
  # `Topology.kind/1`, which calls anything that is not a topology root a worker,
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
    test "a normal callback stop records its message and halts the rest of the batch" do
      answers = %{"b" => {:stop, :normal}}
      assert {:stop, :normal, state} = deliver_result(worker_state(answers), ["a", "b", "c"])

      assert_received {:delivered, "a"}
      assert_received {:delivered, "b"}
      refute_received {:delivered, "c"}
      assert state.callback_state == answers
      assert [] == acks()
      assert map_size(state.acks.acked) == 1
    end

    test "a reasoned callback stop carries its reason" do
      assert {:stop, {:shutdown, :complete}, _state} =
               deliver_result(worker_state(%{"a" => {:stop, {:shutdown, :complete}}}), ["a"])

      assert [_ack] = acks()
    end

    test "an invalid-message callback can acknowledge and stop" do
      state = worker_state(%{"invalid" => {:stop, :normal}})
      command = %Binary.CommandMessage{consumer_id: 1, message_id: message_id()}
      delivery = {:broker_message, {:invalid, command, "invalid", :checksum_mismatch}}

      assert {:stop, :normal, _state} = Worker.handle_info(delivery, state)
      assert_received {:delivered, "invalid"}
      assert [ack] = acks()
      assert ack.validation_error == :ChecksumMismatch
    end

    test "an invalid-message callback can opt into redelivery" do
      state = worker_state(%{"invalid" => :nack}, redelivery_interval: 100)
      command = %Binary.CommandMessage{consumer_id: 1, message_id: message_id()}
      delivery = {:broker_message, {:invalid, command, "invalid", :checksum_mismatch}}

      assert {:noreply, state} = Worker.handle_info(delivery, state)
      assert_received {:delivered, "invalid"}
      assert acks() == []
      assert [%{batch_index: -1} = id] = MapSet.to_list(state.acks.nacked)
      assert {id.ledgerId, id.entryId} == {@ledger, @entry}
    end

    test "does not invent a wire validation error for an unknown high-level reason" do
      state = worker_state(%{})
      command = %Binary.CommandMessage{consumer_id: 1, message_id: message_id()}
      delivery = {:broker_message, {:invalid, command, "invalid", :future_validation_error}}

      assert {:noreply, _state} = Worker.handle_info(delivery, state)
      assert_received {:delivered, "invalid"}
      assert [ack] = acks()
      assert ack.validation_error == nil
    end

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

  describe "acking cumulatively" do
    defp cumulative(answers, opts \\ []) do
      worker_state(answers, Keyword.merge([ack_type: :cumulative, subscription_type: :failover], opts))
    end

    test "sends the wire ack type the subscription cursor moves on" do
      deliver_unbatched(cumulative(%{}), "solo")

      assert [%Binary.CommandAck{ack_type: :Cumulative}] = acks()
    end

    test "does not acknowledge the entry while messages batched after the acked one are deferred" do
      deliver(cumulative(%{"b" => :defer, "c" => :defer}), ["a", "b", "c"])

      # Acking the entry would take "b" and "c" with it, which nothing has processed. The entry
      # before is as far as the cursor can go, and it moves the rest of the way once "c" is acked.
      assert [ack] = acks()
      assert [%{ledgerId: @ledger, entryId: 41, batch_index: -1}] = ack.message_id
    end

    test "acknowledges the entry once its last message is acked" do
      deliver(cumulative(%{"a" => :defer, "b" => :defer}), ["a", "b", "c"])

      assert [ack] = acks()
      assert [%{ledgerId: @ledger, entryId: @entry, batch_index: -1}] = ack.message_id
    end

    test "moves through a batch with ack sets when batch index acking is on" do
      deliver(cumulative(%{"c" => :defer}, batch_index_ack_enabled: true), ["a", "b", "c"])

      assert [first, second] = acks()
      assert first.ack_type == :Cumulative
      assert second.ack_type == :Cumulative
      assert [%{entryId: @entry, ack_set: [0b110], batch_size: 3}] = first.message_id
      assert [%{entryId: @entry, ack_set: [0b100], batch_size: 3}] = second.message_id
    end

    test "starts from the broker's ack set when a partial entry is redelivered" do
      state = cumulative(%{"d" => :defer}, batch_index_ack_enabled: true)

      # "c" was already acknowledged, while "a", "b", and "d" remain outstanding.
      deliver(state, ["a", "b", "c", "d"], ack_set: [0b1011])

      assert delivered_payloads() == ["a", "b", "d"]
      assert [first, second] = Enum.map(acks(), fn ack -> hd(ack.message_id) end)
      assert first.ack_set == [0b1010]
      assert second.ack_set == [0b1000]
    end

    test "leaves a deferred entry unacknowledged until something passes it" do
      state = deliver_unbatched(cumulative(%{"a" => :defer}), "a")
      assert [] == acks()

      deliver_unbatched(state, "b", entry: @entry + 1)

      assert [ack] = acks()
      assert [%{entryId: 43}] = ack.message_id
    end

    test "sends nothing for a message that does not move the cursor on" do
      # "a" and "b" both take the cursor to the entry before this one, so only the first of them
      # is worth a command; "c" then covers the entry itself.
      deliver(cumulative(%{"c" => :defer}), ["a", "b", "c"])

      assert [ack] = acks()
      assert [%{entryId: 41}] = ack.message_id
    end

    test "does not ack an entry the cursor has already passed" do
      state = deliver_unbatched(cumulative(%{}), "b", entry: @entry + 1)
      assert [%{message_id: [%{entryId: 43}]}] = acks()

      deliver_unbatched(state, "a")

      assert [] == acks()
    end

    test "does not let a trailing compacted member acknowledge a deferred visible message" do
      for opts <- [[], [batch_index_ack_enabled: true]] do
        deliver(cumulative(%{"a" => :defer}, opts), ["a", "compacted"], compacted_out: [1])

        assert delivered_payloads() == ["a"]
        assert acks() == []
      end
    end

    test "does not let a compacted member advance a batch-index ack past an earlier callback" do
      state = cumulative(%{"a" => :defer, "c" => :defer}, batch_index_ack_enabled: true)

      deliver(state, ["a", "compacted", "c"], compacted_out: [1])

      assert delivered_payloads() == ["a", "c"]
      assert acks() == []
    end

    test "does not initiate a cumulative ack for an entirely compacted batch" do
      deliver(cumulative(%{}), ["a", "b", "c"], compacted_out: [0, 1, 2])

      assert delivered_payloads() == []
      assert acks() == []
    end
  end

  describe "batch index acking" do
    test "sends partial acknowledgements before a callback stops within a batch" do
      state = worker_state(%{"b" => {:stop, :normal}}, batch_index_ack_enabled: true)

      assert {:stop, :normal, _state} = deliver_result(state, ["a", "b", "c"])

      assert [first, second] = Enum.map(acks(), fn ack -> hd(ack.message_id) end)
      assert first.ack_set == [0b110]
      assert second.ack_set == [0b100]
    end

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
      state = %{
        worker_state(%{})
        | flow_policy: {Flow, :never, []},
          flow_outstanding_permits: 100
      }

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
      state = %{
        worker_state(%{})
        | flow_policy: {Flow, :never, []},
          flow_outstanding_permits: 100
      }

      new_state = deliver(state, ["a", "b", "c"], compacted_out: [1], ack_set: [0b011])

      assert delivered_payloads() == ["a"]
      assert new_state.flow_outstanding_permits == 97
    end
  end

  describe "diverting a batch to the dead letter topic" do
    test "acknowledges nothing when the dead letter topic refuses a message of the entry" do
      state = diverting_state(refuse: ["b"])

      assert_raise MatchError, fn -> deliver(state, ["a", "b", "c"], redelivery_count: 1) end

      assert acks() == []
    end

    test "acknowledges the entry once every message of it has been diverted" do
      state = diverting_state()

      state = deliver(state, ["a", "b", "c"], redelivery_count: 1)

      assert diverted_payloads() == ["a", "b", "c"]
      assert [ack] = acks()
      assert [%{batch_index: -1}] = ack.message_id
      assert state.acks.acked == %{}
    end
  end

  describe "applying the flow policy" do
    test "reports one total for callback-visible and hidden consumption" do
      state = %{worker_state(%{}) | flow_outstanding_permits: 100}

      deliver(state, ["a", "b", "c"], compacted_out: [1], ack_set: [0b011])

      assert delivered_payloads() == ["a"]
      assert permits_reported() == [%{consumed: 3, outstanding: 97}]
    end

    test "counts a delivery diverted in full, which reaches no callback at all" do
      state = %{diverting_state() | flow_outstanding_permits: 100}

      deliver(state, ["a", "b", "c"], redelivery_count: 1)

      assert delivered_payloads() == []
      assert diverted_payloads() == ["a", "b", "c"]
      assert permits_reported() == [%{consumed: 3, outstanding: 97}]
    end

    test "grants what the policy asks for, without asking it again" do
      state = %{
        worker_state(%{})
        | flow_policy: {Flow, :grant_fixed, [self(), 50]},
          flow_outstanding_permits: 100
      }

      new_state = deliver(state, ["a"])

      assert permits_reported() == [%{consumed: 1, outstanding: 99}]
      assert new_state.flow_outstanding_permits == 149

      assert [flow] = flow_commands()
      assert flow.messagePermits == 50
    end

    test "the :auto policy refills a consumer that has reached its threshold" do
      state = %{
        worker_state(%{})
        | flow_policy: :auto,
          flow_threshold: 50,
          flow_refill: 50,
          flow_outstanding_permits: 51
      }

      new_state = deliver(state, ["a", "b", "c"])

      assert new_state.flow_outstanding_permits == 98
      assert [%Binary.CommandFlow{messagePermits: 50}] = flow_commands()
    end

    test "the :auto policy leaves a consumer above its threshold alone" do
      state = %{
        worker_state(%{})
        | flow_policy: :auto,
          flow_threshold: 50,
          flow_refill: 50,
          flow_outstanding_permits: 100
      }

      new_state = deliver(state, ["a", "b", "c"])

      assert new_state.flow_outstanding_permits == 97
      assert flow_commands() == []
    end

    test "a policy that grants nothing never refills, however low the window gets" do
      state = %{worker_state(%{}) | flow_policy: {Flow, :never, []}, flow_outstanding_permits: 3}

      new_state = deliver(state, ["a", "b", "c"])

      assert new_state.flow_outstanding_permits == 0
      assert flow_commands() == []
    end
  end

  ## Helpers

  # A consumer whose dead letter producer is the stub above, past its redelivery limit.
  # DeadLetter.producer/1 looks for a `{:dead_letter, topic}` child reported as a supervisor.
  defp diverting_state(opts \\ []) do
    child = %{
      id: {:dead_letter, "dlq"},
      start: {DeadLetterProducer, :start_link, [{Keyword.get(opts, :refuse, []), self()}]},
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
    {ack_opts, opts} = Keyword.split(opts, [:batch_index_ack_enabled, :ack_type])

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
        flow_policy: {Flow, :report, [self()]},
        flow_initial: 0
      ] ++ opts
    )
  end

  # Broker commands are casts, so they arrive in the test process mailbox.
  defp deliver(state, payloads, opts \\ []) do
    {:noreply, new_state} = deliver_result(state, payloads, opts)
    new_state
  end

  defp deliver_result(state, payloads, opts \\ []) do
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

    Worker.handle_info({:broker_message, {command, metadata, payload, nil}}, state)
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
      num_messages_in_batch: 0
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

  defp flow_commands do
    Enum.filter(receive_commands(), &match?(%Binary.CommandFlow{}, &1))
  end

  defp delivered_payloads(acc \\ []) do
    receive do
      {:delivered, payload} -> delivered_payloads([payload | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp permits_reported(acc \\ []) do
    receive do
      {:permits, flow} -> permits_reported([flow | acc])
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
