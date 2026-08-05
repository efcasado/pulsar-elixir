defmodule Pulsar.Producer.SendTest do
  @moduledoc false
  use ExUnit.Case, async: true

  import Pulsar.Test.Support.BrokerStub, only: [published: 0]

  alias Pulsar.Producer.Worker
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.Test.Support.BrokerStub
  alias Pulsar.Test.Support.ProducerState

  setup do
    broker = start_supervised!({BrokerStub, self()})

    %{state: ProducerState.new(broker, send_timeout: 50)}
  end

  describe "a send the broker never acknowledges" do
    test "answers its caller and stops tracking it", ctx do
      {from, state} = send_message(ctx.state, "a")

      assert map_size(state.pending_frames) == 1
      assert [_frame] = published()

      state = expire(state)

      assert_replied(from, {:error, :send_timeout})
      assert state.pending_frames == %{}
    end

    test "answers every caller of a batch it carried", ctx do
      state = %{ctx.state | batch_enabled: true, batch_size: 2}

      {first, state} = send_message(state, "a")
      {second, state} = send_message(state, "b")

      state = expire(state)

      assert_replied(first, {:error, :send_timeout})
      assert_replied(second, {:error, :send_timeout})
      assert state.pending_frames == %{}
      assert state.pending_messages == 0
    end

    test "drops the rest of a chunked message with it", ctx do
      state = %{ctx.state | chunking_enabled: true, max_message_size: 8}

      {from, state} = send_message(state, String.duplicate("x", 24))
      assert map_size(state.pending_frames) == 3

      state = expire(state)

      assert_replied(from, {:error, :send_timeout})
      assert state.pending_frames == %{}

      # Three frames, one caller: the count lands back on nothing owed.
      assert state.pending_messages == 0
    end

    test "leaves an already answered send alone", ctx do
      {from, state} = send_message(ctx.state, "a")
      [sequence_id] = Map.keys(state.pending_frames)

      receipt = %Binary.CommandSendReceipt{sequence_id: sequence_id, message_id: message_id()}
      {:noreply, state} = Worker.handle_info({:send_receipt, receipt}, state)
      assert_replied(from, {:ok, message_id()})

      # The timer still comes due; by then it is carrying nothing.
      state = expire(state)

      refute_received {_ref, {:error, :send_timeout}}
      assert state.pending_frames == %{}
    end

    test "spares a send that has not been waiting long enough", ctx do
      {first, state} = send_message(ctx.state, "a")

      # Only the first is old enough once the timer comes due.
      Process.sleep(60)
      {_second, state} = send_message(state, "b")

      {:noreply, state} = Worker.handle_info(:expire_sends, state)

      assert_replied(first, {:error, :send_timeout})
      refute_received {_ref, {:error, :send_timeout}}
      assert map_size(state.pending_frames) == 1
      refute is_nil(state.send_timeout_timer), "re-aimed at the send it is still carrying"
    end

    test "arms one timer for the producer, not one per send", ctx do
      {_from, state} = send_message(ctx.state, "a")
      armed = state.send_timeout_timer

      {_from, state} = send_message(state, "b")
      {_from, state} = send_message(state, "c")

      assert state.send_timeout_timer == armed
      assert map_size(state.pending_frames) == 3
    end

    test "answers a caller still waiting to be batched", ctx do
      state = %{ctx.state | batch_enabled: true, batch_size: 100, flush_interval: 30_000}

      {from, state} = send_message(state, "a")

      assert state.batched == 1
      assert [] == published(), "nothing was published: it is waiting for a flush"

      state = expire(state)

      assert_replied(from, {:error, :send_timeout})
      assert state.batch == []
      assert state.batch_started_at == nil
      assert state.pending_messages == 0
    end

    test "spares a batch that has not been waiting long enough", ctx do
      state = %{ctx.state | batch_enabled: true, batch_size: 100, flush_interval: 30_000}

      {_from, state} = send_message(state, "a")
      {:noreply, state} = Worker.handle_info(:expire_sends, state)

      refute_received {_ref, {:error, :send_timeout}}
      assert state.batched == 1
      assert state.batch_started_at
    end

    test "hands a flushed batch the clock it started on, not a fresh one", ctx do
      state = %{ctx.state | batch_enabled: true, batch_size: 2}

      {_first, state} = send_message(state, "a")

      # As if the first message had been sitting in the batch for a second.
      started_at = state.batch_started_at - 1_000
      {_second, state} = send_message(%{state | batch_started_at: started_at}, "b")

      assert [{_sequence_id, {_callers, _metadata, sent_at}}] = Map.to_list(state.pending_frames)
      assert sent_at == started_at
      assert is_nil(state.batch_started_at)
    end

    test "schedules nothing when the timeout is off", ctx do
      {_from, state} = send_message(%{ctx.state | send_timeout: false}, "a")

      assert is_nil(state.send_timeout_timer)
      refute_received :expire_sends
    end
  end

  describe "a send the broker rejects" do
    test "drops the rest of a chunked message with it", ctx do
      state = %{ctx.state | chunking_enabled: true, max_message_size: 8}

      {from, state} = send_message(state, String.duplicate("x", 24))
      assert map_size(state.pending_frames) == 3

      state = reject_oldest(state)

      assert_replied(from, {:error, {:PersistenceError, "storage down"}})
      assert state.pending_frames == %{}

      # Same again: three frames, one caller.
      assert state.pending_messages == 0
    end

    # Chunks left behind expired later, answering a caller that already had its error.
    test "answers its caller once, and not again when the timer comes due", ctx do
      state = %{ctx.state | chunking_enabled: true, max_message_size: 8}

      {from, state} = send_message(state, String.duplicate("x", 24))
      state = state |> reject_oldest() |> expire()

      assert_replied(from, {:error, {:PersistenceError, "storage down"}})
      refute_received {_ref, {:error, :send_timeout}}
      assert state.pending_messages == 0
    end
  end

  describe "a producer carrying its limit of sends" do
    test "refuses another rather than queueing it", ctx do
      state = %{ctx.state | max_pending_messages: 1}

      {_from, state} = send_message(state, "a")

      assert {:reply, {:error, :producer_queue_full}, ^state} =
               Worker.handle_call({:send_message, "b", []}, {self(), make_ref()}, state)
    end

    test "counts messages waiting to be batched", ctx do
      state = %{ctx.state | batch_enabled: true, batch_size: 10, max_pending_messages: 2}

      {_from, state} = send_message(state, "a")
      {_from, state} = send_message(state, "b")

      assert state.batched == 2
      assert [] == published(), "neither has been published yet"

      assert {:reply, {:error, :producer_queue_full}, _state} =
               Worker.handle_call({:send_message, "c", []}, {self(), make_ref()}, state)
    end

    test "accepts again once a send is acknowledged", ctx do
      state = %{ctx.state | max_pending_messages: 1}

      {_from, state} = send_message(state, "a")
      [sequence_id] = Map.keys(state.pending_frames)

      receipt = %Binary.CommandSendReceipt{sequence_id: sequence_id, message_id: message_id()}
      {:noreply, state} = Worker.handle_info({:send_receipt, receipt}, state)

      assert {:noreply, _state} = Worker.handle_call({:send_message, "b", []}, {self(), make_ref()}, state)
    end

    # Counting frames took a producer past its limit on one large message.
    test "counts a chunked message once, however many frames carry it", ctx do
      state = %{ctx.state | chunking_enabled: true, max_message_size: 8, max_pending_messages: 2}

      {_from, state} = send_message(state, String.duplicate("x", 40))

      assert map_size(state.pending_frames) == 5, "five frames"
      assert state.pending_messages == 1, "one message"

      assert {:noreply, _state} = Worker.handle_call({:send_message, "b", []}, {self(), make_ref()}, state)
    end

    test "lets a send go again once its caller has been answered", ctx do
      {from, state} = send_message(ctx.state, "a")
      assert state.pending_messages == 1

      [sequence_id] = Map.keys(state.pending_frames)
      receipt = %Binary.CommandSendReceipt{sequence_id: sequence_id, message_id: message_id()}
      {:noreply, state} = Worker.handle_info({:send_receipt, receipt}, state)

      assert_replied(from, {:ok, message_id()})
      assert state.pending_messages == 0
    end

    test "does not count a send it refused outright", ctx do
      # Nothing is published, so nothing waits: the caller already has its error.
      state = %{ctx.state | chunking_enabled: true, max_message_size: 0, broker_max_message_size: 0}

      assert {:reply, {:error, :metadata_too_large}, state} =
               Worker.handle_call({:send_message, "a", []}, {self(), make_ref()}, state)

      assert state.pending_messages == 0
    end

    test "carries as many as it likes when the limit is off", ctx do
      state = %{ctx.state | max_pending_messages: false}

      state = Enum.reduce(1..20, state, fn i, acc -> elem(send_message(acc, "msg-#{i}"), 1) end)

      assert map_size(state.pending_frames) == 20
    end
  end

  ## Helpers

  defp send_message(state, payload) do
    from = {self(), make_ref()}
    {:noreply, new_state} = Worker.handle_call({:send_message, payload, []}, from, state)

    {from, new_state}
  end

  defp reject_oldest(state) do
    [sequence_id | _] = Enum.sort(Map.keys(state.pending_frames))
    error = %Binary.CommandSendError{sequence_id: sequence_id, error: :PersistenceError, message: "storage down"}
    {:noreply, new_state} = Worker.handle_info({:send_error, error}, state)

    new_state
  end

  # Waits for the producer's timer to come due, then hands it back as the producer would get it.
  defp expire(state) do
    assert_receive :expire_sends, 500
    {:noreply, new_state} = Worker.handle_info(:expire_sends, state)

    new_state
  end

  defp assert_replied({_pid, ref}, reply) do
    assert_received {^ref, ^reply}
  end

  defp message_id, do: %Binary.MessageIdData{ledgerId: 7, entryId: 42, partition: -1}
end
