defmodule Pulsar.Producer.DelayedDeliveryTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Pulsar.Producer.Worker
  alias Pulsar.Protocol

  # Answers the :gen_statem.call publish_message/2 makes, and hands the frame to the test.
  defmodule BrokerStub do
    @moduledoc false
    use GenServer

    def start_link(notify_pid), do: GenServer.start_link(__MODULE__, notify_pid)

    @impl true
    def init(notify_pid), do: {:ok, notify_pid}

    @impl true
    def handle_call({:publish_message, frame}, _from, notify_pid) do
      send(notify_pid, {:published, frame})
      {:reply, :ok, notify_pid}
    end
  end

  @at_time 1_900_000_000_000

  setup do
    broker = start_supervised!({BrokerStub, self()})

    %{state: producer_state(broker)}
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

  defp published(acc \\ []) do
    receive do
      {:published, frame} ->
        {:ok, {command, metadata, payload, _broker_metadata}} = Protocol.decode(frame)
        published([%{command: command, metadata: metadata, payload: payload} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
