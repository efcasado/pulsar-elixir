defmodule Pulsar.Producer.SendAsyncTest do
  @moduledoc false
  use ExUnit.Case, async: true

  import Pulsar.Test.Support.BrokerStub, only: [published: 0]

  alias Pulsar.Producer
  alias Pulsar.Producer.Worker
  alias Pulsar.Test.Support.BrokerStub
  alias Pulsar.Test.Support.ProducerState

  # Stands in for a producer worker: it answers the cast, or sits on it, so `await/2` can be
  # driven without a broker behind it.
  defmodule StubWorker do
    @moduledoc false
    use GenServer

    def start_link(answer), do: GenServer.start_link(__MODULE__, answer)

    @impl true
    def init(answer), do: {:ok, answer}

    @impl true
    def handle_cast({:send_message, payload, _opts, from}, :echo = state) do
      GenServer.reply(from, {:ok, payload})
      {:noreply, state}
    end

    def handle_cast({:send_message, _payload, _opts, _from}, :silent = state), do: {:noreply, state}
  end

  setup do
    broker = start_supervised!({BrokerStub, self()})

    %{state: ProducerState.new(broker)}
  end

  describe "taking a send by cast" do
    test "publishes it and parks its caller", ctx do
      {:noreply, state} = cast(ctx.state, "a")

      assert [_sent] = published()
      assert map_size(state.pending_frames) == 1
      assert state.pending_messages == 1
    end

    test "delivers a refusal to its caller rather than returning it", ctx do
      state = %{ctx.state | max_pending_messages: 1}
      {:noreply, state} = cast(state, "a")

      {ref, {:noreply, state}} = cast_with_ref(state, "b")

      assert_received {^ref, {:error, :producer_queue_full}}
      assert map_size(state.pending_frames) == 1
    end

    test "refuses while the producer is still registering", ctx do
      {ref, {:noreply, state}} = cast_with_ref(%{ctx.state | ready: false}, "a")

      assert_received {^ref, {:error, :producer_waiting}}
      assert [] == published()
      assert state.pending_messages == 0
    end

    test "publishes in the order it was handed the messages", ctx do
      state =
        Enum.reduce(["a", "b", "c"], ctx.state, fn payload, acc ->
          {:noreply, next} = cast(acc, payload)
          next
        end)

      assert map_size(state.pending_frames) == 3
      assert ["a", "b", "c"] == Enum.map(published(), & &1.payload)
    end
  end

  describe "await/2" do
    test "answers with what the producer replied" do
      worker = start_supervised!({StubWorker, :echo})

      ref = send_async_to(worker, "a")

      assert {:ok, "a"} = Producer.await(ref)
    end

    test "reports a producer that goes down before answering" do
      worker = start_supervised!({StubWorker, :silent})

      ref = send_async_to(worker, "a")
      GenServer.stop(worker, :shutdown)

      assert {:error, {:producer_died, :shutdown}} = Producer.await(ref)
    end

    test "gives up on its own without cancelling the send" do
      worker = start_supervised!({StubWorker, :silent})

      ref = send_async_to(worker, "a")

      assert {:error, :timeout} = Producer.await(ref, 20)
      assert Process.alive?(worker), "the send is still the producer's to answer"
    end

    test "belongs to the process that started the send" do
      worker = start_supervised!({StubWorker, :echo})
      ref = send_async_to(worker, "a")

      elsewhere = Task.async(fn -> Producer.await(ref, 20) end)

      assert {:error, :timeout} = Task.await(elsewhere)
      assert {:ok, "a"} = Producer.await(ref), "the answer was here all along"
    end

    test "leaves nothing behind for a send it already answered" do
      worker = start_supervised!({StubWorker, :echo})

      ref = send_async_to(worker, "a")
      assert {:ok, "a"} = Producer.await(ref)

      GenServer.stop(worker, :shutdown)

      refute_receive {:DOWN, ^ref, :process, _pid, _reason}, 50
    end
  end

  # `Topology.kind/1` reads anything that is not a topology or group supervisor as a worker, so a
  # stub answers `send/3` as a producer would.
  describe "send/3 over send_async/3" do
    test "answers with what the producer replied" do
      worker = start_supervised!({StubWorker, :echo})

      assert {:ok, "a"} = Producer.send(worker, "a")
    end

    test "gives up with a timeout of its own" do
      worker = start_supervised!({StubWorker, :silent})

      assert {:error, :timeout} = Producer.send(worker, "a", timeout: 20)
    end

    test "reports a producer that goes down" do
      worker = start_supervised!({StubWorker, :silent})
      task = Task.async(fn -> Producer.send(worker, "a") end)

      Process.sleep(20)
      GenServer.stop(worker, :shutdown)

      assert {:error, {:producer_died, :shutdown}} = Task.await(task)
    end
  end

  ## Helpers

  # What `Pulsar.Producer.send_async/3` does once it has resolved a worker.
  defp send_async_to(worker, payload) do
    ref = Process.monitor(worker)
    GenServer.cast(worker, {:send_message, payload, [], {self(), ref}})

    ref
  end

  defp cast(state, payload) do
    {_ref, result} = cast_with_ref(state, payload)

    result
  end

  defp cast_with_ref(state, payload) do
    ref = make_ref()

    {ref, Worker.handle_cast({:send_message, payload, [], {self(), ref}}, state)}
  end
end
