defmodule Pulsar.ConsumerTest do
  use ExUnit.Case, async: true

  alias Pulsar.Consumer
  alias Pulsar.Topology.Group

  describe "await_ready/2" do
    test "reports a missing named consumer after the wait" do
      assert Consumer.await_ready(:missing, client: :consumer_missing_client, timeout: 0) ==
               {:error, :not_found}
    end

    test "reports a stale consumer pid" do
      assert Consumer.await_ready(dead_pid(), timeout: 25) == {:error, :not_found}
    end

    test "validates its options" do
      assert_raise NimbleOptions.ValidationError, fn ->
        Consumer.await_ready(:consumer, timeout: :soon)
      end
    end
  end

  describe "topic/1" do
    test "returns not found for a stale worker pid" do
      consumer = dead_pid()

      assert Consumer.topic(consumer) == {:error, :not_found}
    end

    test "returns not found when a group disappears during traversal" do
      assert Consumer.topic(dying_group()) == {:error, :not_found}
    end
  end

  describe "send_flow/3" do
    test "reports a missing name consistently with stop/2" do
      opts = [client: :consumer_missing_client]

      assert Consumer.send_flow(:missing, 1, opts) == {:error, :not_found}
      assert Consumer.stop(:missing, opts) == {:error, :not_found}
    end

    test "requires a positive permit count for pids and names" do
      assert_raise FunctionClauseError, fn -> Consumer.send_flow(self(), 0) end
      assert_raise FunctionClauseError, fn -> Consumer.send_flow(:consumer, 0) end
    end

    test "returns an error for a stale worker pid" do
      assert {:error, _reason} = Consumer.send_flow(dead_pid(), 1)
    end

    test "returns an error when a group disappears during traversal" do
      assert {:error, _reason} = Consumer.send_flow(dying_group(), 1)
    end
  end

  describe "stop/2" do
    test "rejects a worker pid without stopping it" do
      worker = start_supervised!({Agent, fn -> :worker end})

      assert Consumer.stop(worker) == {:error, :not_found}
      assert Process.alive?(worker)
    end

    test "returns not found for a stale pid" do
      assert Consumer.stop(dead_pid()) == {:error, :not_found}
    end
  end

  defp dead_pid do
    pid = spawn(fn -> :ok end)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}
    pid
  end

  # Topology.kind/1 identifies a group from its proc_lib initial call. This process exits
  # when Supervisor.which_children/1 traverses it, making the otherwise narrow race deterministic.
  defp dying_group do
    caller = self()

    pid =
      spawn(fn ->
        Process.put(:"$initial_call", {:supervisor, Group, 1})
        send(caller, {:ready, self()})

        receive do
          _message -> exit(:gone)
        end
      end)

    assert_receive {:ready, ^pid}
    pid
  end
end
