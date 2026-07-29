defmodule Pulsar.TopologyTest do
  use ExUnit.Case, async: true

  alias Pulsar.Topology

  defmodule Owner do
    @moduledoc false
    use GenServer

    def start_link(_), do: GenServer.start_link(__MODULE__, nil)
    def spawn_child(pid), do: GenServer.call(pid, :spawn_child)

    @impl true
    def init(nil), do: {:ok, nil}

    @impl true
    def handle_call(:spawn_child, _from, state) do
      {:ok, child} = Agent.start_link(fn -> :resource end)
      {:reply, child, state}
    end
  end

  describe "remove/1 when the first ancestor is not a supervisor" do
    # Started with start_link/1 from an ordinary process, $ancestors heads with the caller.
    # Asking it to terminate_child crashes it on an unmatched call.
    test "stops the resource without disturbing its caller" do
      {:ok, owner} = Owner.start_link(nil)
      resource = Owner.spawn_child(owner)
      ref = Process.monitor(owner)

      assert Topology.remove(resource) == :ok
      refute Process.alive?(resource)

      refute_receive {:DOWN, ^ref, :process, _pid, _reason}, 200
      assert Process.alive?(owner)
    end
  end

  describe "partitions/1" do
    test "counts a partition whose group is not currently running" do
      # which_children reports a child's pid as :restarting or :undefined while it is between
      # lives. Pulsar partitions never shrink, so a transient drop in the count would be wrong,
      # and routing already hashes over every configured partition including those.
      children =
        for index <- 0..2 do
          %{id: "p-#{index}", start: {Supervisor, :start_link, [[], [strategy: :one_for_one]]}, type: :supervisor}
        end

      {:ok, root} = Supervisor.start_link(children, strategy: :one_for_one)
      assert Topology.partitions(root) == 3

      :ok = Supervisor.terminate_child(root, "p-1")
      assert [{"p-1", :undefined, :supervisor, _}] = Enum.filter(Supervisor.which_children(root), &(elem(&1, 0) == "p-1"))

      assert Topology.partitions(root) == 3
    end
  end
end
