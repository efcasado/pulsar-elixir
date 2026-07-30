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

  defmodule StubWorker do
    @moduledoc false
    use Agent

    def start_link(opts), do: Agent.start_link(fn -> Keyword.fetch!(opts, :topic) end)
  end

  @topic "persistent://public/default/t"
  @name "#{@topic}-producer"

  defp start_topology(partitions) do
    registry = :"registry-#{System.unique_integer([:positive])}"
    start_supervised!({Registry, keys: :unique, name: registry})

    opts = [
      topic: @topic,
      name: @name,
      client: :test,
      partitions: partitions,
      count_key: 1,
      partition_discovery_interval_ms: false
    ]

    root =
      if partitions == 0 do
        start_supervised!(%{id: :root, start: {Pulsar.Group, :start_link, [StubWorker, registry, :count_key, opts]}})
      else
        start_supervised!(%{
          id: :root,
          start: {Supervisor, :start_link, [Topology, {StubWorker, registry, :count_key, opts}]},
          type: :supervisor
        })
      end

    {root, registry}
  end

  describe "partitions/1" do
    test "counts a partition whose group is not currently running" do
      # which_children reports a child's pid as :restarting or :undefined while it is between
      # lives. Pulsar partitions never shrink, so a transient drop in the count would be wrong,
      # and routing already hashes over every configured partition including those.
      {root, _registry} = start_topology(3)
      assert Topology.partitions(root) == 3

      :ok = Supervisor.terminate_child(root, Pulsar.Topic.partition(@topic, 1))

      assert Topology.partitions(root) == 3
    end

    test "is zero for a non-partitioned topic" do
      {root, _registry} = start_topology(0)

      assert Topology.partitions(root) == 0
    end
  end

  describe "groups/1" do
    # Checked against the registry rather than by counting: an index taken from a child's
    # position rather than parsed from its name would still produce 0..11, but would pair the
    # wrong group with it as soon as which_children reports them in any other order.
    test "pairs each index with the group registered for that partition" do
      {root, registry} = start_topology(12)
      groups = Topology.groups(root)

      assert length(groups) == 12

      for {index, pid} <- groups do
        assert [{^pid, _value}] = Registry.lookup(registry, Pulsar.Topic.partition(@name, index))
      end
    end

    test "reports a partition between lives without dropping it" do
      {root, _registry} = start_topology(3)
      :ok = Supervisor.terminate_child(root, Pulsar.Topic.partition(@topic, 1))

      assert List.keyfind(Topology.groups(root), 1, 0) == {1, :undefined}
    end

    test "answers a non-partitioned topic with itself as the only group" do
      {root, _registry} = start_topology(0)

      assert Topology.groups(root) == [{0, root}]
    end
  end
end
