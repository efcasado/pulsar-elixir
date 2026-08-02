defmodule Pulsar.TopologyTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.Test.Support.Utils
  alias Pulsar.Topology
  alias Pulsar.Topology.Discovery

  setup [:telemetry_listen]

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

  describe "remove/2 with an expected owner" do
    test "removes the resource directly and falls back when the expected owner is wrong" do
      owner = start_dynamic_supervisor()
      other = start_dynamic_supervisor()

      {:ok, directly_removed} = DynamicSupervisor.start_child(owner, {Agent, fn -> :resource end})
      assert Topology.remove(directly_removed, owner) == :ok
      refute Process.alive?(directly_removed)

      {:ok, removed_via_fallback} = DynamicSupervisor.start_child(owner, {Agent, fn -> :resource end})
      assert Topology.remove(removed_via_fallback, other) == :ok
      refute Process.alive?(removed_via_fallback)
      assert DynamicSupervisor.which_children(owner) == []
    end
  end

  defp start_dynamic_supervisor do
    start_supervised!(%{
      id: {:dynamic_supervisor, System.unique_integer([:positive])},
      start: {DynamicSupervisor, :start_link, [[strategy: :one_for_one]]},
      type: :supervisor
    })
  end

  defmodule StubWorker do
    @moduledoc false
    use Agent

    def start_link(opts), do: Agent.start_link(fn -> Keyword.fetch!(opts, :topic) end)
  end

  defmodule DisappearingSupervisor do
    @moduledoc false

    def start_link(_opts) do
      pid =
        spawn_link(fn ->
          receive do
            _message -> exit(:gone)
          end
        end)

      {:ok, pid}
    end
  end

  @topic "persistent://public/default/t"
  @name "#{@topic}-producer"

  defp start_topology(partitions) do
    resolver = fn _topic, _opts -> {:ok, partitions} end
    {root, registry} = start_async_topology(resolver)

    expected_status =
      if partitions == 0,
        do: {:ready, :non_partitioned},
        else: {:ready, {:partitioned, partitions}}

    :ok = Utils.wait_for(fn -> Topology.status(root) == expected_status end, 100, 10)

    {root, registry}
  end

  defp start_async_topology(resolver, opts \\ [], controller_opts \\ []) do
    registry = :"registry-#{System.unique_integer([:positive])}"
    start_supervised!({Registry, keys: :unique, name: registry})

    topology_opts =
      Keyword.merge(
        [
          topic: @topic,
          name: @name,
          client: :test,
          count_key: 1,
          partition_discovery_interval_ms: false
        ],
        opts
      )

    root =
      start_supervised!(%{
        id: {:root, System.unique_integer([:positive])},
        start:
          {Topology, :start_link,
           [StubWorker, registry, :count_key, topology_opts, Keyword.put(controller_opts, :resolver, resolver)]},
        type: :supervisor
      })

    {root, registry}
  end

  describe "asynchronous initialization" do
    test "starts and registers the stable root before metadata resolves" do
      test_pid = self()

      resolver = fn _topic, _opts ->
        send(test_pid, {:resolution_started, self()})

        receive do
          :resolve -> {:ok, 2}
        end
      end

      {root, registry} = start_async_topology(resolver)

      assert_receive {:resolution_started, resolver_pid}
      assert Process.alive?(root)
      assert [{^root, _value}] = Registry.lookup(registry, @name)
      assert Topology.groups(root) == []

      send(resolver_pid, :resolve)

      :ok = Utils.wait_for(fn -> Topology.status(root) == {:ready, {:partitioned, 2}} end, 100, 10)
      assert Topology.status(root) == {:ready, {:partitioned, 2}}
      assert length(Topology.groups(root)) == 2
    end

    test "retries failed initialization without blocking the topology" do
      attempts = start_supervised!({Agent, fn -> 0 end})

      resolver = fn _topic, _opts ->
        attempt = Agent.get_and_update(attempts, &{&1, &1 + 1})
        if attempt == 0, do: {:error, :no_broker_available}, else: {:ok, 3}
      end

      {root, _registry} = start_async_topology(resolver, count_key: 2)

      :ok = Utils.wait_for(fn -> Topology.status(root) == {:ready, {:partitioned, 3}} end, 100, 10)
      assert Agent.get(attempts, & &1) >= 2
      assert length(Topology.groups(root)) == 3
    end

    test "a false polling interval still performs initial discovery exactly once" do
      test_pid = self()

      resolver = fn _topic, _opts ->
        send(test_pid, :resolved)
        {:ok, 0}
      end

      {root, _registry} = start_async_topology(resolver)

      :ok = Utils.wait_for(fn -> Topology.status(root) == {:ready, :non_partitioned} end, 100, 10)
      assert_receive :resolved
      refute_receive :resolved, 150
      assert Topology.status(root) == {:ready, :non_partitioned}
      assert [{0, group}] = Topology.groups(root)
      assert is_pid(group)
    end

    @tag telemetry_listen: [
           [:pulsar, :topology, :discovery, :stop],
           [:pulsar, :topology, :reconciliation, :stop]
         ]
    test "locally revives a non-partitioned group when metadata polling is disabled" do
      test_pid = self()
      topic = "#{@topic}-local-reconciliation"

      resolver = fn resolved_topic, _opts ->
        send(test_pid, {:resolved, resolved_topic})
        {:ok, 0}
      end

      {root, _registry} =
        start_async_topology(
          resolver,
          [topic: topic, name: "#{topic}-producer", partition_discovery_interval_ms: false],
          reconciliation_interval_ms: 250
        )

      :ok = Utils.wait_for(fn -> Topology.status(root) == {:ready, :non_partitioned} end, 100, 10)
      assert_receive {:resolved, ^topic}

      assert_receive {:telemetry_event,
                      %{
                        event: [:pulsar, :topology, :discovery, :stop],
                        metadata: %{
                          topic: ^topic,
                          client: :test,
                          success: true,
                          partition_count: 0
                        }
                      }}

      [{0, old_group}] = Topology.groups(root)
      [{_id, worker, :worker, _modules}] = Supervisor.which_children(old_group)
      ref = Process.monitor(old_group)

      :ok = Agent.stop(worker)
      assert_receive {:DOWN, ^ref, :process, ^old_group, _reason}
      assert Topology.groups(root) == [{0, :undefined}]

      :ok =
        Utils.wait_for(
          fn ->
            case Topology.groups(root) do
              [{0, new_group}] when is_pid(new_group) -> new_group != old_group
              _not_running -> false
            end
          end,
          100,
          10
        )

      assert_receive {:telemetry_event,
                      %{
                        event: [:pulsar, :topology, :reconciliation, :stop],
                        metadata: %{
                          topic: ^topic,
                          source: :local,
                          success: true,
                          desired_partition_count: 0,
                          partition_count: 0,
                          revived_groups: [0]
                        }
                      }}

      refute_receive {:resolved, ^topic}, 300
    end

    test "periodically reconciles partitions added after initialization" do
      test_pid = self()
      responses = start_supervised!({Agent, fn -> [2, 4, 2] end})

      resolver = fn _topic, _opts ->
        partitions =
          Agent.get_and_update(responses, fn
            [current] -> {current, [current]}
            [current | rest] -> {current, rest}
          end)

        send(test_pid, {:resolved, partitions})
        {:ok, partitions}
      end

      {root, _registry} = start_async_topology(resolver, partition_discovery_interval_ms: 10)

      assert_receive {:resolved, 2}
      :ok = Utils.wait_for(fn -> Topology.status(root) == {:ready, {:partitioned, 2}} end, 100, 10)
      assert_receive {:resolved, 4}
      :ok = Utils.wait_for(fn -> length(Topology.groups(root)) == 4 end, 100, 10)
      assert_receive {:resolved, 2}
      :ok = Utils.wait_for(fn -> Topology.status(root) == {:ready, {:partitioned, 4}} end, 100, 10)
      assert Topology.status(root) == {:ready, {:partitioned, 4}}
      assert length(Topology.groups(root)) == 4
    end

    test "periodically revives every partition group after they stop normally" do
      test_pid = self()
      resolutions = start_supervised!({Agent, fn -> 0 end})

      resolver = fn _topic, _opts ->
        case Agent.get_and_update(resolutions, &{&1, &1 + 1}) do
          0 ->
            {:ok, 2}

          _later ->
            send(test_pid, {:resolution_started, self()})

            receive do
              :resolve -> {:ok, 2}
            end
        end
      end

      {root, _registry} = start_async_topology(resolver, partition_discovery_interval_ms: 100)

      :ok = Utils.wait_for(fn -> Topology.status(root) == {:ready, {:partitioned, 2}} end, 100, 10)
      assert_receive {:resolution_started, discovery}, 1_000

      old_groups = Map.new(Topology.groups(root))

      refs =
        Enum.map(old_groups, fn {_index, group} ->
          [{_id, worker, :worker, _modules}] = Supervisor.which_children(group)
          ref = Process.monitor(group)
          :ok = Agent.stop(worker)
          {ref, group}
        end)

      for {ref, group} <- refs do
        assert_receive {:DOWN, ^ref, :process, ^group, _reason}
      end

      assert Process.alive?(root)
      assert Enum.all?(Topology.groups(root), &match?({_index, :undefined}, &1))

      send(discovery, :resolve)

      :ok =
        Utils.wait_for(
          fn ->
            groups = Map.new(Topology.groups(root))

            Enum.all?(old_groups, fn {index, old_group} ->
              case Map.fetch(groups, index) do
                {:ok, new_group} when is_pid(new_group) -> new_group != old_group
                _not_running -> false
              end
            end)
          end,
          100,
          10
        )

      for {index, new_group} <- Topology.groups(root) do
        assert [{_id, new_worker, :worker, _modules}] = Supervisor.which_children(new_group)
        assert Agent.get(new_worker, & &1) == Pulsar.Topic.partition(@topic, index)
      end
    end
  end

  # workers/1 answers only for the modules a topology is actually made of, so these declare
  # those rather than going through start_topology/1 and its stub worker.
  defp worker_spec(id, module \\ Pulsar.Consumer.Worker) do
    %{id: id, start: {Agent, :start_link, [fn -> id end]}, type: :worker, modules: [module]}
  end

  defp start_supervisor(children) do
    start_supervised!(%{
      id: {:sup, System.unique_integer([:positive])},
      type: :supervisor,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]}
    })
  end

  describe "workers/1" do
    test "collects the workers across every partition" do
      partitions =
        for index <- 0..2 do
          %{
            id: Pulsar.Topic.partition(@topic, index),
            type: :supervisor,
            start: {Supervisor, :start_link, [[worker_spec("w-#{index}")], [strategy: :one_for_one]]}
          }
        end

      assert length(Topology.workers(start_supervisor(partitions))) == 3
    end

    test "leaves out a child that is not one of the topology's workers" do
      root = start_supervisor([worker_spec("w-1"), worker_spec(Discovery, Discovery)])

      assert length(Topology.workers(root)) == 1
    end

    test "leaves out a worker that is not currently running" do
      root = start_supervisor([worker_spec("w-1")])

      :ok = Supervisor.terminate_child(root, "w-1")

      assert Topology.workers(root) == []
    end

    test "leaves out a supervisor that disappears during traversal" do
      child = %{
        id: :disappearing,
        start: {DisappearingSupervisor, :start_link, [[]]},
        restart: :temporary,
        type: :supervisor
      }

      assert Topology.workers(start_supervisor([child])) == []
    end
  end

  describe "groups/1" do
    test "pairs each index with the group for that partition" do
      {root, registry} = start_topology(12)
      groups = Topology.groups(root)

      assert length(groups) == 12
      assert [{^root, _value}] = Registry.lookup(registry, @name)

      for {index, pid} <- groups do
        assert [{_id, worker, :worker, _modules}] = Supervisor.which_children(pid)
        assert Agent.get(worker, & &1) == Pulsar.Topic.partition(@topic, index)
        assert Registry.lookup(registry, Pulsar.Topic.partition(@name, index)) == []
      end
    end

    test "reports a partition between lives without dropping it" do
      {root, _registry} = start_topology(3)
      :ok = Supervisor.terminate_child(root, {:partition, 1})

      assert List.keyfind(Topology.groups(root), 1, 0) == {1, :undefined}
    end

    test "answers a non-partitioned topic with its internal group" do
      {root, _registry} = start_topology(0)

      assert [{0, group}] = Topology.groups(root)
      assert group != root
      assert Topology.kind(root) == :topology
      assert Topology.kind(group) == :group
    end
  end
end
