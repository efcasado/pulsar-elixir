defmodule Pulsar.TopologyTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.Backoff
  alias Pulsar.Client
  alias Pulsar.Topology
  alias Pulsar.Topology.Controller
  alias Pulsar.Topology.Group
  alias Pulsar.Topology.Root

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
      opts = [
        topic: "persistent://public/default/t",
        name: "owner-root-#{System.unique_integer([:positive])}",
        client: :test,
        consumer_count: 1,
        partition_discovery_interval_ms: false
      ]

      {:ok, root} =
        Root.start_link(StubWorker, nil, :consumers, opts, resolver: fn _topic, _opts -> Process.sleep(:infinity) end)

      {:reply, root, state}
    end
  end

  describe "stop/1 when the first ancestor is not a supervisor" do
    # Started with start_link/1 from an ordinary process, $ancestors heads with the caller.
    # Asking it to terminate_child crashes it on an unmatched call.
    test "stops the resource without disturbing its caller" do
      {:ok, owner} = Owner.start_link(nil)
      resource = Owner.spawn_child(owner)
      ref = Process.monitor(owner)

      assert Topology.stop(resource) == :ok
      refute Process.alive?(resource)

      refute_receive {:DOWN, ^ref, :process, _pid, _reason}, 200
      assert Process.alive?(owner)
    end
  end

  describe "stop/1" do
    test "removes the resource from the supervisor that owns it" do
      owner = start_dynamic_supervisor()
      {:ok, resource} = DynamicSupervisor.start_child(owner, topology_spec(StubWorker))

      assert Topology.stop(resource) == :ok
      refute Process.alive?(resource)
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

  defmodule TransientWorker do
    @moduledoc false
    use Agent

    def child_spec(opts) do
      %{
        id: __MODULE__,
        start: {__MODULE__, :start_link, [opts]},
        restart: :transient,
        type: :worker
      }
    end

    def start_link(opts), do: Agent.start_link(fn -> Keyword.fetch!(opts, :topic) end)
  end

  defmodule PartitionFourFails do
    @moduledoc false
    use Agent

    def start_link(opts) do
      topic = Keyword.fetch!(opts, :topic)

      if String.ends_with?(topic, "-partition-4") do
        {:error, :partition_four_failed}
      else
        Agent.start_link(fn -> topic end)
      end
    end
  end

  defmodule CrashingWorker do
    @moduledoc false

    # Costs a round trip before failing, the way a real one does: failing instantly exhausts every
    # budget above it, taking milliseconds does not.
    def start_link(_opts), do: {:ok, spawn_link(fn -> Process.sleep(5) && exit(:crashed) end)}
  end

  defmodule OptsWorker do
    @moduledoc false
    use Agent

    def start_link(opts), do: Agent.start_link(fn -> opts end)
  end

  defmodule DisappearingSupervisor do
    @moduledoc false

    def start_link(reason) do
      pid =
        spawn_link(fn ->
          receive do
            _message -> exit(reason)
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

    :ok = Topology.await_ready(root, 1_000)

    {root, registry}
  end

  defp start_async_topology(resolver, opts \\ [], controller_opts \\ []) do
    {worker, controller_opts} = Keyword.pop(controller_opts, :worker, StubWorker)
    {kind, controller_opts} = Keyword.pop(controller_opts, :kind, :consumers)
    registry = :"registry-#{System.unique_integer([:positive])}"
    start_supervised!({Registry, keys: :unique, name: registry})

    topology_opts =
      Keyword.merge(
        [
          topic: @topic,
          name: @name,
          client: :test,
          partition_discovery_interval_ms: false
        ],
        opts
      )

    topology_opts =
      if kind == :consumers,
        do: Keyword.put_new(topology_opts, :consumer_count, 1),
        else: topology_opts

    root =
      start_supervised!(%{
        id: {:root, System.unique_integer([:positive])},
        start:
          {Root, :start_link, [worker, registry, kind, topology_opts, Keyword.put(controller_opts, :resolver, resolver)]},
        type: :supervisor
      })

    {root, registry}
  end

  describe "asynchronous initialization" do
    test "await_ready/2 waits for discovery and respects its timeout" do
      test_pid = self()

      resolver = fn _topic, _opts ->
        send(test_pid, {:resolution_started, self()})

        receive do
          :resolve -> {:ok, 2}
        end
      end

      {root, _registry} = start_async_topology(resolver)

      assert_receive {:resolution_started, discovery}
      assert Topology.await_ready(root, 25) == {:error, :timeout}

      waiter = Task.async(fn -> Topology.await_ready(root, 1_000) end)
      send(discovery, :resolve)

      assert Task.await(waiter) == :ok
      assert length(Topology.groups(root)) == 2
    end

    test "await_ready/2 rejects a stale pid" do
      root = spawn(fn -> :ok end)
      ref = Process.monitor(root)
      assert_receive {:DOWN, ^ref, :process, ^root, _reason}

      assert Topology.await_ready(root, 25) == {:error, :not_found}
    end

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

      :ok = Topology.await_ready(root, 1_000)
      assert length(Topology.groups(root)) == 2
    end

    test "retries failed initialization without blocking the topology" do
      attempts = start_supervised!({Agent, fn -> 0 end})

      resolver = fn _topic, _opts ->
        attempt = Agent.get_and_update(attempts, &{&1, &1 + 1})
        if attempt == 0, do: {:error, :no_broker_available}, else: {:ok, 3}
      end

      {root, _registry} = start_async_topology(resolver, consumer_count: 2)

      :ok = Topology.await_ready(root, 1_000)
      assert Agent.get(attempts, & &1) >= 2
      assert length(Topology.groups(root)) == 3
    end

    test "retries wrapped ServiceNotReady metadata failures" do
      attempts = start_supervised!({Agent, fn -> 0 end})

      resolver = fn _topic, _opts ->
        case Agent.get_and_update(attempts, &{&1, &1 + 1}) do
          0 -> {:error, {:partition_metadata_check_failed, :ServiceNotReady}}
          _later -> {:ok, 1}
        end
      end

      {root, _registry} = start_async_topology(resolver)

      :ok = Topology.await_ready(root, 1_000)
      assert Agent.get(attempts, & &1) >= 2
    end

    test "retries connection loss during metadata discovery" do
      attempts = start_supervised!({Agent, fn -> 0 end})

      resolver = fn _topic, _opts ->
        case Agent.get_and_update(attempts, &{&1, &1 + 1}) do
          0 -> {:error, :connection_lost}
          _later -> {:ok, 1}
        end
      end

      {root, _registry} = start_async_topology(resolver)

      :ok = Topology.await_ready(root, 1_000)
      assert Agent.get(attempts, & &1) >= 2
    end

    test "retries when the selected broker disappears during the metadata call" do
      attempts = start_supervised!({Agent, fn -> 0 end})

      resolver = fn _topic, _opts ->
        case Agent.get_and_update(attempts, &{&1, &1 + 1}) do
          0 -> exit({:noproc, {:gen_statem, :call, [self(), :metadata, 5_000]}})
          _later -> {:ok, 1}
        end
      end

      {root, _registry} = start_async_topology(resolver)

      :ok = Topology.await_ready(root, 1_000)
      assert Agent.get(attempts, & &1) >= 2
    end

    test "stops discovery on terminal metadata failures" do
      failures = [
        {fn _topic, _opts -> {:error, {:AuthorizationError, "denied"}} end, {:AuthorizationError, "denied"}},
        {fn _topic, _opts -> {:ok, :invalid} end, {:invalid_partition_count, :invalid}},
        {fn _topic, _opts -> raise "resolver bug" end, {:resolver_failed, :error, %RuntimeError{message: "resolver bug"}}}
      ]

      for {resolver, expected} <- failures do
        assert_controller_stops(resolver, expected)
      end
    end

    test "a false polling interval still performs initial discovery exactly once" do
      test_pid = self()

      resolver = fn _topic, _opts ->
        send(test_pid, :resolved)
        {:ok, 0}
      end

      {root, _registry} = start_async_topology(resolver)

      :ok = Topology.await_ready(root, 1_000)
      assert_receive :resolved
      refute_receive :resolved, 150
      assert [{0, group}] = Topology.groups(root)
      assert is_pid(group)
    end

    test "a producer topology starts one worker per partition" do
      {root, _registry} =
        start_async_topology(fn _topic, _opts -> {:ok, 3} end, [],
          worker: OptsWorker,
          kind: :producers
        )

      :ok = Topology.await_ready(root, 1_000)

      groups = Topology.groups(root)
      assert length(groups) == 3

      assert Enum.all?(groups, fn {_index, group} ->
               match?([{_id, pid, :worker, _modules}] when is_pid(pid), Supervisor.which_children(group))
             end)
    end

    @tag telemetry_listen: [[:pulsar, :topology, :discovery, :stop]]
    test "stops metadata polling for a non-partitioned topic" do
      test_pid = self()
      topic = "#{@topic}-non-partitioned-polling"

      resolver = fn resolved_topic, _opts ->
        send(test_pid, {:resolved, resolved_topic})
        {:ok, 0}
      end

      {root, _registry} =
        start_async_topology(
          resolver,
          topic: topic,
          name: "#{topic}-producer",
          partition_discovery_interval_ms: 10
        )

      :ok = Topology.await_ready(root, 1_000)
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

      refute_receive {:resolved, ^topic}, 300
    end

    @tag telemetry_listen: [[:pulsar, :topology, :reconciliation, :stop]]
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
      :ok = Topology.await_ready(root, 1_000)

      assert_receive {:telemetry_event,
                      %{
                        metadata: %{
                          desired_partition_count: 2,
                          partition_count: 2,
                          success: true
                        }
                      }}

      assert_receive {:resolved, 4}

      assert_receive {:telemetry_event,
                      %{
                        metadata: %{
                          desired_partition_count: 4,
                          partition_count: 4,
                          success: true
                        }
                      }}

      assert_receive {:resolved, 2}

      assert_receive {:telemetry_event,
                      %{
                        metadata: %{
                          desired_partition_count: 2,
                          partition_count: 4,
                          success: true
                        }
                      }}

      assert length(Topology.groups(root)) == 4
    end

    test "adds missing partitions from highest to lowest" do
      {root, _registry} = start_topology(4)

      opts = [
        topic: @topic,
        name: @name,
        client: :test,
        partition_discovery_interval_ms: false
      ]

      config = %{worker: PartitionFourFails, kind: :consumers, worker_count: 1, opts: opts}

      assert {:error, {:partition_start_failed, 4, _reason}} =
               Root.reconcile(root, 6, config)

      assert root
             |> Topology.groups()
             |> Enum.map(&elem(&1, 0))
             |> Enum.sort() == [0, 1, 2, 3, 5]
    end

    test "facade operations do not wait for an in-flight metadata poll" do
      test_pid = self()
      resolutions = start_supervised!({Agent, fn -> 0 end})

      resolver = fn _topic, _opts ->
        case Agent.get_and_update(resolutions, &{&1, &1 + 1}) do
          0 ->
            {:ok, 1}

          _later ->
            send(test_pid, {:resolution_started, self()})

            receive do
              :resolve -> {:ok, 1}
            end
        end
      end

      {root, _registry} = start_async_topology(resolver, partition_discovery_interval_ms: 10)

      :ok = Topology.await_ready(root, 1_000)
      assert_receive {:resolution_started, discovery}, 1_000

      operations =
        Task.async(fn ->
          {
            Pulsar.Producer.send(root, "payload"),
            Pulsar.Consumer.send_flow(root, 1),
            Pulsar.Consumer.topic(root)
          }
        end)

      try do
        assert Task.yield(operations, 500) ==
                 {:ok,
                  {
                    {:error, :not_ready},
                    {:error, :no_consumers_available},
                    @topic
                  }}
      after
        send(discovery, :resolve)
      end
    end

    # Regression for #198: a partition whose spec is present but not running is accounted for,
    # and reconciliation has to leave it alone rather than start it over on the next poll.
    test "leaves a partition that is down where it is, while its siblings keep running" do
      {root, _registry} = start_topology(2)

      groups = Map.new(Topology.groups(root))
      stopped = Map.fetch!(groups, 0)
      surviving = Map.fetch!(groups, 1)

      ref = Process.monitor(stopped)
      :ok = Supervisor.terminate_child(root, {:partition, 0})
      assert_receive {:DOWN, ^ref, :process, ^stopped, _reason}

      discovery = discovery(root)
      for _poll <- 1..3, do: send(discovery, :discover)
      assert GenServer.call(discovery, :status) == {:ready, {:partitioned, 2}}

      assert Process.alive?(root)
      assert Map.new(Topology.groups(root)) == %{0 => :undefined, 1 => surviving}
    end

    test "stopping a topology takes its groups and workers with it" do
      {root, _registry} = start_async_topology(fn _topic, _opts -> {:ok, 2} end, partition_discovery_interval_ms: 10)

      :ok = Topology.await_ready(root, 1_000)

      below =
        for {_index, group} <- Topology.groups(root),
            {_id, worker, :worker, _modules} <- Supervisor.which_children(group),
            do: worker

      refs = Map.new([root | below], &{Process.monitor(&1), &1})

      assert Topology.stop(root) == :ok

      for {ref, pid} <- refs, do: assert_receive({:DOWN, ^ref, :process, ^pid, _reason}, 1_000)
    end
  end

  describe "propagation" do
    test "restarts a worker that crashes, leaving its group and root alone" do
      {root, _registry} = start_topology(0)

      [{0, group}] = Topology.groups(root)
      [{_id, worker, :worker, _modules}] = Supervisor.which_children(group)

      worker_ref = Process.monitor(worker)
      group_ref = Process.monitor(group)
      root_ref = Process.monitor(root)

      Process.exit(worker, :kill)
      assert_receive {:DOWN, ^worker_ref, :process, ^worker, :killed}

      refute_receive {:DOWN, ^group_ref, :process, _pid, _reason}, 200
      refute_receive {:DOWN, ^root_ref, :process, _pid, _reason}, 0

      assert [{_id, restarted, :worker, _modules}] = Supervisor.which_children(group)
      assert restarted != worker
      assert Topology.groups(root) == [{0, group}]
    end

    test "stops a worker and leaves its group running while a sibling remains" do
      {root, _registry} = start_async_topology(fn _topic, _opts -> {:ok, 0} end, consumer_count: 2)
      :ok = Topology.await_ready(root, 1_000)

      [{0, group}] = Topology.groups(root)
      [{_id, worker, :worker, _modules} | _rest] = Supervisor.which_children(group)

      ref = Process.monitor(worker)
      assert Topology.stop(worker) == :ok
      assert_receive {:DOWN, ^ref, :process, ^worker, _reason}, 1_000

      assert Process.alive?(group)
      assert Process.alive?(root)

      assert Enum.count(Supervisor.which_children(group), &match?({_id, :undefined, _type, _modules}, &1)) == 1
    end

    test "a transient worker that finishes normally stays absent under permanent boundaries" do
      {root, _registry} =
        start_async_topology(fn _topic, _opts -> {:ok, 0} end, [], worker: TransientWorker)

      :ok = Topology.await_ready(root, 1_000)
      [{0, group}] = Topology.groups(root)
      [{id, worker, :worker, _modules}] = Supervisor.which_children(group)

      ref = Process.monitor(worker)
      :ok = Agent.stop(worker, :normal)
      assert_receive {:DOWN, ^ref, :process, ^worker, :normal}

      assert Process.alive?(group)
      assert Process.alive?(root)
      assert [{^id, :undefined, :worker, _modules}] = Supervisor.which_children(group)
    end

    test "a group that goes down leaves its siblings and its root alone" do
      {root, _registry} = start_topology(2)

      groups = Map.new(Topology.groups(root))
      stopping = Map.fetch!(groups, 0)
      surviving = Map.fetch!(groups, 1)

      ref = Process.monitor(stopping)
      :ok = Supervisor.terminate_child(root, {:partition, 0})
      assert_receive {:DOWN, ^ref, :process, ^stopping, _reason}, 1_000

      assert Process.alive?(root)
      assert Map.new(Topology.groups(root)) == %{0 => :undefined, 1 => surviving}
    end

    test "leaves nothing behind once the resource has been stopped" do
      client = start_dynamic_supervisor()
      {:ok, root} = DynamicSupervisor.start_child(client, topology_spec(StubWorker))

      :ok = Topology.await_ready(root, 1_000)
      [{0, group}] = Topology.groups(root)
      [{_id, worker, :worker, _modules}] = Supervisor.which_children(group)
      discovery = discovery(root)

      refs = Map.new([root, group, worker, discovery], &{Process.monitor(&1), &1})

      assert Topology.stop(root) == :ok

      for {ref, pid} <- refs, do: assert_receive({:DOWN, ^ref, :process, ^pid, _reason}, 1_000)

      assert DynamicSupervisor.which_children(client) == []
    end

    test "stops a producer topology the same way" do
      {root, _registry} = start_async_topology(fn _topic, _opts -> {:ok, 2} end, [], kind: :producers)

      :ok = Topology.await_ready(root, 1_000)
      root_ref = Process.monitor(root)

      assert Topology.stop(root) == :ok

      assert_receive {:DOWN, ^root_ref, :process, ^root, _reason}, 1_000
    end

    test "a broker that is away cannot spend a group's restart budget, whatever its worker count" do
      window_ms = Keyword.fetch!(Client.restart_intensity(:no_such_client, :worker), :max_seconds) * 1_000

      {paced_ms, {:error, :no_broker_available}} =
        :timer.tc(fn -> Backoff.run(fn -> {:error, :no_broker_available} end) end, :millisecond)

      for count <- [1, 10] do
        budget = Group.restart_intensity(:no_such_client, count)

        assert count * div(window_ms, paced_ms) < Keyword.fetch!(budget, :max_restarts)
      end
    end

    test "a resource that cannot stay up escalates to its client instead of disappearing" do
      Process.flag(:trap_exit, true)
      {:ok, client} = DynamicSupervisor.start_link(strategy: :one_for_one, max_restarts: 1, max_seconds: 5)
      ref = Process.monitor(client)

      {:ok, _root} = DynamicSupervisor.start_child(client, topology_spec(CrashingWorker))

      # Escalation must not depend on how fast the failure comes back, hence the round trip above.
      assert_receive {:DOWN, ^ref, :process, ^client, :shutdown}, 30_000
    end
  end

  describe "worker options" do
    test "gives a partition worker its own topic alongside the configured one" do
      {root, _registry} = start_async_topology(fn _topic, _opts -> {:ok, 3} end, [], worker: OptsWorker)
      :ok = Topology.await_ready(root, 1_000)

      for {index, opts} <- worker_opts(root) do
        assert Keyword.fetch!(opts, :topic) == Pulsar.Topic.partition(@topic, index)
        assert Keyword.fetch!(opts, :base_topic) == @topic
        assert Keyword.fetch!(opts, :partition) == index
      end
    end

    test "leaves a non-partitioned worker on the configured topic, with no partition" do
      {root, _registry} = start_async_topology(fn _topic, _opts -> {:ok, 0} end, [], worker: OptsWorker)
      :ok = Topology.await_ready(root, 1_000)

      assert [{0, opts}] = worker_opts(root)
      assert Keyword.fetch!(opts, :topic) == @topic
      assert Keyword.fetch!(opts, :base_topic) == @topic
      assert Keyword.fetch!(opts, :partition) == nil
    end
  end

  describe "companions" do
    test "runs what a resource attaches and tells its workers what it answered" do
      test_pid = self()

      attach = fn opts, root ->
        send(test_pid, {:attached, root})

        {Keyword.put(opts, :companion_root, root),
         [%{id: :companion, start: {Agent, :start_link, [fn -> :companion end]}}]}
      end

      {root, _registry} =
        start_async_topology(fn _topic, _opts -> {:ok, 0} end, [companions: attach], worker: OptsWorker)

      :ok = Topology.await_ready(root, 1_000)

      assert_receive {:attached, ^root}

      assert {:companion, companion, :worker, _modules} =
               root |> Supervisor.which_children() |> List.keyfind(:companion, 0)

      assert is_pid(companion)

      assert [{0, opts}] = worker_opts(root)
      assert Keyword.fetch!(opts, :companion_root) == root
    end

    # It configures the root, so a worker is started without it either way.
    test "keeps the declaration out of the options its workers are started with" do
      attach = fn opts, _root -> {opts, []} end

      for declared <- [[companions: attach], []] do
        {root, _registry} =
          start_async_topology(fn _topic, _opts -> {:ok, 0} end, declared, worker: OptsWorker)

        :ok = Topology.await_ready(root, 1_000)

        assert [{0, opts}] = worker_opts(root)
        refute Keyword.has_key?(opts, :companions)
      end
    end
  end

  defp discovery(root) do
    [pid] = for {{Controller, _kind, _topic, _scheme}, pid, _type, _modules} <- Supervisor.which_children(root), do: pid
    pid
  end

  defp assert_controller_stops(resolver, expected) do
    opts = [topic: @topic, client: :test, partition_discovery_interval_ms: false]
    config = %{worker: StubWorker, kind: :consumers, worker_count: 1, opts: opts}

    assert {:ok, controller} = GenServer.start(Controller, {self(), config, [resolver: resolver]})
    ref = Process.monitor(controller)
    assert_receive {:DOWN, ^ref, :process, ^controller, ^expected}, 1_000
  end

  defp topology_spec(worker) do
    registry = :"registry-#{System.unique_integer([:positive])}"
    start_supervised!({Registry, keys: :unique, name: registry})

    opts = [topic: @topic, name: @name, client: :test, partition_discovery_interval_ms: false, consumer_count: 1]
    controller_opts = [resolver: fn _topic, _opts -> {:ok, 0} end]

    %{
      id: {:root, System.unique_integer([:positive])},
      start: {Root, :start_link, [worker, registry, :consumers, opts, controller_opts]},
      restart: :permanent,
      type: :supervisor
    }
  end

  defp worker_opts(root) do
    for {index, group} <- Topology.groups(root) do
      [{_id, worker, :worker, _modules}] = Supervisor.which_children(group)
      {index, Agent.get(worker, & &1)}
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
      root = start_supervisor([worker_spec("w-1"), worker_spec(Controller, Controller)])

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
        start: {DisappearingSupervisor, :start_link, [:shutdown]},
        restart: :temporary,
        type: :supervisor
      }

      assert Topology.workers(start_supervisor([child])) == []
    end

    test "surfaces an unexpected supervisor exit during traversal" do
      child = %{
        id: :crashing,
        start: {DisappearingSupervisor, :start_link, [:unexpected]},
        restart: :temporary,
        type: :supervisor
      }

      assert catch_exit(Topology.workers(start_supervisor([child])))
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
      assert Topology.kind(root) == :root
      assert Topology.kind(group) == :group
    end

    test "answers workers and stale pids with no groups" do
      {root, _registry} = start_topology(0)
      [{0, group}] = Topology.groups(root)
      [{_id, worker, :worker, _modules}] = Supervisor.which_children(group)

      assert Topology.groups(worker) == []

      stale = spawn(fn -> :ok end)
      ref = Process.monitor(stale)
      assert_receive {:DOWN, ^ref, :process, ^stale, _reason}

      assert Topology.kind(stale) == :worker
      assert Topology.groups(stale) == []
    end
  end
end
