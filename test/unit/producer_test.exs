defmodule Pulsar.ProducerTest do
  use ExUnit.Case, async: true

  alias Pulsar.Producer
  alias Pulsar.Topology
  alias Pulsar.Topology.Group

  defmodule RoutingWorker do
    @moduledoc false
    use GenServer

    def start_link(partition), do: GenServer.start_link(__MODULE__, partition)

    @impl true
    def init(partition), do: {:ok, partition}

    @impl true
    def handle_call({:send_message, _message, _opts}, _from, partition) do
      {:reply, {:ok, partition}, partition}
    end
  end

  describe "await_ready/2" do
    test "reports a missing named producer after the wait" do
      assert Producer.await_ready(:missing, client: :producer_missing_client, timeout: 0) ==
               {:error, :not_found}
    end

    test "reports a stale producer pid" do
      producer = spawn(fn -> :ok end)
      ref = Process.monitor(producer)
      assert_receive {:DOWN, ^ref, :process, ^producer, _reason}

      assert Producer.await_ready(producer, timeout: 25) == {:error, :not_found}
    end
  end

  describe "send/3" do
    test "accepts atom and string names" do
      opts = [client: :producer_send_missing_client]

      assert Producer.send(:missing, "payload", opts) == {:error, :not_found}
      assert Producer.send("missing", "payload", opts) == {:error, :not_found}
      assert Producer.stop(:missing, opts) == {:error, :not_found}
    end

    test "rejects unsupported target types" do
      assert_raise FunctionClauseError, fn -> send_message(Producer, 42, "payload") end
    end

    test "rejects non-binary payloads for pids and names" do
      for producer <- [self(), :producer] do
        assert_raise FunctionClauseError, fn -> send_message(Producer, producer, %{value: 1}) end
      end
    end

    test "rejects an internal group pid" do
      assert Producer.send(group_pid(), "payload") == {:error, :not_found}
    end
  end

  describe "stop/2" do
    test "rejects a worker pid without stopping it" do
      worker = start_supervised!({Agent, fn -> :worker end})

      assert Producer.stop(worker) == {:error, :not_found}
      assert Process.alive?(worker)
    end
  end

  describe "partition routing" do
    test "keeps the old modulus until a growing topology is contiguous" do
      root = start_routing_topology()

      for index <- [0, 1, 2, 3, 5] do
        assert {:ok, _group} = Supervisor.start_child(root, routing_group_spec(index))
      end

      partition_key = key_for_partition(4, 5)

      assert Producer.send(root, "payload", partition_key: partition_key) ==
               {:ok, :erlang.phash2(partition_key, 4)}

      assert {:ok, _group} = Supervisor.start_child(root, routing_group_spec(4))

      assert Producer.send(root, "payload", partition_key: partition_key) ==
               {:ok, :erlang.phash2(partition_key, 6)}
    end
  end

  defp send_message(module, producer, message), do: module.send(producer, message, [])

  defp group_pid do
    caller = self()

    pid =
      spawn(fn ->
        caller_ref = Process.monitor(caller)
        Process.put(:"$initial_call", {:supervisor, Group, 1})
        send(caller, {:ready, self()})

        receive do
          {:DOWN, ^caller_ref, :process, ^caller, _reason} -> :ok
        end
      end)

    assert_receive {:ready, ^pid}
    pid
  end

  defp start_routing_topology do
    test_pid = self()
    registry = :"producer-routing-registry-#{System.unique_integer([:positive])}"
    start_supervised!({Registry, keys: :unique, name: registry})

    resolver = fn _topic, _opts ->
      send(test_pid, :routing_resolution_started)

      receive do
        :finish_routing_resolution -> {:ok, 0}
      end
    end

    opts = [
      topic: "persistent://public/default/routing",
      name: "routing-producer",
      client: :test,
      producer_count: 1,
      partition_discovery_interval_ms: false
    ]

    root =
      start_supervised!(%{
        id: {:routing_topology, System.unique_integer([:positive])},
        start: {Topology, :start_link, [RoutingWorker, registry, :producer_count, opts, [resolver: resolver]]},
        type: :supervisor
      })

    assert_receive :routing_resolution_started
    root
  end

  defp routing_group_spec(index) do
    worker = %{
      id: {:routing_worker, index},
      start: {RoutingWorker, :start_link, [index]},
      type: :worker,
      modules: [Pulsar.Producer.Worker]
    }

    %{
      id: {:partition, index},
      start: {Supervisor, :start_link, [[worker], [strategy: :one_for_one]]},
      restart: :transient,
      type: :supervisor
    }
  end

  defp key_for_partition(partition, partitions) do
    0
    |> Stream.iterate(&(&1 + 1))
    |> Enum.find_value(fn candidate ->
      key = "key-#{candidate}"
      if :erlang.phash2(key, partitions) == partition, do: key
    end)
  end
end
