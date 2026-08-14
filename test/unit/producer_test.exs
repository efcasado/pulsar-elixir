defmodule Pulsar.ProducerTest do
  use ExUnit.Case, async: true

  alias Pulsar.Hash
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
    def handle_cast({:send_message, _message, _opts, from}, partition) do
      GenServer.reply(from, {:ok, partition})
      {:noreply, partition}
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

      partition_key = key_for_partition(4, 5, :murmur3_32)

      assert Producer.send(root, "payload", partition_key: partition_key) ==
               {:ok, partition_for(partition_key, 4, :murmur3_32)}

      assert {:ok, _group} = Supervisor.start_child(root, routing_group_spec(4))

      assert Producer.send(root, "payload", partition_key: partition_key) ==
               {:ok, partition_for(partition_key, 6, :murmur3_32)}
    end

    test "routes a key under murmur3_32 by default" do
      root = start_routing_topology()
      start_partitions(root, 8)

      for candidate <- 0..20 do
        key = "key-#{candidate}"

        assert Producer.send(root, "payload", partition_key: key) ==
                 {:ok, partition_for(key, 8, :murmur3_32)}
      end
    end

    test "routes a key under the configured hashing scheme" do
      root = start_routing_topology(hashing_scheme: :java_string_hash)
      start_partitions(root, 8)

      for candidate <- 0..20 do
        key = "key-#{candidate}"

        assert Producer.send(root, "payload", partition_key: key) ==
                 {:ok, partition_for(key, 8, :java_string_hash)}
      end
    end

    test "routes a key exactly as 2.x did under :phash2_legacy" do
      root = start_routing_topology(hashing_scheme: :phash2_legacy)
      start_partitions(root, 8)

      for candidate <- 0..20 do
        key = "key-#{candidate}"

        assert Producer.send(root, "payload", partition_key: key) ==
                 {:ok, :erlang.phash2(key, 8)}
      end
    end

    test "raises on a non-binary key rather than reporting it as a dead producer" do
      root = start_routing_topology()
      start_partitions(root, 8)

      assert_raise ArgumentError, ~r/:partition_key must be a binary/, fn ->
        Producer.send(root, "payload", partition_key: :tenant_a)
      end
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

  defp start_routing_topology(extra_opts \\ []) do
    test_pid = self()
    registry = :"producer-routing-registry-#{System.unique_integer([:positive])}"
    start_supervised!({Registry, keys: :unique, name: registry})

    resolver = fn _topic, _opts ->
      send(test_pid, :routing_resolution_started)

      receive do
        :finish_routing_resolution -> {:ok, 0}
      end
    end

    opts =
      Keyword.merge(
        [
          topic: "persistent://public/default/routing",
          name: "routing-producer",
          client: :test,
          producer_count: 1,
          partition_discovery_interval_ms: false
        ],
        extra_opts
      )

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

  defp start_partitions(root, count) do
    for index <- 0..(count - 1) do
      assert {:ok, _group} = Supervisor.start_child(root, routing_group_spec(index))
    end
  end

  defp partition_for(key, partitions, scheme), do: Hash.partition(scheme, key, partitions)

  defp key_for_partition(partition, partitions, scheme) do
    0
    |> Stream.iterate(&(&1 + 1))
    |> Enum.find_value(fn candidate ->
      key = "key-#{candidate}"
      if partition_for(key, partitions, scheme) == partition, do: key
    end)
  end
end
