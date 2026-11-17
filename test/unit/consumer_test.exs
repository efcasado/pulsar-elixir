defmodule Pulsar.ConsumerTest do
  use ExUnit.Case, async: true

  alias Pulsar.Client
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

    test "rejects an internal group pid" do
      assert Consumer.topic(group_pid()) == {:error, :not_found}
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

    test "rejects an internal group pid" do
      assert Consumer.send_flow(group_pid(), 1) == {:error, :not_found}
    end
  end

  describe "ack/3" do
    test "reports a missing name consistently with send_flow/3" do
      opts = [client: :consumer_missing_client]

      assert Consumer.ack(:missing, message_id(), opts) == {:error, :not_found}
    end

    test "delivers through the one live worker a name resolves to" do
      {client, name} = start_ack_topology(groups: 1, workers: 1)
      message_id = message_id()

      assert Consumer.ack(name, message_id, client: client) == :ok
      assert_received {:ack_test_worker, {:ack, [^message_id]}}
    end

    test "delivers through the one live worker behind a root pid" do
      {client, name} = start_ack_topology(groups: 1, workers: 1)
      message_id = message_id()
      {:ok, root} = Client.lookup(:consumers, name, client)

      assert Consumer.ack(root, message_id) == :ok
      assert_received {:ack_test_worker, {:ack, [^message_id]}}
    end

    test "reports several live workers behind one group as ambiguous" do
      {client, name} = start_ack_topology(groups: 1, workers: 2)

      assert Consumer.ack(name, message_id(), client: client) == {:error, :ambiguous_worker}
    end

    test "reports several groups as ambiguous" do
      {client, name} = start_ack_topology(groups: 2, workers: 1)

      assert Consumer.ack(name, message_id(), client: client) == {:error, :ambiguous_worker}
    end

    test "reports a group whose worker is between lives as without consumers" do
      {client, name} = start_ack_topology(groups: 1, workers: 0)

      assert Consumer.ack(name, message_id(), client: client) ==
               {:error, :no_consumers_available}
    end

    test "reports a consumer that has not discovered its groups as not ready" do
      {client, name} = start_ack_topology(groups: 0, workers: 1)

      assert Consumer.ack(name, message_id(), client: client) == {:error, :not_ready}
    end

    test "returns an error for a stale worker pid" do
      assert {:error, _reason} = Consumer.ack(dead_pid(), message_id())
    end

    test "rejects an internal group pid" do
      assert Consumer.ack(group_pid(), message_id()) == {:error, :not_found}
    end
  end

  describe "nack/3" do
    test "reports a missing name consistently with send_flow/3" do
      opts = [client: :consumer_missing_client]

      assert Consumer.nack(:missing, message_id(), opts) == {:error, :not_found}
    end

    test "delivers through the one live worker a name resolves to" do
      {client, name} = start_ack_topology(groups: 1, workers: 1)
      message_id = message_id()

      assert Consumer.nack(name, message_id, client: client) == :ok
      assert_received {:ack_test_worker, {:nack, [^message_id]}}
    end

    test "reports several live workers behind one group as ambiguous" do
      {client, name} = start_ack_topology(groups: 1, workers: 2)

      assert Consumer.nack(name, message_id(), client: client) == {:error, :ambiguous_worker}
    end

    test "returns an error for a stale worker pid" do
      assert {:error, _reason} = Consumer.nack(dead_pid(), message_id())
    end

    test "rejects an internal group pid" do
      assert Consumer.nack(group_pid(), message_id()) == {:error, :not_found}
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

  defp message_id, do: %{ledgerId: 1, entryId: 2}

  # A stand-in topology root: registered under a name in a client-shaped registry, reporting
  # a group per :groups with the requested worker count behind it. Workers capture what they
  # are asked so tests can watch the call go through the facade.
  defmodule AckTestRoot do
    @moduledoc false
    use Supervisor

    def start_link(config), do: Supervisor.start_link(__MODULE__, config)

    @impl true
    def init(config) do
      Process.put(:"$initial_call", {:supervisor, Pulsar.Topology.Root, 1})
      Registry.register(config.registry, config.name, nil)

      group_spec = fn index ->
        workers =
          for rank <- 1..max(config.workers, 0)//1 do
            %{
              id: {:worker, rank},
              start: {Pulsar.ConsumerTest.AckTestWorker, :start_link, [config.report_to]},
              modules: [Pulsar.Consumer.Worker]
            }
          end

        %{
          id: {:partition, index},
          start: {Supervisor, :start_link, [workers, [strategy: :one_for_all]]},
          type: :supervisor
        }
      end

      children =
        if config.groups == 0 do
          []
        else
          Enum.map(0..(config.groups - 1), group_spec)
        end

      Supervisor.init(children, strategy: :one_for_all)
    end
  end

  defmodule AckTestWorker do
    @moduledoc false
    use GenServer

    def start_link(report_to), do: GenServer.start_link(__MODULE__, report_to)

    @impl true
    def init(report_to), do: {:ok, report_to}

    @impl true
    def handle_call(call, _from, report_to) do
      send(report_to, {:ack_test_worker, call})
      {:reply, :ok, report_to}
    end
  end

  defp start_ack_topology(groups: groups, workers: workers) do
    client = :consumer_ack_client
    name = :ack_consumer
    registry = Client.registry(:consumers, client)
    start_supervised!({Registry, keys: :unique, name: registry})

    config = %{registry: registry, name: name, report_to: self(), groups: groups, workers: workers}

    start_supervised!(%{
      id: {:ack_test_root, System.unique_integer([:positive])},
      start: {AckTestRoot, :start_link, [config]},
      type: :supervisor
    })

    {client, name}
  end

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
end
