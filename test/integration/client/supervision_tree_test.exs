defmodule Pulsar.Integration.Client.SupervisionTreeTest do
  use ExUnit.Case, async: false

  alias Pulsar.Client
  alias Pulsar.Test.Support.System

  @moduletag :integration

  @topic "persistent://public/default/supervision-tree-test"

  defmodule Handler do
    @moduledoc false
    use Pulsar.Consumer.Callback

    @impl true
    def init([test_pid]), do: {:ok, test_pid}

    @impl true
    def handle_message(message, test_pid) do
      send(test_pid, {:received, message.payload})
      {:ok, test_pid}
    end
  end

  defp host_tree(client, opts) do
    broker = System.broker()

    children = [{Client, [name: client, host: broker.service_url] ++ opts}]

    start_supervised!(%{
      id: :host_tree,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]},
      type: :supervisor
    })
  end

  test "a client starts the consumers and producers declared on it" do
    client = :supervision_tree_client

    supervisor =
      host_tree(client,
        producers: [[topic: @topic, name: :supervision_tree_producer]],
        consumers: [
          [
            topic: @topic,
            subscription_name: "supervision-tree-sub",
            callback_module: Handler,
            initial_position: :earliest,
            init_args: [self()]
          ]
        ]
      )

    assert [_client] = Supervisor.which_children(supervisor)

    # Declared resources are started off the client's boot, so the client is up before they are.
    assert eventually(fn -> match?([_producer], Client.producers(client)) end)
    assert eventually(fn -> match?([_consumer], Client.consumers(client)) end)

    {:ok, _message_id} = Pulsar.Producer.send(:supervision_tree_producer, "from the tree", client: client)

    assert_receive {:received, "from the tree"}, 15_000
  end

  test "declared resources come back when the client restarts" do
    client = :supervision_tree_restart_client

    supervisor =
      host_tree(client,
        consumers: [
          [
            topic: @topic,
            subscription_name: "supervision-tree-restart-sub",
            callback_module: Handler,
            initial_position: :earliest,
            init_args: [self()]
          ]
        ]
      )

    assert eventually(fn -> match?([_consumer], Client.consumers(client)) end)
    [before] = Client.consumers(client)

    # Not Process.exit/2: a supervisor traps exits, so an abnormal signal is ignored and
    # :kill skips the shutdown that releases its children's registered names.
    client_pid = Process.whereis(client)
    ref = Process.monitor(client_pid)
    Supervisor.stop(client, :shutdown)
    assert_receive {:DOWN, ^ref, :process, _, :shutdown}, 5_000

    assert eventually(fn ->
             match?([pid] when is_pid(pid) and pid != before, Client.consumers(client))
           end)

    assert [_client] = Supervisor.which_children(supervisor)
  end

  test "a consumer-side failure leaves producers running" do
    client = :supervision_tree_isolation_client

    supervisor = host_tree(client, [])

    # A runtime producer, not a declared one: Bootstrap recreates declared resources, so only
    # this kind shows whether the branches are genuinely independent.
    {:ok, _producer} = Pulsar.Producer.start(topic: @topic, name: :isolation_producer, client: client)

    assert eventually(fn -> match?([_producer], Client.producers(client)) end)
    assert {:ok, _message_id} = Pulsar.Producer.send(:isolation_producer, "before", client: client)

    # The whole consumer branch, as when its children exhaust their restart intensity —
    # a failure inside the branch is contained by the branch and never reaches the producers.
    branch = branch(client, :consumers)
    ref = Process.monitor(branch)
    Supervisor.stop(branch, :shutdown)
    assert_receive {:DOWN, ^ref, :process, _pid, :shutdown}, 5_000

    assert eventually(fn -> match?([_producer], Client.producers(client)) end)
    assert {:ok, _message_id} = Pulsar.Producer.send(:isolation_producer, "after", client: client)

    assert [_client] = Supervisor.which_children(supervisor)
  end

  defp branch(client, kind) do
    resources = child_pid(Process.whereis(client), :resources)
    child_pid(resources, kind)
  end

  defp child_pid(supervisor, id) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.find_value(fn {child_id, pid, _type, _modules} -> child_id == id && pid end)
  end

  defp eventually(fun, attempts \\ 100) do
    cond do
      fun.() -> true
      attempts == 0 -> false
      true -> Process.sleep(100) && eventually(fun, attempts - 1)
    end
  end
end
