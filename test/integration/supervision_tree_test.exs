defmodule Pulsar.Integration.SupervisionTreeTest do
  use ExUnit.Case, async: false

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

    children = [{Pulsar.Client, [name: client, host: broker.service_url] ++ opts}]

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

    assert [_producer] = DynamicSupervisor.which_children(Pulsar.Client.producer_supervisor(client))
    assert [_consumer] = DynamicSupervisor.which_children(Pulsar.Client.consumer_supervisor(client))

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

    [{_, before, _, _}] = DynamicSupervisor.which_children(Pulsar.Client.consumer_supervisor(client))

    ref = Process.monitor(Process.whereis(client))
    Process.exit(Process.whereis(client), :kill)
    assert_receive {:DOWN, ^ref, :process, _, :killed}, 5_000

    assert eventually(fn ->
             match?([{_, pid, _, _}] when is_pid(pid) and pid != before, consumer_children(client))
           end)

    assert [_client] = Supervisor.which_children(supervisor)
  end

  defp consumer_children(client) do
    DynamicSupervisor.which_children(Pulsar.Client.consumer_supervisor(client))
  catch
    :exit, _ -> []
  end

  defp eventually(fun, attempts \\ 100) do
    cond do
      fun.() -> true
      attempts == 0 -> false
      true -> Process.sleep(100) && eventually(fun, attempts - 1)
    end
  end
end
