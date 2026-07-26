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

  test "a client, consumer and producer start from a host supervision tree" do
    broker = System.broker()

    children = [
      {Pulsar.Client, name: :supervision_tree_client, host: broker.service_url},
      {Pulsar.Producer, topic: @topic, name: :supervision_tree_producer, client: :supervision_tree_client},
      {Pulsar.Consumer,
       topic: @topic,
       subscription_name: "supervision-tree-sub",
       callback_module: Handler,
       client: :supervision_tree_client,
       initial_position: :earliest,
       init_args: [self()]}
    ]

    supervisor =
      start_supervised!(%{
        id: :host_tree,
        start: {Supervisor, :start_link, [children, [strategy: :rest_for_one]]},
        type: :supervisor
      })

    assert [_client, _producer, _consumer] = Supervisor.which_children(supervisor)

    {:ok, _message_id} =
      Pulsar.Producer.send(:supervision_tree_producer, "from the tree", client: :supervision_tree_client)

    assert_receive {:received, "from the tree"}, 15_000
  end

  test "two consumers on one topic can be siblings" do
    # The child spec's id defaults to the consumer's name, so a static tree accepts
    # more than one consumer per topic.
    broker = System.broker()

    children = [
      {Pulsar.Client, name: :sibling_consumers_client, host: broker.service_url},
      {Pulsar.Consumer,
       topic: @topic,
       subscription_name: "sibling-a",
       callback_module: Handler,
       client: :sibling_consumers_client,
       init_args: [self()]},
      {Pulsar.Consumer,
       topic: @topic,
       subscription_name: "sibling-b",
       callback_module: Handler,
       client: :sibling_consumers_client,
       init_args: [self()]}
    ]

    supervisor =
      start_supervised!(%{
        id: :sibling_tree,
        start: {Supervisor, :start_link, [children, [strategy: :rest_for_one]]},
        type: :supervisor
      })

    assert length(Supervisor.which_children(supervisor)) == 3
  end
end
