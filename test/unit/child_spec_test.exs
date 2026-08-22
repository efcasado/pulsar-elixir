defmodule Pulsar.ChildSpecTest do
  use ExUnit.Case, async: true

  describe "Pulsar.Client.child_spec/1" do
    test "keys the id on the client's name so several clients can share a tree" do
      assert Pulsar.Client.child_spec(name: :analytics, host: "h").id == :analytics
      refute Pulsar.Client.child_spec(name: :events, host: "h").id == Pulsar.Client
    end

    test "starts two clients in one static supervision tree" do
      children = [
        {Pulsar.Client, name: :two_clients_a, host: "pulsar://127.0.0.1:6650"},
        {Pulsar.Client, name: :two_clients_b, host: "pulsar://127.0.0.1:6650"}
      ]

      supervisor =
        start_supervised!(%{
          id: :two_clients_tree,
          start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]},
          type: :supervisor
        })

      assert length(Supervisor.which_children(supervisor)) == 2
    end
  end

  describe "Pulsar.Consumer.child_spec/1" do
    test "is a supervisor spec keyed on the consumer's name" do
      spec =
        Pulsar.Consumer.child_spec(
          topic: "persistent://public/default/orders",
          subscription_name: "svc",
          callback_module: MyApp.Handler
        )

      assert spec.type == :supervisor
      assert spec.restart == :permanent
      assert spec.id == {Pulsar.Consumer, "persistent://public/default/orders-svc"}
    end

    test "gives two consumers on one topic distinct ids" do
      # A static supervision tree rejects duplicate ids, so the default cannot be the
      # module name.
      base = [topic: "t", callback_module: MyApp.Handler]

      one = Pulsar.Consumer.child_spec(base ++ [subscription_name: "a"])
      two = Pulsar.Consumer.child_spec(base ++ [subscription_name: "b"])

      refute one.id == two.id
    end

    test "prefers an explicit name for the id" do
      spec =
        Pulsar.Consumer.child_spec(
          topic: "t",
          subscription_name: "s",
          callback_module: MyApp.Handler,
          name: "orders-consumer"
        )

      assert spec.id == {Pulsar.Consumer, "orders-consumer"}
    end

    test "requires the topic, subscription and callback module" do
      for missing <- [:topic, :subscription_name, :callback_module] do
        opts =
          Keyword.delete(
            [topic: "t", subscription_name: "s", callback_module: MyApp.Handler],
            missing
          )

        assert_raise NimbleOptions.ValidationError, ~r/#{missing}/, fn ->
          Pulsar.Consumer.start_link(opts)
        end
      end
    end
  end

  describe "worker child specs" do
    test "consumer workers are transient while producer workers remain permanent" do
      assert Pulsar.Consumer.Worker.child_spec([]).restart == :transient
      assert Map.get(Pulsar.Producer.Worker.child_spec([]), :restart, :permanent) == :permanent
    end
  end

  describe "Pulsar.Producer.child_spec/1" do
    test "is a supervisor spec keyed on the producer's name" do
      spec = Pulsar.Producer.child_spec(topic: "persistent://public/default/audit")

      assert spec.type == :supervisor
      assert spec.restart == :permanent
      assert spec.id == {Pulsar.Producer, "persistent://public/default/audit-producer"}
    end

    test "prefers an explicit name for the id" do
      assert Pulsar.Producer.child_spec(topic: "t", name: :audit).id == {Pulsar.Producer, :audit}
    end

    test "requires the topic" do
      assert_raise NimbleOptions.ValidationError, ~r/:topic/, fn ->
        Pulsar.Producer.start_link(name: :audit)
      end
    end
  end
end
