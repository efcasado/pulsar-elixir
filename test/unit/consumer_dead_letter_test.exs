defmodule Pulsar.Consumer.DeadLetterTest do
  use ExUnit.Case, async: true

  alias Pulsar.Client
  alias Pulsar.Consumer.DeadLetter
  alias Pulsar.Consumer.Worker, as: ConsumerWorker
  alias Pulsar.Topology

  @topic "persistent://public/default/orders"
  @subscription "billing"
  @consumer_name "#{@topic}-#{@subscription}"
  @default_dlq "#{@topic}-#{@subscription}-DLQ"

  defp opts(overrides \\ []) do
    Keyword.merge(
      [
        topic: @topic,
        name: @consumer_name,
        client: :test,
        subscription_name: @subscription,
        consumer_count: 1,
        partition_discovery_interval_ms: false,
        dead_letter_policy: [max_redelivery: 3]
      ],
      overrides
    )
  end

  describe "child_specs/1" do
    test "a consumer without a dead letter policy owns no producer" do
      assert DeadLetter.child_specs(opts(dead_letter_policy: nil)) == []
    end

    test "defaults the topic to the base topic and subscription" do
      assert [%{id: {:dead_letter, @default_dlq}} = spec] = DeadLetter.child_specs(opts())
      assert {Pulsar.Producer, :start_link, [producer_opts]} = spec.start
      assert Keyword.fetch!(producer_opts, :topic) == @default_dlq
    end

    test "honours an explicit dead letter topic" do
      explicit = "persistent://public/default/parked"
      specs = DeadLetter.child_specs(opts(dead_letter_policy: [max_redelivery: 3, topic: explicit]))

      assert [%{id: {:dead_letter, ^explicit}}] = specs
    end

    # Two subscriptions may be configured to divert into the same topic, and each still needs
    # its own registered producer.
    test "names the producer after the consumer rather than the dead letter topic" do
      explicit = [max_redelivery: 3, topic: "persistent://public/default/parked"]

      names =
        for name <- [@consumer_name, "#{@consumer_name}-other"] do
          [%{start: {_module, _fun, [producer_opts]}}] =
            DeadLetter.child_specs(opts(name: name, dead_letter_policy: explicit))

          Keyword.fetch!(producer_opts, :name)
        end

      assert names == Enum.uniq(names)
    end
  end

  describe "annotate/2" do
    test "tells the workers which root owns their dead letter producer" do
      root = self()

      assert opts() |> DeadLetter.annotate(root) |> Keyword.fetch!(:dead_letter_root) == root
    end

    test "leaves a consumer without a dead letter policy alone" do
      annotated = [dead_letter_policy: nil] |> opts() |> DeadLetter.annotate(self())

      refute Keyword.has_key?(annotated, :dead_letter_root)
    end
  end

  describe "under a consumer topology" do
    setup do
      start_supervised!({Registry, keys: :unique, name: Client.registry(:producers, :test)})
      start_supervised!({Registry, keys: :unique, name: Client.registry(:consumers, :test)})

      :ok
    end

    test "runs the dead letter producer as a child of the consumer it belongs to" do
      root = start_consumer_topology()

      assert {_id, dlq, :supervisor, _modules} =
               root
               |> Supervisor.which_children()
               |> List.keyfind({:dead_letter, @default_dlq}, 0)

      assert is_pid(dlq)
      assert Topology.kind(dlq) == :topology
      assert Topology.resource?(dlq, :producers)
    end

    test "the dead letter producer comes up without waiting on a broker" do
      root = start_consumer_topology()

      # The consumer's own discovery never resolves here, so this also shows the producer is
      # not sequenced behind it.
      assert {_id, dlq, :supervisor, _modules} =
               root |> Supervisor.which_children() |> List.keyfind({:dead_letter, @default_dlq}, 0)

      assert Client.lookup(:producers, "#{@consumer_name}-dead-letter-producer", :test) == {:ok, dlq}
    end

    test "a consumer without a dead letter policy has no such child" do
      root = start_consumer_topology(dead_letter_policy: nil)

      assert root |> Supervisor.which_children() |> List.keyfind({:dead_letter, @default_dlq}, 0) == nil
    end

    # Discovery never resolves, so no consumer worker is started and none of this needs a broker.
    defp start_consumer_topology(overrides \\ []) do
      registry = :"consumer-topology-#{System.unique_integer([:positive])}"
      start_supervised!({Registry, keys: :unique, name: registry})

      start_supervised!(%{
        id: {:root, System.unique_integer([:positive])},
        type: :supervisor,
        start:
          {Topology, :start_link,
           [
             ConsumerWorker,
             registry,
             :consumer_count,
             opts(overrides),
             [resolver: fn _topic, _opts -> Process.sleep(:infinity) end]
           ]}
      })
    end
  end
end
