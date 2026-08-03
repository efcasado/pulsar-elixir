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

  defp attach(overrides \\ []), do: DeadLetter.attach(opts(overrides), self())

  describe "attach/2" do
    test "a consumer without a dead letter policy attaches nothing" do
      assert {annotated, []} = attach(dead_letter_policy: nil)
      refute Keyword.has_key?(annotated, :dead_letter_root)
    end

    test "tells the workers which root owns the producer it attached" do
      root = self()

      assert {annotated, [_spec]} = DeadLetter.attach(opts(), root)
      assert Keyword.fetch!(annotated, :dead_letter_root) == root
    end

    test "defaults the topic to the base topic and subscription" do
      assert {_opts, [%{id: {:dead_letter, @default_dlq}} = spec]} = attach()
      assert {Topology, :start_link, [_worker, _registry, _count_key, producer_opts]} = spec.start
      assert Keyword.fetch!(producer_opts, :topic) == @default_dlq
    end

    test "honours an explicit dead letter topic" do
      explicit = "persistent://public/default/parked"

      assert {_opts, [%{id: {:dead_letter, ^explicit}}]} =
               attach(dead_letter_policy: [max_redelivery: 3, topic: explicit])
    end

    # Two subscriptions may be configured to divert into the same topic, and each still gets
    # its own producer with its own identity.
    test "names the producer after the consumer rather than the dead letter topic" do
      explicit = [max_redelivery: 3, topic: "persistent://public/default/parked"]

      names =
        for name <- [@consumer_name, "#{@consumer_name}-other"] do
          {_opts, [%{start: {_module, _fun, [_worker, _registry, _count_key, producer_opts]}}]} =
            attach(name: name, dead_letter_policy: explicit)

          Keyword.fetch!(producer_opts, :name)
        end

      assert names == Enum.uniq(names)
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

    # It comes up even though the consumer's own discovery never resolves here, so it is not
    # sequenced behind it and does not wait on a broker.
    test "the dead letter producer is reached through its consumer, not the producer registry" do
      root = start_consumer_topology()

      assert {_id, dlq, :supervisor, _modules} =
               root |> Supervisor.which_children() |> List.keyfind({:dead_letter, @default_dlq}, 0)

      assert is_pid(dlq)

      # Registering would tie a consumer's startup to a branch that restarts separately from it.
      assert Client.lookup(:producers, "#{@consumer_name}-dead-letter-producer", :test) ==
               {:error, :not_found}
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
             Keyword.put(opts(overrides), :companions, &DeadLetter.attach/2),
             [resolver: fn _topic, _opts -> Process.sleep(:infinity) end]
           ]}
      })
    end
  end
end
