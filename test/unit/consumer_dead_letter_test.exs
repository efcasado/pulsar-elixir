defmodule Pulsar.Consumer.DeadLetterTest do
  use ExUnit.Case, async: true

  alias Pulsar.Client
  alias Pulsar.Consumer.DeadLetter
  alias Pulsar.Consumer.Worker, as: ConsumerWorker
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Proto
  alias Pulsar.Test.Support.Utils
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
      assert {Pulsar.Producer, :start_link_unregistered, [producer_opts]} = spec.start
      assert Keyword.fetch!(producer_opts, :topic) == @default_dlq
    end

    test "honours an explicit dead letter topic" do
      explicit = "persistent://public/default/parked"

      assert {_opts, [%{id: {:dead_letter, ^explicit}}]} =
               attach(dead_letter_policy: [max_redelivery: 3, topic: explicit])
    end

    test "passes producer options through to the producer it attaches" do
      policy = [max_redelivery: 3, producer: [compression: :lz4, batch_enabled: false]]

      assert {_opts, [%{start: {_module, _fun, [producer_opts]}}]} = attach(dead_letter_policy: policy)
      assert Keyword.fetch!(producer_opts, :compression) == :lz4
      assert Keyword.fetch!(producer_opts, :batch_enabled) == false
    end

    test "the consumer still decides the topic, client and name" do
      policy = [max_redelivery: 3, producer: [compression: :lz4]]

      assert {_opts, [%{start: {_module, _fun, [producer_opts]}}]} = attach(dead_letter_policy: policy)
      assert Keyword.fetch!(producer_opts, :topic) == @default_dlq
      assert Keyword.fetch!(producer_opts, :client) == :test
      assert Keyword.fetch!(producer_opts, :name) == "#{@consumer_name}-dead-letter-producer"
    end

    # Two subscriptions may be configured to divert into the same topic, and each still gets
    # its own producer with its own identity.
    test "names the producer after the consumer rather than the dead letter topic" do
      explicit = [max_redelivery: 3, topic: "persistent://public/default/parked"]

      names =
        for name <- [@consumer_name, "#{@consumer_name}-other"] do
          {_opts, [%{start: {_module, _fun, [producer_opts]}}]} =
            attach(name: name, dead_letter_policy: explicit)

          Keyword.fetch!(producer_opts, :name)
        end

      assert names == Enum.uniq(names)
    end
  end

  describe "origin_properties/2" do
    defp message(message_id, properties \\ []) do
      %Pulsar.Message{
        payload: "payload",
        message_id: message_id,
        raw: %{metadata: %{properties: Enum.map(properties, fn {k, v} -> %Proto.KeyValue{key: k, value: v} end)}}
      }
    end

    defp message_id(overrides \\ []) do
      struct(%Proto.MessageIdData{ledgerId: 7, entryId: 42, partition: -1, batch_index: -1}, overrides)
    end

    test "names the topic the message was consumed from" do
      properties = DeadLetter.origin_properties("orders-partition-2", message(message_id()))

      assert properties["REAL_TOPIC"] == "orders-partition-2"
    end

    test "names the message it came from" do
      properties = DeadLetter.origin_properties("orders", message(message_id(partition: 3)))

      assert properties["ORIGIN_MESSAGE_ID"] == "7:42:3"
    end

    test "adds to the origin's properties rather than replacing them" do
      properties = DeadLetter.origin_properties("orders", message(message_id(), [{"tenant", "acme"}]))

      assert properties["tenant"] == "acme"
      assert Map.has_key?(properties, "REAL_TOPIC")
    end
  end

  describe "under a consumer topology" do
    setup do
      start_supervised!({Registry, keys: :unique, name: Client.registry(:producers, :test)})
      start_supervised!({Registry, keys: :unique, name: Client.registry(:consumers, :test)})

      :ok
    end

    test "runs the dead letter producer as a child of the consumer it belongs to" do
      dlq = dead_letter_producer(root())

      assert is_pid(dlq)
      assert Topology.kind(dlq) == :topology
      assert Topology.resource?(dlq, :producers)
    end

    test "restarts the dead letter producer when it dies" do
      root = root()
      dlq = dead_letter_producer(root)
      ref = Process.monitor(dlq)

      Process.exit(dlq, :kill)
      assert_receive {:DOWN, ^ref, :process, ^dlq, :killed}

      restarted = Utils.wait_for(fn -> dead_letter_producer(root) end, until: &(is_pid(&1) and &1 != dlq))

      assert is_pid(restarted)
      assert Topology.resource?(restarted, :producers)
    end

    test "a retired dead letter producer is not started over" do
      root = root()
      dlq = dead_letter_producer(root)
      ref = Process.monitor(dlq)

      # A static child, so it is reached by id; stopping a :permanent child would start it over.
      :ok = Topology.remove(dlq)
      assert_receive {:DOWN, ^ref, :process, ^dlq, _reason}

      assert dead_letter_producer(root) == nil
    end

    test "goes down with the consumer that owns it" do
      root = root()
      dlq = dead_letter_producer(root)
      ref = Process.monitor(dlq)

      Supervisor.stop(root)

      assert_receive {:DOWN, ^ref, :process, ^dlq, _reason}
    end

    # It comes up even though the consumer's own discovery never resolves here, so it is not
    # sequenced behind it and does not wait on a broker.
    test "the dead letter producer is reached through its consumer, not the producer registry" do
      assert root() |> dead_letter_producer() |> is_pid()

      # Registering would tie a consumer's startup to a branch that restarts separately from it.
      assert Client.lookup(:producers, "#{@consumer_name}-dead-letter-producer", :test) ==
               {:error, :not_found}
    end

    test "a consumer without a dead letter policy has no such child" do
      assert [dead_letter_policy: nil] |> root() |> dead_letter_producer() == nil
    end

    defp dead_letter_producer(root) do
      case root |> Supervisor.which_children() |> List.keyfind({:dead_letter, @default_dlq}, 0) do
        {_id, pid, :supervisor, _modules} when is_pid(pid) -> pid
        _absent -> nil
      end
    end

    # Discovery never resolves, so no consumer worker is started and none of this needs a broker.
    defp root(overrides \\ []) do
      registry = :"consumer-topology-#{System.unique_integer([:positive])}"
      start_supervised!({Registry, keys: :unique, name: registry})

      start_supervised!(%{
        id: {:root, System.unique_integer([:positive])},
        type: :supervisor,
        restart: :temporary,
        start:
          {Topology, :start_link,
           [
             ConsumerWorker,
             registry,
             :consumers,
             Keyword.put(opts(overrides), :companions, &DeadLetter.attach/2),
             [resolver: fn _topic, _opts -> Process.sleep(:infinity) end]
           ]}
      })
    end
  end
end
