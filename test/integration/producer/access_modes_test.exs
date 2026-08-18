defmodule Pulsar.Integration.AccessModesTest do
  use Pulsar.Test.Case, async: true

  @shared_topic "persistent://public/default/producer-shared-test"
  @exclusive_topic "persistent://public/default/producer-exclusive-test"
  @wait_exclusive_topic "persistent://public/default/producer-wait-exclusive-test"
  @exclusive_with_fencing_topic "persistent://public/default/producer-exclusive-fencing-test"
  @opened [:pulsar, :producer, :opened, :stop]

  setup_all do
    :ok = System.create_topic(@shared_topic)
    :ok = System.create_topic(@exclusive_topic)
    :ok = System.create_topic(@wait_exclusive_topic)
    :ok = System.create_topic(@exclusive_with_fencing_topic)
  end

  test ":shared lets several producers publish to one topic at once" do
    assert {:ok, group_pid_1} =
             Pulsar.Producer.start(@shared_topic, access_mode: :shared, client: @client)

    assert {:ok, group_pid_2} =
             Pulsar.Producer.start(@shared_topic,
               access_mode: :shared,
               name: "shared-producer-2",
               client: @client
             )

    :ok = Pulsar.Producer.await_ready(group_pid_1)
    :ok = Pulsar.Producer.await_ready(group_pid_2)

    assert {:ok, _} = Pulsar.Producer.send(group_pid_1, "Message from producer 1")
    assert {:ok, _} = Pulsar.Producer.send(group_pid_2, "Message from producer 2")

    Pulsar.Producer.stop(group_pid_1)
    Pulsar.Producer.stop(group_pid_2)
  end

  @tag telemetry_listen: [@opened]
  test ":exclusive fences a second producer until the first releases the topic" do
    assert {:ok, group_pid_1} =
             Pulsar.Producer.start(@exclusive_topic, access_mode: :exclusive, client: @client)

    :ok = Pulsar.Producer.await_ready(group_pid_1)
    [producer_1] = Topology.workers(group_pid_1)

    assert {:ok, _} = Pulsar.Producer.send(group_pid_1, "Exclusive message", client: @client)

    assert {:ok, group_pid_2} =
             Pulsar.Producer.start(@exclusive_topic,
               access_mode: :exclusive,
               name: "exclusive-2",
               client: @client
             )

    :ok = Topology.await_ready(group_pid_2, 1_000)

    # Fenced by the producer already holding the topic, so it gives up rather than ever
    # becoming ready.
    assert Pulsar.Producer.await_ready(group_pid_2, timeout: 2_000) == {:error, :timeout}
    assert Topology.workers(group_pid_2) == []

    assert Process.alive?(group_pid_2)
    assert group_pid_2 in Pulsar.Client.producers(@client)
    assert {:error, :no_producers_available} = Pulsar.Producer.send(group_pid_2, "Rejected message")

    assert_receive {:telemetry_event,
                    %{
                      event: @opened,
                      metadata: %{success: false, error: :producer_fenced, producer_name: "exclusive-2" <> _}
                    }}

    ref = Process.monitor(group_pid_2)
    assert :ok = Pulsar.Producer.stop(group_pid_2, client: @client)
    assert_receive {:DOWN, ^ref, :process, ^group_pid_2, _reason}
    refute group_pid_2 in Pulsar.Client.producers(@client)

    ref = Process.monitor(producer_1)
    Pulsar.Producer.stop(group_pid_1)
    assert_receive {:DOWN, ^ref, :process, ^producer_1, _reason}

    assert {:ok, group_pid_2} =
             Pulsar.Producer.start(@exclusive_topic,
               access_mode: :exclusive,
               name: "exclusive-3",
               client: @client
             )

    :ok = Pulsar.Producer.await_ready(group_pid_2)

    assert {:ok, _} = Pulsar.Producer.send(group_pid_2, "New exclusive owner", client: @client)

    Pulsar.Producer.stop(group_pid_2)
  end

  test ":wait_for_exclusive queues behind the exclusive producer rather than failing" do
    # See: https://github.com/apache/pulsar/blob/master/pip/pip-68.md

    assert {:ok, group_pid_1} =
             Pulsar.Producer.start(@wait_exclusive_topic,
               access_mode: :exclusive,
               name: "producer-1",
               client: @client
             )

    :ok = Pulsar.Producer.await_ready(group_pid_1)
    [producer_1] = Topology.workers(group_pid_1)

    {:ok, group_pid_2} =
      Pulsar.Producer.start(@wait_exclusive_topic,
        access_mode: :wait_for_exclusive,
        name: "waiting-producer-2",
        client: @client
      )

    # await_ready/2 would wait out its whole timeout here: this producer is queued behind the
    # exclusive one and stays unready until that one goes.
    [producer_2] = Utils.wait_for(fn -> Topology.workers(group_pid_2) end, until: &match?([_], &1))

    Utils.wait_for(fn ->
      String.starts_with?(:sys.get_state(producer_2).producer_name || "", "waiting-producer-2")
    end)

    refute :sys.get_state(producer_2).ready

    assert {:ok, _} = Pulsar.Producer.send(group_pid_1, "Message from first producer", client: @client)

    assert {:error, :producer_waiting} =
             Pulsar.Producer.send(group_pid_2, "Message from second producer while waiting", client: @client)

    ref = Process.monitor(producer_1)
    Pulsar.Producer.stop(group_pid_1)
    assert_receive {:DOWN, ^ref, :process, ^producer_1, _reason}

    :ok = Pulsar.Producer.await_ready(group_pid_2)

    assert {:ok, _} = Pulsar.Producer.send(group_pid_2, "Message from second producer", client: @client)

    Pulsar.Producer.stop(group_pid_2)
  end

  @tag telemetry_listen: [@opened]
  test ":exclusive_with_fencing takes the topic from the producer holding it" do
    {:ok, group_pid_1} =
      Pulsar.Producer.start(@exclusive_with_fencing_topic,
        access_mode: :exclusive,
        name: "original-exclusive",
        client: @client
      )

    :ok = Pulsar.Producer.await_ready(group_pid_1)
    [producer_1] = Topology.workers(group_pid_1)

    # Taken before the fencing below, which is what brings this producer down.
    fenced_ref = Process.monitor(producer_1)

    assert :sys.get_state(producer_1).topic_epoch == 0
    assert {:ok, _} = Pulsar.Producer.send(group_pid_1, "Message from original producer", client: @client)

    {:ok, group_pid_2} =
      Pulsar.Producer.start(@exclusive_with_fencing_topic,
        access_mode: :exclusive_with_fencing,
        name: "fencing-takeover",
        client: @client
      )

    :ok = Pulsar.Producer.await_ready(group_pid_2)
    [producer_2] = Topology.workers(group_pid_2)

    producer_2_state = :sys.get_state(producer_2)
    assert producer_2_state.topic_epoch == 1

    # Both producers are still alive (broker doesn't proactively close fenced producers)
    assert Process.alive?(producer_1)
    assert Process.alive?(producer_2)

    Utils.wait_for(fn ->
      match?(
        {:error, {:producer_died, _}},
        Pulsar.Producer.send(group_pid_1, "Message from fenced producer", client: @client)
      )
    end)

    assert_receive {:DOWN, ^fenced_ref, :process, ^producer_1, _reason},
                   5_000,
                   "the fenced producer should have been stopped"

    Utils.wait_for(fn ->
      match?({:ok, _}, Pulsar.Producer.send(group_pid_2, "Probe message", client: @client))
    end)

    # Both opened before either was fenced. A worker is named after its group with an index
    # suffix, so the prefix is what identifies it.
    assert_receive {:telemetry_event,
                    %{event: @opened, metadata: %{success: true, producer_name: "original-exclusive" <> _}}}

    assert_receive {:telemetry_event,
                    %{event: @opened, metadata: %{success: true, producer_name: "fencing-takeover" <> _}}}

    # Fencing is only observed once the broker has reconnected, which is what the wait is for.
    assert_receive {:telemetry_event,
                    %{
                      event: @opened,
                      metadata: %{
                        success: false,
                        error: :producer_fenced,
                        producer_name: "original-exclusive" <> _
                      }
                    }},
                   15_000

    Pulsar.Producer.stop(group_pid_2)
  end
end
