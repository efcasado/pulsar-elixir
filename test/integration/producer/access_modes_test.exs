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
    assert {:ok, root_1} =
             Pulsar.Producer.start(@shared_topic, access_mode: :shared, client: @client)

    assert {:ok, root_2} =
             Pulsar.Producer.start(@shared_topic,
               access_mode: :shared,
               name: "shared-producer-2",
               client: @client
             )

    :ok = Pulsar.Producer.await_ready(root_1)
    :ok = Pulsar.Producer.await_ready(root_2)

    assert {:ok, _} = Pulsar.Producer.send(root_1, "Message from producer 1")
    assert {:ok, _} = Pulsar.Producer.send(root_2, "Message from producer 2")

    Pulsar.Producer.stop(root_1)
    Pulsar.Producer.stop(root_2)
  end

  @tag telemetry_listen: [@opened]
  test ":exclusive fences a second producer until the first releases the topic", %{broker: broker} do
    contender = Utils.start_isolated_client(:access_modes_contender, broker)

    assert {:ok, root_1} =
             Pulsar.Producer.start(@exclusive_topic, access_mode: :exclusive, client: @client)

    :ok = Pulsar.Producer.await_ready(root_1)
    [worker_1] = Topology.workers(root_1)

    assert {:ok, _} = Pulsar.Producer.send(root_1, "Exclusive message", client: @client)

    assert {:ok, root_2} =
             Pulsar.Producer.start(@exclusive_topic,
               access_mode: :exclusive,
               name: "exclusive-2",
               client: contender
             )

    ref = Process.monitor(root_2)

    assert_receive {:telemetry_event,
                    %{
                      event: @opened,
                      metadata: %{success: false, error: :producer_fenced, producer_name: "exclusive-2" <> _}
                    }}

    assert_receive {:DOWN, ^ref, :process, ^root_2, :shutdown}, 10_000

    assert Pulsar.Producer.await_ready(root_2, timeout: 1_000) == {:error, :not_found}
    refute root_2 in Pulsar.Client.producers(contender)

    # Its client keeps starting it over while its own budget lasts, so it is still contending for
    # the topic. Take that client down before releasing it, or which producer wins is a race.
    stop_supervised!(:access_modes_contender)

    ref = Process.monitor(worker_1)
    Pulsar.Producer.stop(root_1)
    assert_receive {:DOWN, ^ref, :process, ^worker_1, _reason}

    assert {:ok, root_2} =
             Pulsar.Producer.start(@exclusive_topic,
               access_mode: :exclusive,
               name: "exclusive-3",
               client: @client
             )

    :ok = Pulsar.Producer.await_ready(root_2)

    assert {:ok, _} = Pulsar.Producer.send(root_2, "New exclusive owner", client: @client)

    Pulsar.Producer.stop(root_2)
  end

  test ":wait_for_exclusive queues behind the exclusive producer rather than failing" do
    # See: https://github.com/apache/pulsar/blob/master/pip/pip-68.md

    assert {:ok, root_1} =
             Pulsar.Producer.start(@wait_exclusive_topic,
               access_mode: :exclusive,
               name: "producer-1",
               client: @client
             )

    :ok = Pulsar.Producer.await_ready(root_1)
    [worker_1] = Topology.workers(root_1)

    {:ok, root_2} =
      Pulsar.Producer.start(@wait_exclusive_topic,
        access_mode: :wait_for_exclusive,
        name: "waiting-producer-2",
        client: @client
      )

    # The topology-only barrier returns once the queued worker exists, without waiting for the
    # exclusive producer to release the topic.
    :ok = Topology.await_ready(root_2, 10_000)
    [worker_2] = Topology.workers(root_2)
    worker_2_state = :sys.get_state(worker_2)

    assert String.starts_with?(worker_2_state.producer_name, "waiting-producer-2")
    refute worker_2_state.ready

    assert {:ok, _} = Pulsar.Producer.send(root_1, "Message from first producer", client: @client)

    assert {:error, :not_ready} =
             Pulsar.Producer.send(root_2, "Message from second producer while waiting", client: @client)

    ref = Process.monitor(worker_1)
    Pulsar.Producer.stop(root_1)
    assert_receive {:DOWN, ^ref, :process, ^worker_1, _reason}

    :ok = Pulsar.Producer.await_ready(root_2)

    assert {:ok, _} = Pulsar.Producer.send(root_2, "Message from second producer", client: @client)

    Pulsar.Producer.stop(root_2)
  end

  @tag telemetry_listen: [@opened]
  test ":exclusive_with_fencing takes the topic from the producer holding it" do
    {:ok, root_1} =
      Pulsar.Producer.start(@exclusive_with_fencing_topic,
        access_mode: :exclusive,
        name: "original-exclusive",
        client: @client
      )

    :ok = Pulsar.Producer.await_ready(root_1)
    [worker_1] = Topology.workers(root_1)

    # Taken before the fencing below, which is what brings this producer down.
    fenced_ref = Process.monitor(worker_1)

    assert :sys.get_state(worker_1).topic_epoch == 0
    assert {:ok, _} = Pulsar.Producer.send(root_1, "Message from original producer", client: @client)

    {:ok, root_2} =
      Pulsar.Producer.start(@exclusive_with_fencing_topic,
        access_mode: :exclusive_with_fencing,
        name: "fencing-takeover",
        client: @client
      )

    :ok = Pulsar.Producer.await_ready(root_2)
    [worker_2] = Topology.workers(root_2)

    worker_2_state = :sys.get_state(worker_2)
    assert worker_2_state.topic_epoch == 1

    # Both producers are still alive (broker doesn't proactively close fenced producers)
    assert Process.alive?(worker_1)
    assert Process.alive?(worker_2)

    Utils.wait_for(fn ->
      match?(
        {:error, {:producer_died, _}},
        Pulsar.Producer.send(root_1, "Message from fenced producer", client: @client)
      )
    end)

    assert_receive {:DOWN, ^fenced_ref, :process, ^worker_1, _reason},
                   5_000,
                   "the fenced producer should have been stopped"

    Utils.wait_for(fn ->
      match?({:ok, _}, Pulsar.Producer.send(root_2, "Probe message", client: @client))
    end)

    # Both opened before either was fenced. A worker keeps the configured name with the sole
    # worker's suffix, so the prefix is what identifies it.
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

    Pulsar.Producer.stop(root_2)
  end
end
