defmodule Pulsar.Integration.AccessModesTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils
  alias Pulsar.Topology

  @moduletag :integration
  @client :access_modes_test_client
  @shared_topic "persistent://public/default/producer-shared-test"
  @exclusive_topic "persistent://public/default/producer-exclusive-test"
  @wait_exclusive_topic "persistent://public/default/producer-wait-exclusive-test"
  @exclusive_with_fencing_topic "persistent://public/default/producer-exclusive-fencing-test"

  setup_all do
    broker = System.broker()

    :ok = System.create_topic(@shared_topic)
    :ok = System.create_topic(@exclusive_topic)
    :ok = System.create_topic(@wait_exclusive_topic)
    :ok = System.create_topic(@exclusive_with_fencing_topic)

    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)
  end

  setup [:telemetry_listen]

  test "multiple producers can publish with :shared access mode" do
    # Start two separate producer groups with :shared mode on same topic
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

  @tag telemetry_listen: [[:pulsar, :producer, :opened, :stop]]
  test "only one producer can connect with :exclusive access mode" do
    # Start first producer with :exclusive
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

    # Second producer should fail to register (fenced by the existing exclusive producer)
    :ok = Topology.await_ready(group_pid_2, 1_000)
    Utils.wait_for(fn -> Topology.workers(group_pid_2) == [] end)

    assert Pulsar.Producer.await_ready(group_pid_2, timeout: 0) == {:error, :timeout}

    assert Process.alive?(group_pid_2)
    assert group_pid_2 in Pulsar.Client.producers(@client)
    assert {:error, :no_producers_available} = Pulsar.Producer.send(group_pid_2, "Rejected message")

    events = Utils.collect_events([:pulsar, :producer, :opened, :stop], producer_names: ["exclusive-2"])

    assert Enum.any?(
             events,
             &(&1.success == false and &1.error == :producer_fenced and
                 String.starts_with?(&1.producer_name, "exclusive-2"))
           )

    assert :ok = Pulsar.Producer.stop(group_pid_2, client: @client)
    Utils.wait_for(fn -> not Process.alive?(group_pid_2) end)
    refute group_pid_2 in Pulsar.Client.producers(@client)

    # Stop the first producer to release exclusive lock
    Pulsar.Producer.stop(group_pid_1)
    Utils.wait_for(fn -> not Process.alive?(producer_1) end)

    # New exclusive producer should now succeed
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

  test ":wait_for_exclusive waits for exclusive access " do
    # See: https://github.com/apache/pulsar/blob/master/pip/pip-68.md

    # Start first producer with :exclusive - becomes the exclusive producer immediately
    assert {:ok, group_pid_1} =
             Pulsar.Producer.start(@wait_exclusive_topic,
               access_mode: :exclusive,
               name: "producer-1",
               client: @client
             )

    :ok = Pulsar.Producer.await_ready(group_pid_1)
    [producer_1] = Topology.workers(group_pid_1)

    # Start second producer with :wait_for_exclusive. It should not be ready
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

    # First producer can send messages
    assert {:ok, _} = Pulsar.Producer.send(group_pid_1, "Message from first producer", client: @client)

    # Second producer should not be able to send messages yet
    assert {:error, :producer_waiting} =
             Pulsar.Producer.send(group_pid_2, "Message from second producer while waiting", client: @client)

    # Now stop the first producer to release exclusive access
    Pulsar.Producer.stop(group_pid_1)
    Utils.wait_for(fn -> not Process.alive?(producer_1) end)

    # Second producer should now get exclusive access
    :ok = Pulsar.Producer.await_ready(group_pid_2)

    # Second producer should now be able to send messages
    assert {:ok, _} = Pulsar.Producer.send(group_pid_2, "Message from second producer", client: @client)

    Pulsar.Producer.stop(group_pid_2)
  end

  @tag telemetry_listen: [[:pulsar, :producer, :opened, :stop]]
  test ":exclusive_with_fencing takes over and fences old producer" do
    {:ok, group_pid_1} =
      Pulsar.Producer.start(@exclusive_with_fencing_topic,
        access_mode: :exclusive,
        name: "original-exclusive",
        client: @client
      )

    :ok = Pulsar.Producer.await_ready(group_pid_1)
    [producer_1] = Topology.workers(group_pid_1)

    assert :sys.get_state(producer_1).topic_epoch == 0
    assert {:ok, _} = Pulsar.Producer.send(group_pid_1, "Message from original producer", client: @client)

    # Step 2: Start second producer with :exclusive_with_fencing. It should fence out first
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

    # Step 3: Try to send from the fenced (original) producer
    Utils.wait_for(fn ->
      match?(
        {:error, {:producer_died, _}},
        Pulsar.Producer.send(group_pid_1, "Message from fenced producer", client: @client)
      )
    end)

    Utils.wait_for(fn -> not Process.alive?(producer_1) end)
    refute Process.alive?(producer_1), "Old producer should be fenced and stopped"

    # Step 4: Wait for broker to reconnect and producer to be ready to send
    Utils.wait_for(fn ->
      match?({:ok, _}, Pulsar.Producer.send(group_pid_2, "Probe message", client: @client))
    end)

    fenced? =
      &(&1.success == false and &1.error == :producer_fenced and
          String.starts_with?(&1.producer_name, "original-exclusive"))

    # Fencing is observed asynchronously after the broker reconnects, so accumulate events
    # across collection windows until the original producer reports the terminal error.
    all_events =
      Enum.reduce_while(1..150, [], fn _attempt, collected ->
        collected =
          collected ++
            Utils.collect_events([:pulsar, :producer, :opened, :stop],
              producer_names: ["original-exclusive", "fencing-takeover"]
            )

        if Enum.any?(collected, fenced?) do
          {:halt, collected}
        else
          Process.sleep(100)
          {:cont, collected}
        end
      end)

    for group <- ["original-exclusive", "fencing-takeover"] do
      assert Enum.any?(all_events, &(&1.success and String.starts_with?(&1.producer_name, group)))
    end

    assert Enum.any?(all_events, fenced?)

    # Cleanup
    Pulsar.Producer.stop(group_pid_2)
  end
end
