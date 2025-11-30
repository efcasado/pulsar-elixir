defmodule Pulsar.Integration.AccessModesTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  require Logger

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

  test "multiple producers can publish with :Shared access mode" do
    # Start two separate producer groups with :Shared mode on same topic
    assert {:ok, group_pid_1} =
             Pulsar.start_producer(@shared_topic, access_mode: :Shared, client: @client)

    assert {:ok, group_pid_2} =
             Pulsar.start_producer(@shared_topic,
               access_mode: :Shared,
               name: "shared-producer-2",
               client: @client
             )

    [producer_1] = Pulsar.get_producers(group_pid_1)
    [producer_2] = Pulsar.get_producers(group_pid_2)

    # Wait for both producers to register
    Utils.wait_for(fn -> :sys.get_state(producer_1).producer_name != nil end)
    Utils.wait_for(fn -> :sys.get_state(producer_2).producer_name != nil end)

    # Both should send successfully
    assert {:ok, _} = Pulsar.send(group_pid_1, "Message from producer 1")
    assert {:ok, _} = Pulsar.send(group_pid_2, "Message from producer 2")

    Pulsar.stop_producer(group_pid_1)
    Pulsar.stop_producer(group_pid_2)
  end

  @tag telemetry_listen: [[:pulsar, :producer, :opened, :stop]]
  test "only one producer can connect with :Exclusive access mode" do
    # Start first producer with :Exclusive
    assert {:ok, group_pid_1} =
             Pulsar.start_producer(@exclusive_topic, access_mode: :Exclusive, client: @client)

    [producer_1] = Pulsar.get_producers(group_pid_1)

    Utils.wait_for(fn -> :sys.get_state(producer_1).producer_name != nil end)

    assert {:ok, _} = Pulsar.send(group_pid_1, "Exclusive message", client: @client)

    assert {:ok, group_pid_2} =
             Pulsar.start_producer(@exclusive_topic,
               access_mode: :Exclusive,
               name: "exclusive-2",
               client: @client
             )

    # Second producer should fail to register (fenced by the existing exclusive producer)
    Utils.wait_for(fn -> not Process.alive?(group_pid_2) end)

    events = Utils.collect_events([:pulsar, :producer, :opened, :stop], producer_names: ["exclusive-2"])
    assert Enum.any?(events, &match?(%{success: false, error: :producer_fenced, producer_name: "exclusive-2"}, &1))

    # Stop the first producer to release exclusive lock
    Pulsar.stop_producer(group_pid_1)
    Utils.wait_for(fn -> not Process.alive?(producer_1) end)

    # New exclusive producer should now succeed
    assert {:ok, group_pid_2} =
             Pulsar.start_producer(@exclusive_topic,
               access_mode: :Exclusive,
               name: "exclusive-3",
               client: @client
             )

    [producer_2] = Pulsar.get_producers(group_pid_2)
    Utils.wait_for(fn -> :sys.get_state(producer_2).producer_name != nil end)

    assert {:ok, _} = Pulsar.send(group_pid_2, "New exclusive owner", client: @client)

    Pulsar.stop_producer(group_pid_2)
  end

  test ":WaitForExclusive waits for exclusive access " do
    # See: https://github.com/apache/pulsar/blob/master/pip/pip-68.md

    # Start first producer with :Exclusive - becomes the exclusive producer immediately
    assert {:ok, group_pid_1} =
             Pulsar.start_producer(@wait_exclusive_topic,
               access_mode: :Exclusive,
               name: "producer-1",
               client: @client
             )

    [producer_1] = Pulsar.get_producers(group_pid_1)
    Utils.wait_for(fn -> :sys.get_state(producer_1).ready end)

    # Start second producer with :WaitForExclusive. It should not be ready
    {:ok, group_pid_2} =
      Pulsar.start_producer(@wait_exclusive_topic,
        access_mode: :WaitForExclusive,
        name: "waiting-producer-2",
        client: @client
      )

    [producer_2] = Pulsar.get_producers(group_pid_2)

    Utils.wait_for(fn -> :sys.get_state(producer_2).producer_name == "waiting-producer-2" end)
    refute :sys.get_state(producer_2).ready

    # First producer can send messages
    assert {:ok, _} = Pulsar.send(group_pid_1, "Message from first producer", client: @client)

    # Second producer should not be able to send messages yet
    assert {:error, :producer_waiting} =
             Pulsar.send(group_pid_2, "Message from second producer while waiting", client: @client)

    # Now stop the first producer to release exclusive access
    Pulsar.stop_producer(group_pid_1)
    Utils.wait_for(fn -> not Process.alive?(producer_1) end)

    # Second producer should now get exclusive access
    Utils.wait_for(fn -> :sys.get_state(producer_2).ready end)

    # Second producer should now be able to send messages
    assert {:ok, _} = Pulsar.send(group_pid_2, "Message from second producer", client: @client)

    Pulsar.stop_producer(group_pid_2)
  end

  @tag telemetry_listen: [[:pulsar, :producer, :opened, :stop]]
  test ":ExclusiveWithFencing takes over and fences old producer" do
    {:ok, group_pid_1} =
      Pulsar.start_producer(@exclusive_with_fencing_topic,
        access_mode: :Exclusive,
        name: "original-exclusive",
        client: @client
      )

    [producer_1] = Pulsar.get_producers(group_pid_1)
    Utils.wait_for(fn -> :sys.get_state(producer_1).ready end)

    assert :sys.get_state(producer_1).topic_epoch == 0
    assert {:ok, _} = Pulsar.send(group_pid_1, "Message from original producer", client: @client)

    # Step 2: Start second producer with :ExclusiveWithFencing. It should fence out first
    {:ok, group_pid_2} =
      Pulsar.start_producer(@exclusive_with_fencing_topic,
        access_mode: :ExclusiveWithFencing,
        name: "fencing-takeover",
        client: @client
      )

    [producer_2] = Pulsar.get_producers(group_pid_2)
    Utils.wait_for(fn -> :sys.get_state(producer_2).ready end)

    producer_2_state = :sys.get_state(producer_2)
    assert producer_2_state.topic_epoch == 1

    # Both producers are still alive (broker doesn't proactively close fenced producers)
    assert Process.alive?(producer_1)
    assert Process.alive?(producer_2)

    # Step 3: Try to send from the fenced (original) producer
    Utils.wait_for(fn ->
      match?({:error, {:producer_died, _}}, Pulsar.send(group_pid_1, "Message from fenced producer", client: @client))
    end)

    Utils.wait_for(fn -> not Process.alive?(producer_1) end)
    refute Process.alive?(producer_1), "Old producer should be fenced and stopped"

    # Step 4: Wait for broker to reconnect and producer to be ready to send
    Utils.wait_for(fn ->
      match?({:ok, _}, Pulsar.send(group_pid_2, "Probe message", client: @client))
    end)

    # Wait for telemetry events to be collected
    Process.sleep(100)

    all_events =
      Utils.collect_events([:pulsar, :producer, :opened, :stop],
        producer_names: ["original-exclusive", "fencing-takeover"]
      )

    # Should have:
    # - 2 successful initial opens (producer 1 and producer 2)
    # - 1 failed reopen (producer 1 gets fenced when trying to reconnect)
    # - 1 successful reopen (producer 2 reconnected after broker disconnect)
    assert Enum.count(all_events) == 4

    assert Enum.any?(
             all_events,
             &match?(%{success: false, error: :producer_fenced, producer_name: "original-exclusive"}, &1)
           )

    # Cleanup
    Pulsar.stop_producer(group_pid_2)
  end
end
