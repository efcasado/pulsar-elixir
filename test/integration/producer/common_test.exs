defmodule Pulsar.Integration.Producer.CommonTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils
  alias Pulsar.Topology

  @moduletag :integration
  @client :producer_common_test_client
  @topic "persistent://public/default/producer-common-test"

  setup_all do
    broker = System.broker()

    :ok = System.create_topic(@topic)

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

  test "send returns error when producer not found" do
    assert {:error, :not_found} =
             Pulsar.Producer.send("non-existent-producer-group", "message", client: @client)
  end

  test "await_ready waits for producer registration" do
    {:ok, producer} =
      Pulsar.Producer.start(@topic,
        client: @client,
        name: "delayed-producer",
        startup_delay_ms: 500
      )

    assert :ok = Topology.await_ready(producer, 1_000)
    assert Pulsar.Producer.await_ready(producer, timeout: 25) == {:error, :timeout}
    assert :ok = Pulsar.Producer.await_ready(producer)
    assert {:ok, _message_id} = Pulsar.Producer.send(producer, "ready")
    assert :ok = Pulsar.Producer.stop(producer, client: @client)
  end

  @tag telemetry_listen: [
         [:pulsar, :producer, :opened, :stop],
         [:pulsar, :producer, :closed, :stop],
         [:pulsar, :producer, :message, :published]
       ]
  test "create and send message" do
    producer_group_name = "my-producer"

    assert {:ok, group_pid} =
             Pulsar.Producer.start(@topic <> "-lifecycle",
               client: @client,
               name: producer_group_name
             )

    [producer] = Utils.wait_for(fn -> Topology.workers(group_pid) end, until: &match?([_], &1))

    Utils.wait_for(fn ->
      state = :sys.get_state(producer)
      state.producer_name != nil
    end)

    assert %{success_count: 1, failure_count: 0, total_count: 1} =
             Utils.collect_stats([:pulsar, :producer, :opened, :stop], producer_names: [producer_group_name])

    assert {:ok, message_id_data} = Pulsar.Producer.send(producer_group_name, "Hello, Pulsar!", client: @client)

    assert message_id_data.ledgerId
    assert message_id_data.entryId

    assert {:ok, _message_id_data2} = Pulsar.Producer.send(group_pid, "Another message with pid!")

    assert [_first, _second] =
             Utils.collect_events([:pulsar, :producer, :message, :published], producer_names: [producer_group_name])

    assert :ok = Pulsar.Producer.stop(group_pid)
    Utils.wait_for(fn -> not Process.alive?(producer) end)

    assert %{success_count: 1, failure_count: 0, total_count: 1} =
             Utils.collect_stats([:pulsar, :producer, :closed, :stop], producer_names: [producer_group_name])
  end

  test "send multiple messages asynchronously" do
    {:ok, producer} =
      Pulsar.Producer.start(@topic,
        client: @client,
        name: "async-producer"
      )

    assert :ok = Pulsar.Producer.await_ready(producer)

    refs =
      Enum.map(["one", "two", "three"], fn payload ->
        assert {:ok, ref} = Pulsar.Producer.send_async(producer, payload)
        ref
      end)

    for ref <- Enum.reverse(refs) do
      assert {:ok, message_id} = Pulsar.Producer.await(ref)
      assert message_id.ledgerId
      assert message_id.entryId
    end

    assert :ok = Pulsar.Producer.stop(producer, client: @client)
  end
end
