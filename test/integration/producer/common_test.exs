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

    stats = Utils.collect_producer_opened_stats(producer_names: [producer_group_name])
    assert %{success_count: 1, failure_count: 0, total_count: 1} = stats

    assert {:ok, message_id_data} = Pulsar.Producer.send(producer_group_name, "Hello, Pulsar!", client: @client)

    assert message_id_data.ledgerId
    assert message_id_data.entryId

    assert {:ok, _message_id_data2} = Pulsar.Producer.send(group_pid, "Another message with pid!")

    publish_stats = Utils.collect_message_published_stats(producer_names: [producer_group_name])
    assert %{total_count: 2} = publish_stats

    assert :ok = Pulsar.Producer.stop(group_pid)
    Utils.wait_for(fn -> not Process.alive?(producer) end)

    close_stats = Utils.collect_producer_closed_stats(producer_names: [producer_group_name])
    assert %{success_count: 1, failure_count: 0, total_count: 1} = close_stats
  end
end
