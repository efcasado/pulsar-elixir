defmodule Pulsar.Integration.Producer.CommonTest do
  use ExUnit.Case, async: true

  import TelemetryTest

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  require Logger

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
    assert {:error, :producer_not_found} =
             Pulsar.send("non-existent-producer-group", "message", client: @client)
  end

  @tag telemetry_listen: [
         [:pulsar, :producer, :opened, :stop],
         [:pulsar, :producer, :closed, :stop],
         [:pulsar, :producer, :message, :published]
       ]
  test "create and send message" do
    producer_group_name = "my-producer"

    assert {:ok, group_pid} =
             Pulsar.start_producer(@topic <> "-lifecycle",
               client: @client,
               name: producer_group_name
             )

    [producer] = Pulsar.get_producers(group_pid)

    :ok =
      Utils.wait_for(fn ->
        state = :sys.get_state(producer)
        state.producer_name != nil
      end)

    stats = Utils.collect_producer_opened_stats()
    assert %{success_count: 1, failure_count: 0, total_count: 1} = stats

    assert {:ok, message_id_data} = Pulsar.send(producer_group_name, "Hello, Pulsar!", client: @client)

    assert message_id_data.ledgerId
    assert message_id_data.entryId

    assert {:ok, _message_id_data2} = Pulsar.send(group_pid, "Another message with pid!")

    publish_stats = Utils.collect_message_published_stats()
    assert %{total_count: 2} = publish_stats

    assert :ok = Pulsar.stop_producer(group_pid)
    Utils.wait_for(fn -> not Process.alive?(producer) end)

    close_stats = Utils.collect_producer_closed_stats()
    assert %{success_count: 1, failure_count: 0, total_count: 1} = close_stats
  end
end
