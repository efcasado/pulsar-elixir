defmodule Pulsar.Integration.Producer.CommonTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.System

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

  test "send returns error when producer not found" do
    assert {:error, :producer_not_found} = Pulsar.send("non-existent-producer-group", "message", client: @client)
  end
end
