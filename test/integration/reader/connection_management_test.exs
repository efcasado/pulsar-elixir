defmodule Pulsar.Integration.Reader.ConnectionManagementTest do
  use Pulsar.Test.Case, async: true

  @topic "persistent://public/default/reader-connection-management-test"
  @num_messages 20

  setup_all do
    Utils.seed_topic(@topic, Enum.map(1..@num_messages, &"Message #{&1}"), client: @client)

    :ok
  end

  test "stream with external client" do
    result =
      @topic
      |> Pulsar.Reader.stream(client: @client)
      |> Enum.take(5)

    assert length(result) == 5
  end

  test "two clients cannot share a name", %{broker: broker} do
    shared_name = :"reader_conflict_#{:erlang.unique_integer([:positive])}"

    {:ok, _pid} = Pulsar.Client.start_link(name: shared_name, host: broker.service_url)

    assert {:error, {:already_started, _}} =
             Pulsar.Client.start_link(name: shared_name, host: broker.service_url)

    # Cleanup
    Pulsar.Client.stop(shared_name)
  end

  test "reports a client that is not running instead of starting one" do
    assert [{:error, reason}] =
             "persistent://public/default/reader-no-client"
             |> Pulsar.Reader.stream(client: :never_started, timeout: 100)
             |> Enum.take(1)

    assert reason
  end

  test "stream cleanup on halt" do
    timeout_ms = 100

    result =
      @topic
      |> Pulsar.Reader.stream(
        client: @client,
        timeout: timeout_ms
      )
      |> Enum.to_list()

    assert length(result) == @num_messages

    Utils.wait_for(fn -> Pulsar.Client.consumers(@client) == [] end)
  end
end
