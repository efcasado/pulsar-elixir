defmodule Pulsar.Integration.Reader.ConnectionManagementTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.System

  @moduletag :integration
  @client :reader_connection_management_test_client
  @topic "persistent://public/default/reader-connection-management-test"
  @num_messages 20

  setup_all do
    broker = System.broker()

    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: broker.service_url
      )

    {:ok, _producer_pid} =
      Pulsar.start_producer(
        @topic,
        client: @client,
        name: :reader_connection_management_test_producer
      )

    for i <- 1..@num_messages do
      payload = "Message #{i}"
      Pulsar.send(:reader_connection_management_test_producer, payload, client: @client)
    end

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)

    {:ok, broker: broker}
  end

  test "stream with external client" do
    {:ok, stream} = Pulsar.Reader.stream(@topic, client: @client)
    result = Enum.take(stream, 5)

    assert length(result) == 5
  end

  test "stream with internal client", %{broker: broker} do
    client_name = :"reader_internal_#{:erlang.unique_integer([:positive])}"

    {:ok, stream} =
      Pulsar.Reader.stream(@topic,
        host: broker.service_url,
        name: client_name
      )

    result = Enum.take(stream, 5)

    assert length(result) == 5
  end

  test "multiple streams with internal clients using different names", %{broker: broker} do
    name1 = :"reader_multi_1_#{:erlang.unique_integer([:positive])}"
    name2 = :"reader_multi_2_#{:erlang.unique_integer([:positive])}"

    {:ok, stream1} =
      Pulsar.Reader.stream(@topic,
        host: broker.service_url,
        name: name1
      )

    result1 = Enum.take(stream1, 5)

    {:ok, stream2} =
      Pulsar.Reader.stream(@topic,
        host: broker.service_url,
        name: name2
      )

    result2 = Enum.take(stream2, 5)

    assert length(result1) == 5
    assert length(result2) == 5
  end

  test "multiple internal clients with same name fails", %{broker: broker} do
    shared_name = :"reader_conflict_#{:erlang.unique_integer([:positive])}"

    {:ok, _pid} = Pulsar.Client.start_link(name: shared_name, host: broker.service_url)

    assert {:error, {:already_started, _}} =
             Pulsar.Client.start_link(name: shared_name, host: broker.service_url)

    # Cleanup
    Pulsar.Client.stop(shared_name)
  end

  test "stream cleanup on halt" do
    timeout_ms = 100

    {:ok, stream} =
      Pulsar.Reader.stream(@topic,
        client: @client,
        timeout: timeout_ms
      )

    result = Enum.to_list(stream)

    assert length(result) == @num_messages

    Process.sleep(timeout_ms * 3)

    remaining =
      @client
      |> Pulsar.Client.consumer_supervisor()
      |> Supervisor.which_children()
      |> Enum.filter(fn {id, _pid, _type, _modules} ->
        id |> to_string() |> String.contains?("reader-connection-management-test")
      end)

    assert remaining == []
  end

  test "internal client is cleaned up on consumer start failure", %{broker: broker} do
    client_name = :"reader_cleanup_on_failure_#{:erlang.unique_integer([:positive])}"
    invalid_topic = "persistent://nonexistent/namespace/topic"

    assert {:error, _reason} =
             Pulsar.Reader.stream(invalid_topic,
               host: broker.service_url,
               name: client_name
             )

    assert Process.whereis(client_name) == nil
  end
end
