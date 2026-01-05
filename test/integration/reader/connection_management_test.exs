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
    result =
      @topic
      |> Pulsar.Reader.stream(client: @client)
      |> Enum.take(5)

    assert length(result) == 5
  end

  test "stream with internal client", %{broker: broker} do
    client_name = :"reader_internal_#{:erlang.unique_integer([:positive])}"

    result =
      @topic
      |> Pulsar.Reader.stream(
        host: broker.service_url,
        name: client_name
      )
      |> Enum.take(5)

    assert length(result) == 5
  end

  test "multiple streams with internal clients using different names", %{broker: broker} do
    name1 = :"reader_multi_1_#{:erlang.unique_integer([:positive])}"
    name2 = :"reader_multi_2_#{:erlang.unique_integer([:positive])}"

    result1 =
      @topic
      |> Pulsar.Reader.stream(
        host: broker.service_url,
        name: name1
      )
      |> Enum.take(5)

    result2 =
      @topic
      |> Pulsar.Reader.stream(
        host: broker.service_url,
        name: name2
      )
      |> Enum.take(5)

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

    result =
      @topic
      |> Pulsar.Reader.stream(
        client: @client,
        timeout: timeout_ms
      )
      |> Enum.to_list()

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

    result =
      invalid_topic
      |> Pulsar.Reader.stream(
        host: broker.service_url,
        name: client_name
      )
      |> Enum.take(1)

    assert [{:error, _reason}] = result

    assert Process.whereis(client_name) == nil
  end
end
