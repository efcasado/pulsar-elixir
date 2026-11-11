defmodule Pulsar.Integration.Producer.CommonTest do
  use Pulsar.Test.Case, async: true

  @topic "persistent://public/default/producer-common-test"
  @opened [:pulsar, :producer, :opened, :stop]
  @closed [:pulsar, :producer, :closed, :stop]
  @published [:pulsar, :producer, :message, :published]

  setup_all do
    :ok = System.create_topic(@topic)
  end

  test "reports a producer name nothing is registered under" do
    assert {:error, :not_found} =
             Pulsar.Producer.send("non-existent-producer-group", "message", client: @client)
  end

  test "await_ready/2 waits for the producer to register, and honours its timeout" do
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

  # Two clients stand in for two nodes running the same producer: the broker admits one
  # producer per name on a topic, so the default name must not be all it sees.
  test "a producer with the default name runs on several nodes at once", %{broker: broker} do
    other_client = :"#{@client}.other_node"
    start_supervised!({Pulsar.Client, name: other_client, host: broker.service_url})

    producers =
      for client <- [@client, other_client] do
        {:ok, producer} = Pulsar.Producer.start(@topic <> "-default-name", client: client)
        on_exit(fn -> Pulsar.Producer.stop(producer) end)
        producer
      end

    for {producer, client} <- Enum.zip(producers, [@client, other_client]) do
      assert :ok = Pulsar.Producer.await_ready(producer)

      # Still found locally by the default name.
      assert {:ok, _message_id} =
               Pulsar.Producer.send("#{@topic}-default-name-producer", "hello", client: client)
    end

    names = for producer <- producers, [worker] = Topology.workers(producer), do: :sys.get_state(worker).producer_name
    assert names == Enum.uniq(names)
  end

  @tag telemetry_listen: [@opened, @closed, @published]
  test "reports opening, publishing and closing as it goes" do
    producer_group_name = "my-producer"

    assert {:ok, group_pid} =
             Pulsar.Producer.start(@topic <> "-lifecycle",
               client: @client,
               name: producer_group_name
             )

    :ok = Pulsar.Producer.await_ready(group_pid)
    [producer] = Topology.workers(group_pid)
    worker_name = :sys.get_state(producer).producer_name

    assert_receive {:telemetry_event, %{event: @opened, metadata: %{success: true, producer_name: ^worker_name}}}

    refute_receive {:telemetry_event, %{event: @opened, metadata: %{producer_name: ^worker_name}}}

    assert {:ok, message_id_data} = Pulsar.Producer.send(producer_group_name, "Hello, Pulsar!", client: @client)

    assert message_id_data.ledgerId
    assert message_id_data.entryId

    assert {:ok, _message_id_data2} = Pulsar.Producer.send(group_pid, "Another message with pid!")

    for _published <- 1..2 do
      assert_receive {:telemetry_event, %{event: @published, metadata: %{producer_name: ^worker_name}}}
    end

    refute_receive {:telemetry_event, %{event: @published, metadata: %{producer_name: ^worker_name}}}

    ref = Process.monitor(producer)
    assert :ok = Pulsar.Producer.stop(group_pid)
    assert_receive {:DOWN, ^ref, :process, ^producer, _reason}

    assert_receive {:telemetry_event, %{event: @closed, metadata: %{success: true, producer_name: ^worker_name}}}

    refute_receive {:telemetry_event, %{event: @closed, metadata: %{producer_name: ^worker_name}}}
  end

  test "send_async/3 answers every caller, whatever order they are awaited in" do
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
