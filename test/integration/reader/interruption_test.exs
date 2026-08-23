defmodule Pulsar.Integration.Reader.InterruptionTest do
  use Pulsar.Test.Case, async: true

  @topic "persistent://public/default/reader-interruption-test"
  @partitioned_topic "persistent://public/default/partitioned-reader-interruption-test"
  @partitions 3

  setup_all do
    :ok = System.create_topic(@partitioned_topic, @partitions)
    Utils.seed_topic(@topic, ["message"], client: @client)
    Utils.seed_topic(@partitioned_topic, ["message"], client: @client)

    :ok
  end

  test "raises and removes its temporary consumer when its worker exits" do
    {reader, reader_ref} = start_paused_reader(@topic)
    assert_receive {:reader_message, ^reader, %{payload: "message"}}, 5_000

    [root] = Pulsar.Client.consumers(@client)
    [worker] = Topology.workers(root)
    assert Pulsar.Consumer.topic(worker) == @topic

    Process.exit(worker, :kill)
    send(reader, :continue)

    assert_interrupted(reader, @topic)
    assert_receive {:DOWN, ^reader_ref, :process, ^reader, :normal}, 5_000
    assert Pulsar.Client.consumers(@client) == []
  end

  test "losing one partition interrupts the whole Reader" do
    {reader, reader_ref} = start_paused_reader(@partitioned_topic)
    assert_receive {:reader_message, ^reader, %{payload: "message"}}, 5_000

    [root] = Pulsar.Client.consumers(@client)
    [worker | _other_workers] = Topology.workers(root)
    partition_topic = Pulsar.Consumer.topic(worker)

    Process.exit(worker, :kill)
    send(reader, :continue)

    assert_interrupted(reader, partition_topic)
    assert_receive {:DOWN, ^reader_ref, :process, ^reader, :normal}, 5_000
    assert Pulsar.Client.consumers(@client) == []
  end

  defp start_paused_reader(topic) do
    test_pid = self()

    {reader, reader_ref} =
      spawn_monitor(fn ->
        try do
          topic
          |> Pulsar.Reader.stream(client: @client, timeout: :infinity)
          |> Enum.each(fn message ->
            send(test_pid, {:reader_message, self(), message})

            receive do
              :continue -> :ok
            end
          end)
        rescue
          error in RuntimeError -> send(test_pid, {:reader_interrupted, self(), error})
        end
      end)

    on_exit(fn ->
      if Process.alive?(reader), do: Process.exit(reader, :kill)
    end)

    {reader, reader_ref}
  end

  defp assert_interrupted(reader, topic) do
    assert_receive {:reader_interrupted, ^reader, %RuntimeError{message: message}}, 5_000
    assert message =~ "reader worker for #{inspect(topic)} was lost"
    assert message =~ "the non-durable stream cannot continue from a known position"
  end
end
