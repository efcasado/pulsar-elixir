defmodule Pulsar.Integration.Reader.InterruptionTest do
  use Pulsar.Test.Case, async: true

  @topic "persistent://public/default/reader-interruption-test"
  @idle_topic "persistent://public/default/idle-reader-interruption-test"
  @partitioned_topic "persistent://public/default/partitioned-reader-interruption-test"
  @partitions 3

  setup_all do
    :ok = System.create_topic(@idle_topic)
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

  test "leaves unrelated DOWN messages in the enumerating process mailbox" do
    test_pid = self()

    unrelated =
      spawn(fn ->
        receive do
          :stop -> exit(:unrelated_failure)
        end
      end)

    unrelated_ref = Process.monitor(unrelated)

    trigger =
      spawn(fn ->
        [root] =
          Utils.wait_for(fn -> Pulsar.Client.consumers(@client) end,
            until: &match?([_root], &1),
            description: "Reader consumer root to start"
          )

        :ok = Pulsar.Consumer.await_ready(root)
        send(unrelated, :stop)
        send(test_pid, :unrelated_stopped)
      end)

    on_exit(fn ->
      if Process.alive?(trigger), do: Process.exit(trigger, :kill)
      if Process.alive?(unrelated), do: Process.exit(unrelated, :kill)
    end)

    assert @idle_topic
           |> Pulsar.Reader.stream(client: @client, timeout: 500)
           |> Enum.to_list() == []

    assert_receive :unrelated_stopped
    assert_receive {:DOWN, ^unrelated_ref, :process, ^unrelated, :unrelated_failure}
  end

  test "flushes monitors added while recursively awaiting a message" do
    test_pid = self()

    {reader, reader_ref} =
      start_paused_reader(@topic, fn ->
        send(test_pid, {:reader_waiting, self()})

        receive do
          :finish -> :ok
        end
      end)

    assert_receive {:reader_message, ^reader, %{payload: "message"}}, 5_000

    [root] = Pulsar.Client.consumers(@client)
    [worker] = Topology.workers(root)
    %{callback_state: %{reader_ref: callback_ref}} = :sys.get_state(worker)

    late_worker =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    replacement =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    late_topic = "#{@topic}-partition-late"

    on_exit(fn ->
      if Process.alive?(late_worker), do: send(late_worker, :stop)
      if Process.alive?(replacement), do: send(replacement, :stop)
    end)

    send(reader, {:pulsar_reader_ready, callback_ref, late_worker, late_topic})
    send(reader, {:pulsar_reader_ready, callback_ref, replacement, late_topic})
    send(reader, :continue)

    assert_interrupted(reader, late_topic)
    assert_receive {:reader_waiting, ^reader}, 5_000
    assert {:monitors, monitors} = Process.info(reader, :monitors)
    refute {:process, late_worker} in monitors

    send(reader, :finish)
    assert_receive {:DOWN, ^reader_ref, :process, ^reader, :normal}, 5_000
  end

  defp start_paused_reader(topic, after_interruption \\ fn -> :ok end) do
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
          error in RuntimeError ->
            send(test_pid, {:reader_interrupted, self(), error})
            after_interruption.()
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
