defmodule Pulsar.Integration.Consumer.EndOfTopicTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :end_of_topic_test_client
  @consumer_callback Pulsar.Test.Support.DummyConsumer

  setup_all do
    {:ok, _client_pid} =
      Pulsar.Client.start_link(name: @client, host: System.broker().service_url)

    on_exit(fn -> Pulsar.Client.stop(@client) end)

    :ok
  end

  # When a topic is terminated, the broker notifies a caught-up consumer with
  # `CommandReachedEndOfTopic`. Reaching the end of a topic is informational, so
  # the consumer must keep running and its broker connection must stay healthy.
  test "consumer survives reaching the end of a terminated topic" do
    topic = "persistent://public/default/end-of-topic-#{:erlang.unique_integer([:positive])}"
    System.create_topic(topic)

    {:ok, group} =
      Pulsar.start_consumer(topic, "end-of-topic-sub", @consumer_callback,
        client: @client,
        initial_position: :earliest
      )

    messages = for i <- 1..3, do: {"key-#{i}", "message-#{i}"}
    System.produce_messages(topic, messages)

    assert :ok = Utils.wait_for(fn -> total_consumed(group) == length(messages) end)

    [consumer_pid] = Pulsar.get_consumers(group)
    ref = Process.monitor(consumer_pid)

    System.terminate_topic(topic)

    # The consumer must not be taken down by the end-of-topic notification.
    refute_receive {:DOWN, ^ref, :process, ^consumer_pid, _reason}, 3_000
    assert Process.alive?(consumer_pid)

    :ok = Pulsar.stop_consumer(group)
  end

  defp total_consumed(group) do
    group
    |> Pulsar.get_consumers()
    |> Enum.filter(&is_pid/1)
    |> Enum.reduce(0, fn pid, acc ->
      try do
        @consumer_callback.count_messages(pid) + acc
      catch
        :exit, _reason -> acc
      end
    end)
  end
end
