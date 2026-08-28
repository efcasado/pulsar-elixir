defmodule Pulsar.Integration.Reader.FlowControlTest do
  use Pulsar.Test.Case, async: true

  @topic "persistent://public/default/reader-flow-control-test"
  @num_messages 20
  @flow_control [:pulsar, :consumer, :flow_control, :stop]

  setup_all do
    Utils.seed_topic(@topic, Enum.map(1..@num_messages, &"Message #{&1}"), client: @client)

    :ok
  end

  @tag telemetry_listen: [@flow_control]
  test "tops the window up as it is spent, when it is smaller than the topic" do
    messages =
      @topic
      |> Pulsar.Reader.stream(client: @client, flow_permits: 5, timeout: 2_000)
      |> Enum.to_list()

    assert length(messages) == @num_messages

    consumer_id = hd(messages).raw.command.consumer_id

    # Five up front, then five back each time the window empties.
    for _grant <- 1..5, do: assert_granted(consumer_id, 5)
    refute_granted(consumer_id)
  end

  @tag telemetry_listen: [@flow_control]
  test "a window of one is topped up on every message" do
    messages =
      @topic
      |> Pulsar.Reader.stream(client: @client, flow_permits: 1, timeout: 2_000)
      |> Enum.to_list()

    assert length(messages) == @num_messages

    consumer_id = hd(messages).raw.command.consumer_id

    # One up front and one back per message, so a grant per message and one over.
    for _grant <- 1..(@num_messages + 1), do: assert_granted(consumer_id, 1)
    refute_granted(consumer_id)
  end

  @tag telemetry_listen: [@flow_control]
  test "a window wider than the topic is never topped up" do
    messages =
      @topic
      |> Pulsar.Reader.stream(client: @client, flow_permits: 1000, timeout: 2_000)
      |> Enum.to_list()

    assert length(messages) == @num_messages

    consumer_id = hd(messages).raw.command.consumer_id

    # The window never falls far enough to be topped up, so the initial grant is the only one.
    assert_granted(consumer_id, 1000)
    refute_granted(consumer_id)
  end

  test "refills while assembling a chunked message wider than its window" do
    topic = @topic <> "-chunked"
    payload = String.duplicate("chunked-reader-", 10)

    {:ok, producer} =
      Pulsar.Producer.start(
        topic,
        client: @client,
        name: :reader_flow_chunked_producer,
        chunking_enabled: true,
        max_message_size: 8
      )

    :ok = Pulsar.Producer.await_ready(producer)
    assert {:ok, %{num_chunks: num_chunks}} = Pulsar.Producer.send(producer, payload)
    assert num_chunks > 5

    assert [%{payload: ^payload}] =
             topic
             |> Pulsar.Reader.stream(client: @client, flow_permits: 5, timeout: 2_000)
             |> Enum.take(1)
  end

  # Every test listening for this event is sent every consumer's, so the id picks out ours.
  defp assert_granted(consumer_id, permits) do
    assert_receive {:telemetry_event,
                    %{
                      event: @flow_control,
                      metadata: %{consumer_id: ^consumer_id, permits_requested: ^permits}
                    }}
  end

  defp refute_granted(consumer_id) do
    refute_receive {:telemetry_event, %{event: @flow_control, metadata: %{consumer_id: ^consumer_id}}}
  end
end
