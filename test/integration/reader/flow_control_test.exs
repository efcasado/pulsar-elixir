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
  test "small flow_permits triggers refills" do
    messages =
      @topic
      |> Pulsar.Reader.stream(client: @client, flow_permits: 5, timeout: 100)
      |> Enum.to_list()

    assert length(messages) == @num_messages

    consumer_id = hd(messages).raw.command.consumer_id

    # Five up front, then five back each time the window empties.
    for _grant <- 1..5, do: assert_granted(consumer_id, 5)
    refute_granted(consumer_id)
  end

  @tag telemetry_listen: [@flow_control]
  test "flow_permits of 1 triggers refill on every message" do
    messages =
      @topic
      |> Pulsar.Reader.stream(client: @client, flow_permits: 1, timeout: 100)
      |> Enum.to_list()

    assert length(messages) == @num_messages

    consumer_id = hd(messages).raw.command.consumer_id

    # One up front and one back per message, so a grant per message and one over.
    for _grant <- 1..(@num_messages + 1), do: assert_granted(consumer_id, 1)
    refute_granted(consumer_id)
  end

  @tag telemetry_listen: [@flow_control]
  test "large flow_permits requires only initial request" do
    messages =
      @topic
      |> Pulsar.Reader.stream(client: @client, flow_permits: 1000, timeout: 100)
      |> Enum.to_list()

    assert length(messages) == @num_messages

    consumer_id = hd(messages).raw.command.consumer_id

    # The window never falls far enough to be topped up, so the initial grant is the only one.
    assert_granted(consumer_id, 1000)
    refute_granted(consumer_id)
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
