defmodule Pulsar.Integration.Consumer.AckTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :ack_test_client

  # Hands each message to the test process and acknowledges nothing
  # automatically, so the test can drive a manual (cumulative) ack.
  defmodule ManualAckConsumer do
    @moduledoc false
    use Pulsar.Consumer.Callback

    def init(opts), do: {:ok, Keyword.fetch!(opts, :notify_pid)}

    def handle_message(%Pulsar.Message{} = message, notify_pid) do
      send(notify_pid, {:message, message})
      {:noreply, notify_pid}
    end
  end

  setup_all do
    {:ok, _client_pid} =
      Pulsar.Client.start_link(name: @client, host: System.broker().service_url)

    on_exit(fn -> Pulsar.Client.stop(@client) end)

    :ok
  end

  test "cumulative ack advances the cursor past all earlier messages" do
    topic = "persistent://public/default/cumulative-ack-#{:erlang.unique_integer([:positive])}"
    subscription = "cumulative-sub"
    System.create_topic(topic)

    {:ok, group} =
      Pulsar.start_consumer(topic, subscription, ManualAckConsumer,
        client: @client,
        subscription_type: :Exclusive,
        ack_type: :Cumulative,
        initial_position: :earliest,
        init_args: [notify_pid: self()]
      )

    System.produce_messages(topic, for(i <- 1..5, do: {"key-#{i}", "message-#{i}"}))

    # Collect the 5 messages in delivery order (Exclusive preserves order),
    # acknowledging none automatically.
    messages = for _ <- 1..5, do: assert_receive({:message, %Pulsar.Message{} = m}, 5_000) && m
    assert Enum.map(messages, & &1.payload) == for(i <- 1..5, do: "message-#{i}")

    # Cumulatively ack the 3rd message: this marks 1, 2 and 3 as consumed, so the
    # backlog drops to 2. With an Individual ack only message 3 would clear,
    # leaving a backlog of 4.
    [consumer_pid] = Pulsar.get_consumers(group)
    third = Enum.at(messages, 2)
    :ok = Pulsar.Consumer.ack(consumer_pid, third.message_id_to_ack)

    assert :ok = Utils.wait_for(fn -> System.subscription_backlog(topic, subscription) == 2 end)

    :ok = Pulsar.stop_consumer(group)
  end
end
