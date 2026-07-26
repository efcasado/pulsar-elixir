defmodule Pulsar.Integration.Consumer.ValidationTest do
  use ExUnit.Case, async: true

  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.Test.Support.System
  alias Pulsar.Test.Support.Utils

  @moduletag :integration
  @client :validation_test_client
  @topic "persistent://public/default/validation"
  @consumer_callback Pulsar.Test.Support.DummyConsumer

  setup_all do
    broker = System.broker()

    {:ok, _client_pid} = Pulsar.Client.start_link(name: @client, host: broker.service_url)

    on_exit(fn -> Pulsar.Client.stop(@client) end)

    :ok
  end

  # A consumer per test: both assert on everything the callback collected.
  setup do
    name = "validation-consumer-#{:erlang.unique_integer([:positive])}"

    {:ok, group_pid} =
      Pulsar.start_consumer(
        @topic,
        name,
        @consumer_callback,
        client: @client,
        name: name
      )

    [consumer_pid] = Pulsar.get_consumers(group_pid)

    on_exit(fn -> Pulsar.stop_consumer(group_pid) end)

    %{consumer: consumer_pid}
  end

  test "an unverifiable message reaches the callback flagged rather than being dropped", ctx do
    command = %Binary.CommandMessage{
      consumer_id: 1,
      message_id: %Binary.MessageIdData{ledgerId: 1, entryId: 1}
    }

    send(ctx.consumer, {:broker_message, {:invalid, command, <<255, 255, 255>>, :checksum_mismatch}})

    Utils.wait_for(fn -> @consumer_callback.get_messages(ctx.consumer) != [] end)

    assert [message] = @consumer_callback.get_messages(ctx.consumer)
    refute Pulsar.Message.valid?(message)
    assert message.validation_error == :checksum_mismatch
    assert message.payload == <<255, 255, 255>>
    assert message.metadata == nil
    assert message.message_id_to_ack == command.message_id
  end

  test "a callback that does not opt in never sees it" do
    defmodule PlainConsumer do
      @moduledoc false
      use Pulsar.Consumer.Callback

      def init(notify_pid), do: {:ok, notify_pid}

      def handle_message(message, notify_pid) do
        send(notify_pid, {:handled, message})
        {:ok, notify_pid}
      end
    end

    name = "validation-plain-#{:erlang.unique_integer([:positive])}"

    {:ok, group_pid} =
      Pulsar.start_consumer(@topic, name, PlainConsumer, client: @client, name: name, init_args: self())

    [consumer] = Pulsar.get_consumers(group_pid)
    on_exit(fn -> Pulsar.stop_consumer(group_pid) end)

    command = %Binary.CommandMessage{consumer_id: 1, message_id: %Binary.MessageIdData{ledgerId: 9, entryId: 9}}
    send(consumer, {:broker_message, {:invalid, command, <<255>>, :checksum_mismatch}})

    # handle_message/2 can trust what it is given, so it is never called for this.
    refute_receive {:handled, _message}, 500

    assert Pulsar.Consumer.topic(consumer) == @topic
  end

  test "the consumer survives acknowledging it against a real broker", ctx do
    command = %Binary.CommandMessage{
      consumer_id: 1,
      message_id: %Binary.MessageIdData{ledgerId: 2, entryId: 2}
    }

    send(ctx.consumer, {:broker_message, {:invalid, command, <<0>>, :malformed_message_metadata}})

    # Reaching the callback means the ack went out. Had the broker rejected the
    # validation error on it, it would have closed the connection.
    Utils.wait_for(fn -> @consumer_callback.get_messages(ctx.consumer) != [] end)

    assert Pulsar.Consumer.topic(ctx.consumer) == @topic
    assert Process.alive?(ctx.consumer)
  end
end
