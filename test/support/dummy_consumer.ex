defmodule Pulsar.Test.Support.DummyConsumer do
  @behaviour Pulsar.Consumer.Callback

  def init(_opts) do
    {:ok, %{messages: [], count: 0}}
  end

  def handle_message({command, metadata, {_single_metadata, payload}, _broker_metadata}, state) do
    # Build a message structure similar to the old format for compatibility
    message = %{
      id: {command.message_id.ledgerId, command.message_id.entryId},
      metadata: metadata,
      payload: payload,
      partition_key: metadata.partition_key,
      producer_name: metadata.producer_name,
      publish_time: metadata.publish_time
    }

    new_state = %{
      state
      | messages: [message | state.messages],
        count: state.count + 1
    }

    {:ok, new_state}
  end

  def get_messages(consumer_pid) do
    GenServer.call(consumer_pid, :get_messages)
  end

  def clear_messages(consumer_pid) do
    GenServer.cast(consumer_pid, :clear_messages)
  end

  def count_messages(consumer_pid) do
    GenServer.call(consumer_pid, :count_messages)
  end

  def get_state(consumer_pid) do
    GenServer.call(consumer_pid, :get_state)
  end

  def handle_call(:get_messages, _from, state) do
    {:reply, Enum.reverse(state.messages), state}
  end

  def handle_call(:count_messages, _from, state) do
    {:reply, state.count, state}
  end

  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  def handle_cast(:clear_messages, _state) do
    {:noreply, %{messages: [], count: 0}}
  end
end
