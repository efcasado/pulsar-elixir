defmodule Pulsar.Test.Support.DummyConsumer do
  @moduledoc false
  @behaviour Pulsar.Consumer.Callback

  def init(opts) do
    fail_all = Keyword.get(opts, :fail_all, false)
    {:ok, %{messages: [], count: 0, fail_all: fail_all}}
  end

  def handle_message({command, metadata, {_single_metadata, payload}, _broker_metadata}, state) do
    # Build a message structure similar to the old format for compatibility
    message = %{
      id: {command.message_id.ledgerId, command.message_id.entryId},
      metadata: metadata,
      payload: payload,
      partition_key: metadata.partition_key,
      producer_name: metadata.producer_name,
      publish_time: metadata.publish_time,
      redelivery_count: command.redelivery_count
    }

    new_state = %{
      state
      | messages: [message | state.messages],
        count: state.count + 1
    }

    # Fail all messages if fail_all is true
    if state.fail_all do
      {:error, :intentional_failure, new_state}
    else
      {:ok, new_state}
    end
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

  def handle_cast(:clear_messages, state) do
    {:noreply, %{messages: [], count: 0, fail_all: state.fail_all}}
  end
end
