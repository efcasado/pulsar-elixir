defmodule Pulsar.Test.Support.DummyConsumer do
  @moduledoc false
  use Pulsar.Consumer.Callback

  def init(opts) do
    fail_all = Keyword.get(opts, :fail_all, false)

    if notify_pid = Keyword.get(opts, :notify_pid) do
      send(notify_pid, {:consumer_ready, self()})
    end

    {:ok, %{messages: [], count: 0, fail_all: fail_all}}
  end

  def handle_message(%Pulsar.Message{} = message, state) do
    new_state = %{
      state
      | messages: [message | state.messages],
        count: state.count + 1
    }

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
