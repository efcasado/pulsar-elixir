defmodule Pulsar.Test.Support.DummyConsumer do
  @moduledoc false
  use Pulsar.Consumer.Callback

  def init(opts, context) do
    fail_all = Keyword.get(opts, :fail_all, false)

    if notify_pid = Keyword.get(opts, :notify_pid) do
      send(notify_pid, {:consumer_ready, self()})
    end

    {:ok, %{messages: [], count: 0, fail_all: fail_all, is_active: false, context: context}}
  end

  def handle_message(%Pulsar.Message{chunk_metadata: %{chunked: true, complete: false}}, state) do
    {:error, :incomplete_chunk, state}
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

  # Opts in to invalid messages so tests can assert on them; the default drops them.
  def handle_invalid_message(%Pulsar.Message{} = message, state) do
    {:ok, %{state | messages: [message | state.messages], count: state.count + 1}}
  end

  def get_messages(consumer_pid) do
    GenServer.call(consumer_pid, :get_messages)
  end

  def count_messages(consumer_pid) do
    GenServer.call(consumer_pid, :count_messages)
  end

  def context(consumer_pid) do
    GenServer.call(consumer_pid, :context)
  end

  def active?(consumer_pid) do
    GenServer.call(consumer_pid, :active?)
  end

  def became_active(state) do
    {:noreply, %{state | is_active: true}}
  end

  def became_passive(state) do
    {:noreply, %{state | is_active: false}}
  end

  def handle_call(:active?, _from, state) do
    {:reply, state.is_active, state}
  end

  def handle_call(:get_messages, _from, state) do
    {:reply, Enum.reverse(state.messages), state}
  end

  def handle_call(:count_messages, _from, state) do
    {:reply, state.count, state}
  end

  def handle_call(:context, _from, state) do
    {:reply, state.context, state}
  end
end
