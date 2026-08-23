defmodule Pulsar.Test.Support.DummyConsumer do
  @moduledoc """
  A callback that forwards what it is given to a test process, and holds nothing of its own.

  With `forward_to: self()` in its `:init_args` it sends that process:

    * `{:consumer_started, pid, context}` once it has initialized
    * `{:consumer, pid, message}` for every message, valid or not
    * `{:consumer_active, pid, active?}` when the broker makes it the active consumer of a
      `:failover` subscription, or takes that over
    * `{:consumer_end_of_topic, pid}` when it has drained a terminated topic

  `pid` is the worker itself, which is what tells the consumers of one topic apart. A test
  asserts on deliveries with `assert_receive`, and on the absence of one with `refute_receive`,
  rather than asking the callback what it has collected.

  The only thing it decides for itself is what to answer with: `fail_all: true` rejects every
  message, so redelivery and dead lettering have something to act on, and
  `finish_at_end_of_topic: true` finishes only the worker that receives the notification.
  `stop_at_end_of_topic: true` stops the whole consumer once a worker reaches the end of one,
  through `Pulsar.Consumer.stop/2`, the way a coordinator would.

  A consumer started in `setup_all` cannot forward to the test that asserts on it, since that
  callback runs in its own process. Either start it in the test, or point it at the right one
  with `register/2`.
  """
  use Pulsar.Consumer.Callback

  def init(opts, context) do
    forward_to = Keyword.get(opts, :forward_to)
    notify(forward_to, {:consumer_started, self(), context})

    {:ok,
     %{
       forward_to: forward_to,
       fail_all: Keyword.get(opts, :fail_all, false),
       finish_at_end_of_topic: Keyword.get(opts, :finish_at_end_of_topic, false),
       stop_at_end_of_topic: Keyword.get(opts, :stop_at_end_of_topic, false)
     }}
  end

  @doc """
  Points a running consumer at `pid`, for one started somewhere that cannot receive from it.
  """
  def register(consumer_pid, pid), do: GenServer.call(consumer_pid, {:forward_to, pid})

  def handle_message(%Pulsar.Message{} = message, state) do
    notify(state.forward_to, {:consumer, self(), message})

    if state.fail_all do
      {:error, :intentional_failure, state}
    else
      {:ok, state}
    end
  end

  # Opts in to invalid messages so tests can assert on them; the default drops them.
  def handle_invalid_message(%Pulsar.Message{} = message, state) do
    notify(state.forward_to, {:consumer, self(), message})

    {:ok, state}
  end

  def became_active(state) do
    notify(state.forward_to, {:consumer_active, self(), true})

    {:ok, state}
  end

  def became_passive(state) do
    notify(state.forward_to, {:consumer_active, self(), false})

    {:ok, state}
  end

  def reached_end_of_topic(state) do
    notify(state.forward_to, {:consumer_end_of_topic, self()})

    cond do
      state.finish_at_end_of_topic ->
        {:stop, :normal, state}

      state.stop_at_end_of_topic ->
        root = self() |> Pulsar.Topology.owning_supervisor() |> Pulsar.Topology.owning_supervisor()
        Task.start(fn -> Pulsar.Consumer.stop(root) end)
        {:ok, state}

      true ->
        {:ok, state}
    end
  end

  def terminate(reason, state) do
    notify(state.forward_to, {:consumer_terminated, self(), reason})
  end

  def handle_call({:forward_to, pid}, _from, state) do
    {:reply, :ok, %{state | forward_to: pid}}
  end

  defp notify(nil, _message), do: :ok
  defp notify(pid, message), do: send(pid, message)
end
