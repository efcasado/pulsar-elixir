defmodule Pulsar.Test.Support.BrokerStub do
  @moduledoc """
  Answers the `:gen_statem.call` `Pulsar.Broker.publish_message/2` makes, and hands the frame to
  the test process so it can be decoded.
  """

  use GenServer

  alias Pulsar.Protocol

  @doc """
  Starts a stub that accepts everything, or one that refuses the publishes whose zero-based
  position is in `refuse`.
  """
  def start_link({notify_pid, refuse}), do: GenServer.start_link(__MODULE__, {notify_pid, refuse})
  def start_link(notify_pid), do: start_link({notify_pid, []})

  @doc """
  Every frame published so far, decoded, oldest first.
  """
  def published(acc \\ []) do
    receive do
      {:published, frame} ->
        {:ok, {command, metadata, payload, _broker_metadata}} = Protocol.decode(frame)
        published([%{command: command, metadata: metadata, payload: payload} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  @impl true
  def init({notify_pid, refuse}), do: {:ok, %{notify_pid: notify_pid, refuse: refuse, attempts: 0}}

  @impl true
  def handle_call({:publish_message, frame}, _from, %{attempts: attempt} = state) do
    state = %{state | attempts: attempt + 1}

    if attempt in state.refuse do
      {:reply, {:error, :message_too_large}, state}
    else
      send(state.notify_pid, {:published, frame})
      {:reply, :ok, state}
    end
  end
end
