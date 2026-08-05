defmodule Pulsar.Test.Support.BrokerStub do
  @moduledoc """
  Answers the `:gen_statem.call` `Pulsar.Broker.publish_message/2` makes, and hands the frame to
  the test process so it can be decoded.
  """

  use GenServer

  alias Pulsar.Protocol

  def start_link(notify_pid), do: GenServer.start_link(__MODULE__, notify_pid)

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
  def init(notify_pid), do: {:ok, notify_pid}

  @impl true
  def handle_call({:publish_message, frame}, _from, notify_pid) do
    send(notify_pid, {:published, frame})
    {:reply, :ok, notify_pid}
  end
end
