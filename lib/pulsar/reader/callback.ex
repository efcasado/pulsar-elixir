defmodule Pulsar.Reader.Callback do
  @moduledoc """
  Internal callback module for Pulsar.Reader.

  This module handles messages from the consumer and forwards them to the
  reader's stream process. It implements the `Pulsar.Consumer.Callback`
  behaviour.
  """

  use Pulsar.Consumer.Callback

  @impl true
  def init([stream_pid, reader_ref]) do
    send(stream_pid, {:reader_ready, reader_ref, self()})
    {:ok, %{stream_pid: stream_pid, reader_ref: reader_ref}}
  end

  @impl true
  def handle_message(message, state) do
    send(state.stream_pid, {:pulsar_message, state.reader_ref, self(), message})
    {:ok, state}
  end
end
