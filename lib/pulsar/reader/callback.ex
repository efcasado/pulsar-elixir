defmodule Pulsar.Reader.Callback do
  @moduledoc """
  Internal callback module for Pulsar.Reader.

  Implements the `Pulsar.Consumer.Callback` behaviour, forwarding messages to the reader's
  stream process, and hosts the flow policy that reports what each delivery cost.
  """

  use Pulsar.Consumer.Callback

  @impl true
  def init([stream_pid, reader_ref], context) do
    send(stream_pid, {:pulsar_reader_ready, reader_ref, self(), context.topic})
    {:ok, %{stream_pid: stream_pid, reader_ref: reader_ref}}
  end

  @impl true
  def handle_message(message, state) do
    send(state.stream_pid, {:pulsar_message, state.reader_ref, self(), message})
    {:ok, state}
  end

  @doc """
  Flow policy for a reader's consumer, configured as
  `{Pulsar.Reader.Callback, :report_permits, [stream_pid, reader_ref]}`.

  Reports rather than grants: the stream process refills as it consumes, which is what keeps
  the broker from sending further ahead than the stream has read.
  """
  @spec report_permits(map(), pid(), reference()) :: :ok
  def report_permits(%{consumed: 0}, _stream_pid, _reader_ref), do: :ok

  def report_permits(%{consumed: consumed}, stream_pid, reader_ref) do
    send(stream_pid, {:pulsar_permits, reader_ref, self(), consumed})
    :ok
  end
end
