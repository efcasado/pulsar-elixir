defmodule Pulsar.Test.Support.Flow do
  @moduledoc """
  Flow policies for tests that grant permits themselves.
  """

  @doc """
  Grants nothing, leaving every permit to `Pulsar.Consumer.send_flow/3`.
  """
  @spec never(map()) :: :ok
  def never(_flow), do: :ok

  @doc """
  Grants nothing and reports what the delivery cost to `notify_pid`.
  """
  @spec report(map(), pid()) :: :ok
  def report(flow, notify_pid) do
    send(notify_pid, {:permits, flow})
    :ok
  end

  @doc """
  Reports what the delivery cost to `notify_pid`, then always grants `permits`.
  """
  @spec grant_fixed(map(), pid(), pos_integer()) :: {:grant, pos_integer()}
  def grant_fixed(flow, notify_pid, permits) do
    send(notify_pid, {:permits, flow})
    {:grant, permits}
  end
end
