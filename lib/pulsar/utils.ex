defmodule Pulsar.Utils do
  @moduledoc false

  @default_client :default

  @doc """
  Returns a random broker process from the specified client's broker supervisor.

  Defaults to the `:default` client if no client is specified.
  """
  def broker(client \\ @default_client) do
    broker_supervisor = Pulsar.Client.broker_supervisor(client)

    {_id, pid, _, _} =
      broker_supervisor
      |> Supervisor.which_children()
      |> Enum.random()

    pid
  end
end
