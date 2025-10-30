defmodule Pulsar.Utils do
  @moduledoc false
  def broker do
    {_id, pid, _, _} =
      Pulsar.BrokerSupervisor
      |> Supervisor.which_children()
      |> Enum.random()

    pid
  end
end
