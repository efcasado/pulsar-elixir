defmodule Pulsar.Utils do
  def broker() do
    {_id, pid, _, _} =
      Pulsar.BrokerSupervisor
      |> Supervisor.which_children()
      |> Enum.random()

    pid
  end
end
