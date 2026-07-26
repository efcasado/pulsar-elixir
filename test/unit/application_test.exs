defmodule Pulsar.ApplicationTest do
  use ExUnit.Case, async: true

  test "starts only the supervisor that runtime clients are started into" do
    # Boot must not read configuration or open sockets: :pulsar starts before the host
    # application, so anything started here would dispatch into a host that is not ready.
    children = Supervisor.which_children(Pulsar)

    assert [{Pulsar.Supervisor, pid, :supervisor, [DynamicSupervisor]}] = children
    assert is_pid(pid)
  end
end
