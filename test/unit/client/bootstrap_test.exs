defmodule Pulsar.Client.BootstrapTest do
  use ExUnit.Case, async: true

  alias Pulsar.Client
  alias Pulsar.Client.Bootstrap
  alias Pulsar.Producer
  alias Pulsar.Test.Support.Utils

  @host "pulsar://127.0.0.1:1"

  test "accepts an already-started resource owned by the current resource supervisor" do
    client = :bootstrap_owned_resource
    opts = [topic: "owned", name: :owned, client: client]

    start_supervised!({Client, name: client, host: @host})
    {:ok, producer} = Producer.start(opts)

    bootstrap = start_supervised!({Bootstrap, {:producers, client_opts(client, opts)}})

    assert %{pending: [], backoff: 0} = :sys.get_state(bootstrap)
    assert Client.producers(client) == [producer]
  end

  test "retries an already-started resource not owned by the current resource supervisor" do
    client = :bootstrap_unowned_resource
    opts = [topic: "unowned", name: :unowned, client: client]

    start_supervised!({Client, name: client, host: @host})
    outsider = start_supervised!({Producer, opts})
    start_supervised!({Bootstrap, {:producers, client_opts(client, opts)}})

    assert Client.producers(client) == []

    stop_supervised(Producer.child_spec(opts).id)

    [replacement] =
      Utils.wait_for(fn -> Client.producers(client) end,
        until: &match?([_pid], &1),
        description: "the unowned name to be released and retried"
      )

    refute replacement == outsider
  end

  defp client_opts(client, producer) do
    [name: client, host: @host, producers: [producer]]
  end
end
