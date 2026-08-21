defmodule Pulsar.Test.Case do
  @moduledoc """
  The environment integration tests run in: a client of their own, connected to the
  docker-compose cluster and stopped again when the module's tests are done.

  The client is named after the test module, so modules running together cannot be taken for
  one another in a log line or a telemetry event. Its name is `@client`, which is what
  `:client` wants throughout `Pulsar`'s API, and its pid and broker are in the context.

  Tests that manage their own clients, such as the ones covering what a client declares, use
  `ExUnit.Case` directly instead.
  """

  use ExUnit.CaseTemplate

  import TelemetryTest

  alias Pulsar.Test.Support.System

  using do
    quote do
      import TelemetryTest

      alias Pulsar.Test.Support.System
      alias Pulsar.Test.Support.Utils
      alias Pulsar.Topology

      @moduletag :integration
      @client __MODULE__
    end
  end

  setup_all context do
    broker = System.broker()
    # Escalation is by exhaustion, and the defaults spend a few hundred round trips getting there.
    # A handful per second exhausts on the instant retries a misconfiguration produces, and leaves
    # the seconds-apart restarts of a reconnect well inside the window.
    {:ok, _client} =
      Pulsar.Client.start_link(
        name: context.module,
        host: broker.service_url,
        worker_restart_intensity: [max_restarts: 3, max_seconds: 1],
        resource_restart_intensity: [max_restarts: 3, max_seconds: 1]
      )

    on_exit(fn -> Pulsar.Client.stop(context.module) end)

    {:ok, client: context.module, broker: broker}
  end

  setup [:telemetry_listen]
end
