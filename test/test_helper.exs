alias Pulsar.Test.Support.System

Logger.configure(level: :info)

Application.ensure_all_started(:telemetry_test)

:ok = System.start_pulsar()

ExUnit.start()

ExUnit.after_suite(fn _result ->
  :ok = System.stop_pulsar()
end)
