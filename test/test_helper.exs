alias Pulsar.Test.Support.System

Logger.configure(level: :info)

Application.put_env(:junit_formatter, :report_dir, "test/reports")
Application.put_env(:junit_formatter, :report_file, "junit.xml")
Application.put_env(:junit_formatter, :automatic_create_dir?, true)

Application.put_env(:pulsar_elixir, :startup_delay_ms, 100)
Application.put_env(:pulsar_elixir, :startup_jitter_ms, 100)

Application.ensure_all_started(:telemetry_test)

:ok = System.start_pulsar()

ExUnit.configure(formatters: [ExUnit.CLIFormatter, JUnitFormatter])
ExUnit.start()

ExUnit.after_suite(fn _result ->
  :ok = System.stop_pulsar()
end)
