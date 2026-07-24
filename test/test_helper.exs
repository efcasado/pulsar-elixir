Logger.configure(level: :info)

Application.put_env(:junit_formatter, :report_dir, "test/reports")
Application.put_env(:junit_formatter, :report_file, "junit.xml")
Application.put_env(:junit_formatter, :automatic_create_dir?, true)

Application.put_env(:pulsar, :startup_delay_ms, 100)
Application.put_env(:pulsar, :startup_jitter_ms, 100)
Application.put_env(:pulsar, :partition_discovery_interval_ms, false)

Application.ensure_all_started(:telemetry_test)

ExUnit.configure(formatters: [ExUnit.CLIFormatter, JUnitFormatter])
ExUnit.start()

# The CLI tag filters are already applied when this file loads, so we can skip
# the (slow) docker-compose cluster when no integration test is going to run.
excluded_tags = Keyword.get(ExUnit.configuration(), :exclude, [])

integration_excluded? =
  Enum.any?(excluded_tags, fn
    :integration -> true
    {:integration, _value} -> true
    _tag -> false
  end)

if !integration_excluded? do
  :ok = Pulsar.Test.Support.System.start_pulsar()

  ExUnit.after_suite(fn _result ->
    :ok = Pulsar.Test.Support.System.stop_pulsar()
  end)
end
