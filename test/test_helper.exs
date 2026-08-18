Application.put_env(:junit_formatter, :report_dir, "test/reports")
Application.put_env(:junit_formatter, :report_file, "junit.xml")
Application.put_env(:junit_formatter, :automatic_create_dir?, true)

Application.ensure_all_started(:telemetry_test)

# Most of what a test waits on here is a broker round trip, which the 100ms default does not
# cover. refute_receive keeps its own, shorter default, so asserting absence stays quick.
ExUnit.configure(
  formatters: [ExUnit.CLIFormatter, JUnitFormatter],
  capture_log: true,
  assert_receive_timeout: 5_000
)

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
