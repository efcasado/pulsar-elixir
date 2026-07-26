defmodule Pulsar.Producer.OptionsTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Pulsar.Producer.Options

  @required [topic: "t"]

  defp validate!(opts), do: Options.validate!(Keyword.merge(@required, opts))

  describe "validate!/1" do
    test "applies defaults for options that have one" do
      opts = validate!([])

      assert opts[:client] == :default
      assert opts[:producer_count] == 1
      assert opts[:access_mode] == :Shared
      assert opts[:compression] == :NONE
      assert opts[:batch_enabled] == false
    end

    test "defaults the startup delays and partition discovery" do
      opts = validate!([])

      assert opts[:startup_delay_ms] == 1_000
      assert opts[:startup_jitter_ms] == 1_000
      assert opts[:partition_discovery_interval_ms] == 60_000
    end

    test "accepts a name as either a string or an atom" do
      assert validate!(name: "a-producer")[:name] == "a-producer"
      assert validate!(name: :a_producer)[:name] == :a_producer
    end

    test "rejects an unknown access mode" do
      assert_raise NimbleOptions.ValidationError, ~r/:access_mode/, fn ->
        validate!(access_mode: :Whatever)
      end
    end

    test "rejects a non-integer producer count" do
      assert_raise NimbleOptions.ValidationError, ~r/:producer_count/, fn ->
        validate!(producer_count: "two")
      end
    end

    test "accepts false to disable partition discovery" do
      assert validate!(partition_discovery_interval_ms: false)[:partition_discovery_interval_ms] == false
      assert validate!(partition_discovery_interval_ms: 5_000)[:partition_discovery_interval_ms] == 5_000
    end

    test "warns about unknown options rather than failing" do
      log = capture_log(fn -> validate!(batch_sze: 10, nonsense: true) end)

      assert log =~ "ignoring unknown options"
      assert log =~ ":batch_sze"
      assert log =~ ":nonsense"
    end
  end

  describe "docs/0" do
    test "documents every option in the schema" do
      docs = Options.docs()

      # Options with `doc: false` are internal and deliberately absent.
      documented = for {option, spec} <- Options.schema(), spec[:doc] != false, do: option

      assert documented != []

      for option <- documented do
        assert docs =~ "`:#{option}`", "#{option} is missing from the generated docs"
      end
    end
  end
end
