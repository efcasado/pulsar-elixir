defmodule Pulsar.Consumer.OptionsTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Pulsar.Consumer.Options

  describe "validate!/1" do
    test "applies defaults for options that have one" do
      opts = Options.validate!([])

      assert opts[:client] == :default
      assert opts[:subscription_type] == :Shared
      assert opts[:consumer_count] == 1
      assert opts[:flow_initial] == 100
      assert opts[:initial_position] == :latest
      assert opts[:durable] == true
    end

    test "leaves options backed by the application environment absent" do
      opts = Options.validate!([])

      refute Keyword.has_key?(opts, :startup_delay_ms)
      refute Keyword.has_key?(opts, :startup_jitter_ms)
      refute Keyword.has_key?(opts, :partition_discovery_interval_ms)
    end

    test "accepts a name as either a string or an atom" do
      assert Options.validate!(name: "a-group")[:name] == "a-group"
      assert Options.validate!(name: MyApp.Consumer)[:name] == MyApp.Consumer
    end

    test "rejects an unknown subscription type" do
      assert_raise NimbleOptions.ValidationError, ~r/:subscription_type/, fn ->
        Options.validate!(subscription_type: :Whatever)
      end
    end

    test "accepts a start_message_id as a ledger and entry pair" do
      assert Options.validate!(start_message_id: {1, 2})[:start_message_id] == {1, 2}

      assert_raise NimbleOptions.ValidationError, ~r/:start_message_id/, fn ->
        Options.validate!(start_message_id: 1)
      end
    end

    test "warns about unknown options rather than failing" do
      log = capture_log(fn -> Options.validate!(flow_intial: 10, nonsense: true) end)

      assert log =~ "ignoring unknown options"
      assert log =~ ":flow_intial"
      assert log =~ ":nonsense"
    end
  end

  describe "validate!/1 with a dead letter policy" do
    test "is absent unless asked for" do
      refute Keyword.has_key?(Options.validate!([]), :dead_letter_policy)
    end

    test "requires a redelivery threshold once asked for" do
      assert Options.validate!(dead_letter_policy: [max_redelivery: 3])[:dead_letter_policy] ==
               [max_redelivery: 3]

      assert_raise NimbleOptions.ValidationError, ~r/required :max_redelivery option not found/, fn ->
        Options.validate!(dead_letter_policy: [topic: "a-dlq"])
      end

      assert_raise NimbleOptions.ValidationError, ~r/:max_redelivery/, fn ->
        Options.validate!(dead_letter_policy: [max_redelivery: nil])
      end
    end

    test "rejects a misspelled key rather than silently disabling the policy" do
      assert_raise NimbleOptions.ValidationError, ~r/unknown options \[:max_redeliveries\]/, fn ->
        Options.validate!(dead_letter_policy: [max_redeliveries: 3])
      end
    end
  end

  describe "validate!/1 chunk cleanup" do
    test "sweeps every 30 seconds by default" do
      assert Options.validate!([])[:chunk_cleanup_interval] == 30_000
    end

    test "accepts false, and nil as its alias, to disable the sweep" do
      # docs/chunking.md documented nil before the schema existed.
      assert Options.validate!(chunk_cleanup_interval: false)[:chunk_cleanup_interval] == false
      assert Options.validate!(chunk_cleanup_interval: nil)[:chunk_cleanup_interval] == nil
    end

    test "rejects an interval that would sweep continuously" do
      assert_raise NimbleOptions.ValidationError, ~r/:chunk_cleanup_interval/, fn ->
        Options.validate!(chunk_cleanup_interval: 0)
      end
    end
  end

  describe "docs/0" do
    test "documents every option in the schema" do
      docs = Options.docs()

      for option <- Keyword.keys(Options.schema()) do
        assert docs =~ "`:#{option}`", "#{option} is missing from the generated docs"
      end
    end
  end
end
