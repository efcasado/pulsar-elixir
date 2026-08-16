defmodule Pulsar.Consumer.OptionsTest do
  use ExUnit.Case, async: true

  alias Pulsar.Consumer.Options
  alias Pulsar.Test.Support.Flow

  @required [topic: "t", subscription_name: "s", callback_module: MyApp.Handler]

  defp validate!(opts), do: Options.validate!(Keyword.merge(@required, opts))

  describe "validate!/1" do
    test "applies defaults for options that have one" do
      opts = validate!([])

      assert opts[:client] == :default
      assert opts[:subscription_type] == :shared
      assert opts[:consumer_count] == 1
      assert opts[:flow_initial] == 100
      assert opts[:initial_position] == :latest
      assert opts[:durable] == true
    end

    test "defaults the startup delays and partition discovery" do
      opts = validate!([])

      assert opts[:startup_delay_ms] == 0
      assert opts[:startup_jitter_ms] == 0
      assert opts[:partition_discovery_interval_ms] == 60_000
    end

    test "defaults the flow policy to :auto" do
      assert validate!([])[:flow_policy] == :auto
    end

    test "keeps flow_initial under a policy of its own, so a worker still starts with a window" do
      policy = {Flow, :never, []}
      opts = validate!(flow_policy: policy, flow_initial: 25)

      assert opts[:flow_policy] == policy
      assert opts[:flow_initial] == 25
    end

    test "rejects an automatic consumer that would be granted nothing to start with" do
      assert_raise ArgumentError, ~r/never receives a message/, fn ->
        validate!(flow_policy: :auto, flow_initial: 0)
      end
    end

    test "allows a policy of its own to start with nothing, since permits can come from elsewhere" do
      policy = {Flow, :never, []}

      assert validate!(flow_policy: policy, flow_initial: 0)[:flow_policy] == policy
    end

    test "rejects a policy whose function cannot take the flow it would be given" do
      assert_raise NimbleOptions.ValidationError, ~r|never/2|, fn ->
        validate!(flow_policy: {Flow, :never, [:extra]})
      end
    end

    test "rejects a policy that is neither :auto nor an MFA" do
      assert_raise NimbleOptions.ValidationError, ~r/:auto or a \{module, function, args\}/, fn ->
        validate!(flow_policy: :whenever)
      end
    end

    test "accepts a name as either a string or an atom" do
      assert validate!(name: "a-group")[:name] == "a-group"
      assert validate!(name: MyApp.Consumer)[:name] == MyApp.Consumer
    end

    test "rejects an unknown subscription type" do
      assert_raise NimbleOptions.ValidationError, ~r/:subscription_type/, fn ->
        validate!(subscription_type: :Whatever)
      end
    end

    test "accepts a start_message_id as a ledger and entry pair" do
      assert validate!(start_message_id: {1, 2})[:start_message_id] == {1, 2}

      assert_raise NimbleOptions.ValidationError, ~r/:start_message_id/, fn ->
        validate!(start_message_id: 1)
      end
    end

    test "rejects unknown options" do
      assert_raise NimbleOptions.ValidationError, ~r/unknown options.*:flow_intial.*:nonsense/, fn ->
        validate!(flow_intial: 10, nonsense: true)
      end
    end
  end

  describe "validate!/1 ack type" do
    test "acknowledges individually by default" do
      assert validate!([])[:ack_type] == :individual
    end

    for subscription_type <- [:exclusive, :failover] do
      test "allows cumulative acks on a #{subscription_type} subscription, which has one cursor" do
        opts = validate!(ack_type: :cumulative, subscription_type: unquote(subscription_type))

        assert opts[:ack_type] == :cumulative
      end
    end

    for subscription_type <- [:shared, :key_shared] do
      test "refuses cumulative acks on a #{subscription_type} subscription the broker would reject" do
        assert_raise ArgumentError, ~r/no single cursor/, fn ->
          validate!(ack_type: :cumulative, subscription_type: unquote(subscription_type))
        end
      end
    end

    test "refuses cumulative acks on the default subscription type rather than defaulting around it" do
      assert_raise ArgumentError, ~r/:shared subscription/, fn ->
        validate!(ack_type: :cumulative)
      end
    end

    test "rejects an unknown ack type" do
      assert_raise NimbleOptions.ValidationError, ~r/:ack_type/, fn ->
        validate!(ack_type: :Cumulative)
      end
    end
  end

  describe "validate!/1 with a dead letter policy" do
    test "is absent unless asked for" do
      refute Keyword.has_key?(validate!([]), :dead_letter_policy)
    end

    test "requires a redelivery threshold once asked for" do
      assert validate!(dead_letter_policy: [max_redelivery: 3])[:dead_letter_policy] ==
               [max_redelivery: 3]

      assert_raise NimbleOptions.ValidationError, ~r/required :max_redelivery option not found/, fn ->
        validate!(dead_letter_policy: [topic: "a-dlq"])
      end

      assert_raise NimbleOptions.ValidationError, ~r/:max_redelivery/, fn ->
        validate!(dead_letter_policy: [max_redelivery: nil])
      end
    end

    test "rejects a misspelled key rather than silently disabling the policy" do
      assert_raise NimbleOptions.ValidationError, ~r/unknown options \[:max_redeliveries\]/, fn ->
        validate!(dead_letter_policy: [max_redeliveries: 3])
      end
    end
  end

  describe "validate!/1 chunk cleanup" do
    test "sweeps every 30 seconds by default" do
      assert validate!([])[:chunk_cleanup_interval] == 30_000
    end

    test "accepts false, and nil as its alias, to disable the sweep" do
      # docs/chunking.md documented nil before the schema existed.
      assert validate!(chunk_cleanup_interval: false)[:chunk_cleanup_interval] == false
      assert validate!(chunk_cleanup_interval: nil)[:chunk_cleanup_interval] == nil
    end

    test "rejects an interval that would sweep continuously" do
      assert_raise NimbleOptions.ValidationError, ~r/:chunk_cleanup_interval/, fn ->
        validate!(chunk_cleanup_interval: 0)
      end
    end
  end

  describe "validate!/1 with dead letter producer options" do
    defp policy(producer), do: [dead_letter_policy: [max_redelivery: 3, producer: producer]]

    test "accepts producer options" do
      assert validate!(policy(compression: :lz4))[:dead_letter_policy][:producer] == [compression: :lz4]
    end

    test "rejects an option no producer has" do
      assert_raise NimbleOptions.ValidationError, ~r/nonsense/, fn ->
        validate!(policy(nonsense: true))
      end
    end

    test "rejects an option with the wrong value, naming what it accepts" do
      assert_raise NimbleOptions.ValidationError, ~r/compression/, fn ->
        validate!(policy(compression: :LZ4))
      end
    end

    # These come from the consumer that owns the producer, so accepting them would silently
    # either be overridden or detach the producer from its consumer.
    for managed <- [:topic, :client, :name] do
      test "rejects #{managed}, which the consumer decides" do
        assert_raise NimbleOptions.ValidationError, ~r/belongs to the consumer/, fn ->
          validate!(policy([{unquote(managed), "nope"}]))
        end
      end
    end
  end
end
