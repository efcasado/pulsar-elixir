defmodule Pulsar.Integration.Consumer.DeadLetterPolicyTest do
  use Pulsar.Test.Case, async: true

  alias Pulsar.Protocol.Binary.Pulsar.Proto
  alias Pulsar.Test.Support.DummyConsumer

  @topic "persistent://public/default/dlq-test-topic"
  @messages Enum.map(1..3, &"Message #{&1}")

  setup_all do
    Utils.seed_topic(@topic, @messages, client: @client)

    :ok
  end

  test "an invalid message reaches the DLQ carrying its payload and is acknowledged" do
    topic = "persistent://public/default/dlq-invalid-topic"
    subscription = "invalid"
    dlq_topic = topic <> "-" <> subscription <> "-DLQ"

    {:ok, consumer_group} =
      Pulsar.Consumer.start(topic, subscription, DummyConsumer,
        client: @client,
        redelivery_interval: 100,
        dead_letter_policy: [max_redelivery: 1, topic: dlq_topic]
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [consumer] = Topology.workers(consumer_group)

    {:ok, dlq_group} =
      Pulsar.Consumer.start(dlq_topic, "dlq-consumer", DummyConsumer,
        client: @client,
        initial_position: :earliest
      )

    :ok = Pulsar.Consumer.await_ready(dlq_group)
    [dlq_consumer] = Topology.workers(dlq_group)

    command = %Proto.CommandMessage{
      consumer_id: 1,
      message_id: %Proto.MessageIdData{ledgerId: 1, entryId: 1},
      redelivery_count: 5
    }

    send(consumer, {:broker_message, {:invalid, command, "corrupt-payload", :checksum_mismatch}})

    Utils.wait_for(fn -> DummyConsumer.count_messages(dlq_consumer) > 0 end)

    assert [dlq_message] = DummyConsumer.get_messages(dlq_consumer)
    assert dlq_message.payload == "corrupt-payload"

    # The acknowledgement carries a validation error the broker has to accept; had
    # it not, the connection would have gone and taken the consumer with it.
    assert Process.alive?(consumer)
    assert Pulsar.Consumer.topic(consumer) == topic
  end

  test "diverts a message once it has been redelivered :max_redelivery times" do
    topic = @topic
    subscription = "failing"
    dlq_topic = topic <> "-" <> subscription <> "-DLQ"
    max_redelivery = 3

    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        topic,
        subscription,
        DummyConsumer,
        init_args: [fail_all: true],
        client: @client,
        initial_position: :earliest,
        subscription_type: :shared,
        redelivery_interval: 100,
        dead_letter_policy: [
          max_redelivery: max_redelivery,
          topic: dlq_topic
        ]
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [failing_consumer] = Topology.workers(consumer_group)

    {:ok, dlq_consumer_group} =
      Pulsar.Consumer.start(
        dlq_topic,
        "dlq-consumer",
        DummyConsumer,
        client: @client,
        subscription_type: :shared,
        initial_position: :earliest
      )

    :ok = Pulsar.Consumer.await_ready(dlq_consumer_group)
    [dlq_consumer] = Topology.workers(dlq_consumer_group)

    Utils.wait_for(fn ->
      DummyConsumer.count_messages(dlq_consumer) == length(@messages)
    end)

    failing_consumer_count = DummyConsumer.count_messages(failing_consumer)

    # The delivery that reaches the threshold is diverted rather than delivered, so the callback
    # sees a message on every attempt below it and never on the one that dead letters it.
    assert failing_consumer_count == length(@messages) * max_redelivery

    dlq_messages = DummyConsumer.get_messages(dlq_consumer)
    assert length(dlq_messages) == length(@messages)

    dlq_payloads = Enum.map(dlq_messages, & &1.payload)
    assert dlq_payloads == @messages
  end

  test "redelivers forever, and creates no topic, when no policy is set" do
    topic = @topic
    subscription = "no-dlq"
    expected_dlq_topic = "#{topic}-#{subscription}-DLQ"

    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        topic,
        subscription,
        DummyConsumer,
        init_args: [fail_all: true],
        client: @client,
        initial_position: :earliest,
        subscription_type: :shared,
        redelivery_interval: 100
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [failing_consumer] = Topology.workers(consumer_group)

    Utils.wait_for(fn ->
      DummyConsumer.count_messages(failing_consumer) >= length(@messages) * 2
    end)

    {:ok, topics} = System.list_topics()
    refute expected_dlq_topic in topics
  end

  test "names the topic after the subscription when the policy does not" do
    topic = @topic
    subscription = "default-name"
    expected_dlq_topic = "#{topic}-#{subscription}-DLQ"

    {:ok, consumer_group} =
      Pulsar.Consumer.start(
        topic,
        subscription,
        DummyConsumer,
        init_args: [fail_all: true],
        client: @client,
        initial_position: :earliest,
        subscription_type: :shared,
        redelivery_interval: 100,
        dead_letter_policy: [
          max_redelivery: 2
        ]
      )

    :ok = Pulsar.Consumer.await_ready(consumer_group)
    [failing_consumer] = Topology.workers(consumer_group)

    {:ok, dlq_consumer_group} =
      Pulsar.Consumer.start(
        expected_dlq_topic,
        "dlq-default-monitor",
        DummyConsumer,
        client: @client,
        subscription_type: :shared,
        initial_position: :earliest
      )

    :ok = Pulsar.Consumer.await_ready(dlq_consumer_group)
    [dlq_consumer] = Topology.workers(dlq_consumer_group)

    Utils.wait_for(fn ->
      DummyConsumer.count_messages(dlq_consumer) == length(@messages)
    end)

    dlq_messages = DummyConsumer.get_messages(dlq_consumer)
    assert length(dlq_messages) == length(@messages)

    assert DummyConsumer.count_messages(failing_consumer) == length(@messages) * 2
  end

  test "producer options configure the running dead letter producer" do
    topic = "#{@topic}-producer-options"
    subscription = "producer-options"

    {:ok, consumer_group} =
      Pulsar.Consumer.start(topic, subscription, DummyConsumer,
        client: @client,
        redelivery_interval: 100,
        dead_letter_policy: [max_redelivery: 1, producer: [compression: :lz4, batch_enabled: true]]
      )

    dead_letter_root =
      Utils.wait_for(
        fn ->
          consumer_group
          |> Supervisor.which_children()
          |> Enum.find_value(fn
            {{:dead_letter, _topic}, pid, :supervisor, _modules} when is_pid(pid) -> pid
            _child -> nil
          end)
        end,
        until: &is_pid/1
      )

    :ok = Pulsar.Producer.await_ready(dead_letter_root)
    [producer] = Topology.workers(dead_letter_root)

    producer_state = :sys.get_state(producer)

    # Both differ from the schema defaults, :none and false, so a default cannot satisfy them.
    assert producer_state.compression == :lz4
    assert producer_state.batch_enabled == true
  end

  @tag telemetry_listen: [
         [:pulsar, :consumer, :message, :nacked],
         [:pulsar, :consumer, :redelivery, :requested],
         [:pulsar, :consumer, :dead_letter, :diverted]
       ]
  test "reports rejecting, retrying and parking a message" do
    topic = "#{@topic}-telemetry"
    subscription = "telemetry"
    dead_letter_topic = "#{topic}-#{subscription}-DLQ"

    {:ok, group} =
      Pulsar.Consumer.start(topic, subscription, DummyConsumer,
        init_args: [fail_all: true],
        client: @client,
        initial_position: :earliest,
        redelivery_interval: 100,
        dead_letter_policy: [max_redelivery: 1]
      )

    :ok = Pulsar.Consumer.await_ready(group)
    [_consumer] = Topology.workers(group)

    {:ok, producer} = Pulsar.Producer.start(topic, client: @client)
    :ok = Pulsar.Producer.await_ready(producer, client: @client)
    {:ok, _id} = Pulsar.Producer.send(producer, "doomed")

    assert_receive {:telemetry_event,
                    %{
                      event: [:pulsar, :consumer, :message, :nacked],
                      measurements: %{count: 1},
                      metadata: %{topic: ^topic, subscription_name: ^subscription}
                    }},
                   5_000

    assert_receive {:telemetry_event,
                    %{
                      event: [:pulsar, :consumer, :redelivery, :requested],
                      measurements: %{count: 1}
                    }},
                   5_000

    assert_receive {:telemetry_event,
                    %{
                      event: [:pulsar, :consumer, :dead_letter, :diverted],
                      measurements: %{count: 1},
                      metadata: %{
                        topic: ^topic,
                        subscription_name: ^subscription,
                        dead_letter_topic: ^dead_letter_topic,
                        redelivery_count: 1
                      }
                    }},
                   5_000
  end

  @tag telemetry_listen: [[:pulsar, :consumer, :dead_letter, :failed]]
  test "reports a dead letter topic it cannot publish to" do
    topic = "#{@topic}-telemetry-failed"
    subscription = "telemetry-failed"
    dead_letter_topic = "#{topic}-#{subscription}-DLQ"

    {:ok, group} =
      Pulsar.Consumer.start(topic, subscription, DummyConsumer,
        init_args: [fail_all: true],
        client: @client,
        initial_position: :earliest,
        redelivery_interval: 100,
        dead_letter_policy: [max_redelivery: 1]
      )

    :ok = Pulsar.Consumer.await_ready(group)
    [consumer] = Topology.workers(group)

    # A root with no dead letter child, so the producer cannot be resolved and diverting fails
    # the way it does against a dead letter topic that is unavailable. Pointing the worker at
    # nil instead would stop it diverting at all, which is a different path.
    {:ok, empty_root} = Supervisor.start_link([], strategy: :one_for_one)
    :sys.replace_state(consumer, &%{&1 | dead_letter_root: empty_root})

    command = %Proto.CommandMessage{
      consumer_id: 1,
      message_id: %Proto.MessageIdData{ledgerId: 1, entryId: 1},
      redelivery_count: 5
    }

    send(consumer, {:broker_message, {:invalid, command, "corrupt", :checksum_mismatch}})

    assert_receive {:telemetry_event,
                    %{
                      event: [:pulsar, :consumer, :dead_letter, :failed],
                      measurements: %{count: 1},
                      # Named even though the producer could not be resolved, which is when
                      # an alert on this event most needs to say which topic is affected.
                      metadata: %{
                        topic: ^topic,
                        dead_letter_topic: ^dead_letter_topic,
                        reason: :no_dead_letter_producer
                      }
                    }},
                   5_000
  end
end
