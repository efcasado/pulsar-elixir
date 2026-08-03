defmodule Accountant do
  @moduledoc false
  use Pulsar.Consumer.Callback

  def init([reporter], _context) do
    Process.send(reporter, {:consumer_ready, self()}, [])
    {:ok, reporter}
  end

  def handle_message(%Pulsar.Message{payload: payload}, reporter) do
    case String.to_integer(payload) do
      number when rem(number, 2) == 0 ->
        IO.puts("balanced #{number}")
        {:ok, reporter}

      number ->
        {:error, {:does_not_balance, number}, reporter}
    end
  end
end

defmodule Auditor do
  @moduledoc false
  use Pulsar.Consumer.Callback

  def init([reporter], _context) do
    Process.send(reporter, {:consumer_ready, self()}, [])
    {:ok, reporter}
  end

  def handle_message(%Pulsar.Message{payload: payload} = message, reporter) do
    properties = Pulsar.Message.properties(message)

    IO.puts(
      "parked #{payload} from #{properties["REAL_TOPIC"]} " <>
        "(origin #{properties["ORIGIN_MESSAGE_ID"]})"
    )

    Process.send(reporter, {:parked, String.to_integer(payload)}, [])
    {:ok, reporter}
  end
end

defmodule Main do
  @moduledoc false

  @broker "pulsar://broker1:6650"
  @topic "persistent://public/default/ledger"
  @subscription "accounting"
  @dead_letter "#{@topic}-#{@subscription}-DLQ"

  @numbers 1..6
  @max_redelivery 3

  def run do
    {:ok, _pid} =
      Pulsar.Client.start_link(
        host: @broker,
        producers: [[topic: @topic, name: :ledger]],
        consumers: [accountant(), auditor()]
      )

    # Both subscriptions have to exist before anything is published: the accountant's starts at
    # :latest and would miss what came before it, and the auditor's is what creates the dead
    # letter topic, so the producer diverting into it never races the broker.
    await_consumers(2)

    Enum.each(@numbers, &publish/1)

    odds = Enum.filter(@numbers, &(rem(&1, 2) == 1))
    await_parked(MapSet.new(odds))
  end

  defp accountant do
    [
      topic: @topic,
      subscription_name: @subscription,
      callback_module: Accountant,
      init_args: [self()],
      # Without a redelivery interval a rejected message is never redelivered, its redelivery
      # count never grows, and the policy below never triggers.
      redelivery_interval: 500,
      dead_letter_policy: [max_redelivery: @max_redelivery]
    ]
  end

  defp auditor do
    [
      topic: @dead_letter,
      subscription_name: "audit",
      callback_module: Auditor,
      init_args: [self()],
      initial_position: :earliest
    ]
  end

  # A producer answers {:error, :not_ready} until its topic has been discovered.
  defp publish(number) do
    case Pulsar.Producer.send(:ledger, Integer.to_string(number)) do
      {:ok, _message_id} ->
        :ok

      {:error, _reason} ->
        Process.sleep(100)
        publish(number)
    end
  end

  defp await_consumers(0), do: :ok

  defp await_consumers(remaining) do
    receive do
      {:consumer_ready, _pid} -> await_consumers(remaining - 1)
    end
  end

  defp await_parked(expected) do
    if MapSet.size(expected) == 0 do
      IO.puts("every odd number reached the dead letter topic after #{@max_redelivery} attempts")
    else
      receive do
        {:parked, number} -> await_parked(MapSet.delete(expected, number))
      end
    end
  end
end

Logger.configure(level: :warning)

Main.run()
