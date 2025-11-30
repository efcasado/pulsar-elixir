defmodule BingoPlayer do
  @moduledoc false
  use Pulsar.Consumer.Callback

  require Logger

  def init([game_master, card_size]) do
    card = card(card_size)
    IO.puts("#{inspect(self())} started with card: #{inspect(card, charlists: :as_lists)}")
    {:ok, {game_master, card}}
  end

  def handle_message(%Pulsar.Message{payload: payload}, {game_master, card}) do
    number = String.to_integer(payload)

    updated_card = scratch(number, card)

    if length(updated_card) < length(card) do
      if bingo?(updated_card) do
        IO.puts("#{inspect(self())}: 🎉 BINGO!")
        Process.send(game_master, self(), [])
      else
        IO.puts("#{inspect(self())}: scratched #{number}! Card now has: #{inspect(updated_card, charlists: :as_lists)}")
      end
    end

    {:ok, {game_master, updated_card}}
  end

  def bingo?([]), do: true
  def bingo?(_card), do: false

  def scratch(number, card) do
    Enum.reject(card, &(&1 == number))
  end

  defp card(size) do
    1..99
    |> Enum.shuffle()
    |> Enum.take(size)
  end
end

defmodule Main do
  @moduledoc false
  require Logger

  @broker "pulsar://broker1:6650"
  @topic "persistent://public/default/bingo"

  @num_players 3
  @card_size 5

  def call_numbers([], _producer), do: :ok

  def call_numbers(numbers, producer) do
    [number | other_numbers] = Enum.shuffle(numbers)

    IO.puts("#{inspect(self())} calling number: #{number}")
    Pulsar.send(producer, Integer.to_string(number))

    call_numbers(Enum.shuffle(other_numbers), producer)
  end

  def run do
    deps = Application.spec(:pulsar, :applications)
    Enum.each(deps, &Application.ensure_all_started/1)

    config = [
      host: @broker,
      socket_opts: [verify: :verify_none],
      producers: [
        game_master: [
          topic: @topic
        ]
      ],
      consumers: consumers(@num_players, @card_size, @topic)
    ]

    {:ok, _pid} = Pulsar.start(config)

    spawn(fn -> call_numbers(1..99, :game_master) end)

    and_the_winner_is()
  end

  defp consumers(num_players, card_size, topic) do
    Enum.map(1..num_players, fn player_number ->
      name = "player-#{player_number}"
      atom_name = String.to_atom(name)

      {atom_name,
       [
         topic: topic,
         subscription_name: name,
         subscription_type: :Exclusive,
         callback_module: BingoPlayer,
         durable: false,
         init_args: [self(), card_size]
       ]}
    end)
  end

  defp and_the_winner_is do
    receive do
      winner ->
        IO.puts("🥁 and the winner is ... congratulations, #{inspect(winner)}!!!")
    end
  end
end

Logger.configure(level: :warning)

Main.run()
