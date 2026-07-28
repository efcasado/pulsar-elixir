defmodule Pulsar.BackoffTest do
  use ExUnit.Case, async: true

  alias Pulsar.Backoff

  @max 30_000

  test "starts within the first 100ms rather than at zero" do
    for _attempt <- 1..100 do
      wait = Backoff.next(0)
      assert wait >= 1 and wait <= 100
    end
  end

  test "roughly doubles each time" do
    waits = Enum.scan(1..6, 0, fn _attempt, previous -> Backoff.next(previous) end)

    waits
    |> Enum.zip(tl(waits))
    |> Enum.each(fn {previous, next} -> assert next > previous end)
  end

  test "settles at the ceiling, jitter aside" do
    # Jitter is added after the cap, so the ceiling is the bound plus one jitter.
    assert Backoff.next(@max * 10) <= @max + 100
    assert Backoff.next(@max * 10) > @max
  end

  test "never returns the same wait for every caller" do
    # Two connections dropping together must not reconnect in lockstep.
    waits = for _caller <- 1..50, do: Backoff.next(1_000)

    assert Enum.uniq(waits) != [hd(waits)]
  end
end
