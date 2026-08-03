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

  describe "run/1" do
    test "retries the standard transient broker errors" do
      for reason <- [:disconnected, {:ServiceNotReady, "try again"}] do
        counter = :counters.new(1, [])

        assert Backoff.run(fn ->
                 :counters.add(counter, 1, 1)

                 case :counters.get(counter, 1) do
                   1 -> {:error, reason}
                   2 -> {:ok, :retried}
                 end
               end) == {:ok, :retried}
      end
    end

    test "returns other broker errors without retrying" do
      counter = :counters.new(1, [])
      error = {:error, {:AuthorizationError, "denied"}}

      assert Backoff.run(fn ->
               :counters.add(counter, 1, 1)
               error
             end) == error

      assert :counters.get(counter, 1) == 1
    end
  end

  describe "run/3" do
    defp retryable?(:transient), do: true
    defp retryable?(_reason), do: false

    test "returns a success without retrying" do
      assert Backoff.run(fn -> {:ok, :first_try} end, &retryable?/1) == {:ok, :first_try}
    end

    # The distinction the predicate carries: a topic that does not exist will not start
    # existing, and retrying it is what turns one failure into a restart storm.
    test "returns an error the predicate rejects without retrying" do
      {elapsed, result} =
        :timer.tc(fn -> Backoff.run(fn -> {:error, :fatal} end, &retryable?/1) end, :millisecond)

      assert result == {:error, :fatal}
      assert elapsed < 50
    end

    test "retries an error the predicate accepts until it succeeds" do
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      result =
        Backoff.run(
          fn ->
            case Agent.get_and_update(counter, &{&1 + 1, &1 + 1}) do
              attempt when attempt < 3 -> {:error, :transient}
              attempt -> {:ok, attempt}
            end
          end,
          &retryable?/1
        )

      assert result == {:ok, 3}
    end

    test "supports an infinite budget" do
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      result =
        Backoff.run(
          fn ->
            case Agent.get_and_update(counter, &{&1 + 1, &1 + 1}) do
              1 -> {:error, :transient}
              attempt -> {:ok, attempt}
            end
          end,
          &retryable?/1,
          :infinity
        )

      assert result == {:ok, 2}
    end

    test "gives up with the error unchanged once the budget is spent" do
      assert Backoff.run(fn -> {:error, :transient} end, &retryable?/1, 200) == {:error, :transient}
    end

    # The budget is what keeps a retrying worker from outliving the 5s a supervisor allows it
    # for shutdown, so it bounds elapsed time rather than a count of attempts.
    test "spends no longer than the budget it was given" do
      budget = 300

      {elapsed, {:error, :transient}} =
        :timer.tc(fn -> Backoff.run(fn -> {:error, :transient} end, &retryable?/1, budget) end, :millisecond)

      assert elapsed <= budget
    end

    # The calls being retried carry their own multi-second timeouts, so a budget that charged
    # only the sleeps would bound almost nothing when the broker is the slow part.
    test "charges the time the function itself spends" do
      budget = 300

      slow = fn ->
        Process.sleep(120)
        {:error, :transient}
      end

      {elapsed, {:error, :transient}} =
        :timer.tc(fn -> Backoff.run(slow, &retryable?/1, budget) end, :millisecond)

      # Without charging the calls, four sleeps of 120ms would fit inside the sleep budget
      # alone and this would run for the best part of a second.
      assert elapsed <= budget + 120
    end
  end
end
