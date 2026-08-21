defmodule Pulsar.BackoffTest do
  use ExUnit.Case, async: true

  alias Pulsar.Backoff

  @max 30_000
  @scheduler_tolerance 50

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
      # The last two are how Pulsar.Topology.Resolver reports a failed lookup: the reason that
      # decides this is wrapped, and a bundle unload answers :ServiceNotReady through it.
      reasons = [
        :connection_lost,
        :disconnected,
        :no_broker_available,
        {:ServiceNotReady, "try again"},
        # The broker leaves the message unset sometimes, which must not read as a wrapper.
        {:ServiceNotReady, nil},
        {:lookup_failed, :ServiceNotReady},
        {:partition_metadata_check_failed, :ServiceNotReady}
      ]

      for reason <- reasons do
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

    test "only retries resolver exits caused by a disappearing server call" do
      call = {:gen_statem, :call, [self(), :metadata, 5_000]}

      assert Backoff.retryable?({:resolver_failed, :exit, {:noproc, call}})
      assert Backoff.retryable?({:resolver_failed, :exit, {:timeout, call}})

      refute Backoff.retryable?({:resolver_failed, :exit, :noproc})
      refute Backoff.retryable?({:resolver_failed, :error, %RuntimeError{message: "bug"}})
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

    # The final wait is capped rather than abandoned, so callers get the whole window they
    # requested without an exponential step taking the retry past it.
    test "uses the full budget without waiting a complete oversized backoff" do
      budget = 300

      {elapsed, {:error, :transient}} =
        :timer.tc(fn -> Backoff.run(fn -> {:error, :transient} end, &retryable?/1, budget) end, :millisecond)

      assert elapsed >= budget - 25
      assert elapsed <= budget + @scheduler_tolerance
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
      assert elapsed <= budget + 120 + @scheduler_tolerance
    end
  end
end
