defmodule Pulsar.Test.Utils do
  defp wait_until(_fun, attempts \\ 100, interval_ms \\ 100)

  defp wait_until(_fun, 0, _interval_ms) do
    :error
  end

  defp wait_until(fun, attempts, interval_ms) do
    case fun.() do
      true ->
        :ok

      false ->
        Process.sleep(interval_ms)
        wait_until(fun, attempts - 1, interval_ms)
    end
  end
end
