defmodule Pulsar.Test.Support.Utils do
  def wait_for(_fun, attempts \\ 100, interval_ms \\ 100)

  def wait_for(_fun, 0, _interval_ms) do
    :error
  end

  def wait_for(fun, attempts, interval_ms) do
    case fun.() do
      true ->
        :ok

      false ->
        Process.sleep(interval_ms)
        wait_for(fun, attempts - 1, interval_ms)
    end
  end
end
