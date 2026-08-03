defmodule Pulsar.ProducerTest do
  use ExUnit.Case, async: true

  alias Pulsar.Producer

  describe "await_ready/2" do
    test "reports a missing named producer after the wait" do
      assert Producer.await_ready(:missing, client: :producer_missing_client, timeout: 0) ==
               {:error, :not_found}
    end

    test "reports a stale producer pid" do
      producer = spawn(fn -> :ok end)
      ref = Process.monitor(producer)
      assert_receive {:DOWN, ^ref, :process, ^producer, _reason}

      assert Producer.await_ready(producer, timeout: 25) == {:error, :not_found}
    end
  end

  describe "send/3" do
    test "accepts atom and string names" do
      opts = [client: :producer_send_missing_client]

      assert Producer.send(:missing, "payload", opts) == {:error, :not_found}
      assert Producer.send("missing", "payload", opts) == {:error, :not_found}
      assert Producer.stop(:missing, opts) == {:error, :not_found}
    end

    test "rejects unsupported target types" do
      assert_raise FunctionClauseError, fn -> send_message(Producer, 42, "payload") end
    end

    test "rejects non-binary payloads for pids and names" do
      for producer <- [self(), :producer] do
        assert_raise FunctionClauseError, fn -> send_message(Producer, producer, %{value: 1}) end
      end
    end
  end

  defp send_message(module, producer, message), do: module.send(producer, message, [])
end
