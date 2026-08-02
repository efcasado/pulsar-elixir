defmodule Pulsar.ProducerTest do
  use ExUnit.Case, async: true

  alias Pulsar.Producer

  describe "send/3" do
    test "accepts atom and string names" do
      opts = [client: :producer_send_missing_client]

      assert Producer.send(:missing, "payload", opts) == {:error, :producer_not_found}
      assert Producer.send("missing", "payload", opts) == {:error, :producer_not_found}
    end

    test "rejects unsupported target types" do
      assert_raise FunctionClauseError, fn -> apply(Producer, :send, [42, "payload", []]) end
    end

    test "rejects non-binary payloads for pids and names" do
      for producer <- [self(), :producer] do
        assert_raise FunctionClauseError, fn -> apply(Producer, :send, [producer, %{value: 1}, []]) end
      end
    end
  end
end
