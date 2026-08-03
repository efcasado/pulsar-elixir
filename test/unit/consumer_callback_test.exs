defmodule Pulsar.Consumer.CallbackTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Pulsar.Consumer.Callback

  defmodule WithInit do
    @moduledoc false
    use Callback

    def init(init_args, context), do: {:ok, {init_args, context}}

    def handle_message(_message, state), do: {:ok, state}
  end

  defmodule WithoutInit do
    @moduledoc false
    use Callback

    def handle_message(_message, state), do: {:ok, state}
  end

  @context %{
    topic: "persistent://public/default/orders-partition-2",
    base_topic: "persistent://public/default/orders",
    partition: 2,
    subscription_name: "order-service",
    subscription_type: :shared,
    consumer_name: "orders-order-service-partition-2-1"
  }

  describe "init/2" do
    test "receives the init args and the consumer's context" do
      assert WithInit.init(:args, @context) == {:ok, {:args, @context}}
    end

    test "defaults to {:ok, nil}" do
      assert WithoutInit.init(:args, @context) == {:ok, nil}
    end

    test "is the only arity the behaviour defines" do
      callbacks = Callback.behaviour_info(:callbacks)

      assert {:init, 2} in callbacks
      refute {:init, 1} in callbacks
    end

    test "refuses to compile a callback still defining init/1" do
      assert_raise CompileError, ~r/defines init\/1.*would never be called/s, fn ->
        Code.compile_string("""
        defmodule Pulsar.Consumer.CallbackTest.Unmigrated do
          use Pulsar.Consumer.Callback

          def init(opts), do: {:ok, opts}

          def handle_message(_message, state), do: {:ok, state}
        end
        """)
      end
    end
  end
end
