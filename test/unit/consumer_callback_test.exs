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
    subscription_type: :Shared,
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
  end
end
