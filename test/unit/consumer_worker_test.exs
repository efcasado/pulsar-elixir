defmodule Pulsar.Consumer.WorkerTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Pulsar.Consumer.Worker

  defmodule Callback do
    @moduledoc false
    use Pulsar.Consumer.Callback

    def init(:refuse, _context), do: {:error, :init_refused}
    def init(init_args, context), do: {:ok, {init_args, context}}

    def handle_message(_message, state), do: {:ok, state}
  end

  @topic "persistent://public/default/orders"

  defp worker_state do
    struct(Worker,
      topic: Pulsar.Topic.partition(@topic, 2),
      base_topic: @topic,
      partition: 2,
      subscription_name: "order-service",
      subscription_type: :Shared,
      consumer_name: "orders-order-service-partition-2-1",
      callback_module: Callback
    )
  end

  describe "callback initialization" do
    test "hands the callback its resolved topic and subscription" do
      assert {:noreply, state} = Worker.handle_continue({:init_callback, :args}, worker_state())

      assert {:args, context} = state.callback_state

      assert context == %{
               topic: "persistent://public/default/orders-partition-2",
               base_topic: @topic,
               partition: 2,
               subscription_name: "order-service",
               subscription_type: :Shared,
               consumer_name: "orders-order-service-partition-2-1"
             }
    end

    test "marks the consumer ready only once the callback has initialized" do
      refute worker_state().ready

      assert {:noreply, state} = Worker.handle_continue({:init_callback, :args}, worker_state())
      assert state.ready
    end

    test "stops without a state when the callback refuses to initialize" do
      assert {:stop, :init_refused, nil} = Worker.handle_continue({:init_callback, :refuse}, worker_state())
    end
  end
end
