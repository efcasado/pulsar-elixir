defmodule Pulsar.Consumer.Callback do
  @moduledoc """
  Macro for creating Pulsar consumer callback modules that support internal state.

  This module provides a `use` macro that sets up your module as a consumer callback
  with default implementations for optional callbacks.

  ## Usage

  Use this module in your consumer callback:

      defmodule MyApp.MessageCounter do
        use Pulsar.Consumer.Callback

        # Implement required callbacks
        def handle_message(message, state) do
          # Process message
          {:ok, state}
        end
      end

  When you `use Pulsar.Consumer.Callback`, your module automatically:
  - Gets the `@behaviour Pulsar.Consumer.Callback` annotation
  - Receives default implementations for all optional callbacks
  - Can override any callback by simply defining it

  ## Required Callbacks

  - `handle_message/2` - Handle incoming messages with current state

  ## Optional Callbacks

  All optional callbacks have sensible defaults that you can override:

  - `init/1` - Initialize the callback module state (default: `{:ok, nil}`)
  - `terminate/2` - Cleanup when consumer terminates (default: `:ok`)
  - `handle_call/3` - Handle synchronous calls (default: `{:reply, {:error, :not_implemented}, state}`)
  - `handle_cast/2` - Handle asynchronous casts (default: `{:noreply, state}`)
  - `handle_info/2` - Handle other messages (default: `{:noreply, state}`)
  - `became_active/1` - Called when this consumer becomes the active consumer of a `:Failover` subscription (default: `{:noreply, state}`)
  - `became_passive/1` - Called when this consumer becomes a passive (standby) consumer of a `:Failover` subscription (default: `{:noreply, state}`)
  - `handle_invalid_message/2` - Called instead of `handle_message/2` for a message whose
    bytes could not be trusted (default: log a warning and acknowledge it, so it is not
    redelivered). Override it to record or divert such messages; see `Pulsar.Message.valid?/1`
    for what they contain. Because they are routed here, `handle_message/2` only ever
    receives messages that arrived intact.

  ## Message Format

  `handle_message/2` receives a `Pulsar.Message` struct containing all message information.
  See `Pulsar.Message` for detailed field documentation.

  You can pattern match on the struct:

      def handle_message(%Pulsar.Message{payload: payload}, state) do
        # Process the payload
        {:ok, state}
      end

  Or access fields directly:

      def handle_message(%Pulsar.Message{} = message, state) do
        payload = message.payload
        message_id = message.message_id_to_ack
        {:ok, state}
      end

  ## Example Implementation

      defmodule MyApp.MessageCounter do
        use Pulsar.Consumer.Callback

        def init(opts) do
          max_messages = Keyword.get(opts, :max_messages, 1000)
          {:ok, %{count: 0, max_messages: max_messages, messages: []}}
        end

        def handle_message(%Pulsar.Message{payload: payload}, callback_state) do
          new_state = %{
            callback_state
            | count: callback_state.count + 1,
              messages: [payload | callback_state.messages]
          }

          # Stop processing if we've reached the limit
          if new_state.count >= new_state.max_messages do
            {:stop, new_state}
          else
            {:ok, new_state}
          end
        end


        def terminate(_reason, callback_state) do
          IO.puts("Processed \#{callback_state.count} messages")
          :ok
        end
      end

  ## Multiple Consumers

  For shared or key-shared subscriptions, `:consumer_count` runs several consumer processes
  on one subscription to increase throughput:

      {Pulsar.Client,
       host: "pulsar://localhost:6650",
       consumers: [
         [topic: topic,
          subscription_name: subscription,
          callback_module: MyCallback,
          subscription_type: :Key_Shared,
          consumer_count: 3,
          name: :orders]
       ]}

  Each process has independent callback state. The consumer facade intentionally hides
  those short-lived worker processes, so state that must be shared, queried, or aggregated
  across consumers should live in an application process with its own public API.

  ## Return Values

  ### `init/1` (Optional)

  If not implemented, defaults to `{:ok, nil}`.

  - `{:ok, state}` - Successful initialization with initial state
  - `{:error, reason}` - Initialization failed

  ### `handle_message/2`

  - `{:ok, new_state}` - Message processed successfully, acknowledge message automatically
  - `{:error, reason, new_state}` - Processing failed, track for redelivery
  - `{:noreply, new_state}` - Message processed, but don't automatically ACK/NACK. Use `Pulsar.Consumer.ack/2` or `Pulsar.Consumer.nack/2` for manual acknowledgment
  - `{:stop, new_state}` - Message processed successfully, acknowledge, then stop consumer

  ### `terminate/2` (Optional)

  If not implemented, defaults to `:ok`.

  - `:ok` - Cleanup completed successfully
  - Any other value is ignored

  ## Failover Consumer Events

  For `:Failover` subscriptions, `became_active/1` and `became_passive/1`
  notify a consumer when the broker promotes it to (or demotes it from) the
  active role. Useful for metrics/alerting or warming up state before a
  takeover; not meaningful for other subscription types. A passive consumer
  never receives messages, so there is nothing to pause or resume.

  ## Manual Acknowledgment

  When you return `{:noreply, state}` from `handle_message/2`, the message will NOT be automatically
  acknowledged or negatively acknowledged. This gives you full control over when and how to ACK/NACK messages,
  which is useful for:

  - Broadway pipelines that handle acknowledgment in batches
  - Async processing where acknowledgment happens after the callback returns
  - Custom acknowledgment logic based on downstream processing

  Example with manual ACK:

      def handle_message(%Pulsar.Message{payload: payload, message_id_to_ack: message_id_to_ack}, state) do
        # Use message_id_to_ack (not command.message_id) for manual ACK/NACK
        # This ensures batch messages are ACKed with the correct batch_index

        consumer = self()

        # Send to async processor
        Task.start(fn ->
          case process_async(payload) do
            :ok -> Pulsar.Consumer.ack(consumer, message_id_to_ack)
            {:error, _} -> Pulsar.Consumer.nack(consumer, message_id_to_ack)
          end
        end)

        {:noreply, state}
      end
  """

  @type message_args :: Pulsar.Message.t()

  @type init_arg :: term()
  @type state :: term()
  @type reason :: term()

  @callback init(init_arg) :: {:ok, state} | {:error, reason}
  @callback handle_message(message_args, state) ::
              {:ok, state}
              | {:error, reason, state}
              | {:noreply, state}
              | {:stop, state}

  @optional_callbacks init: 1,
                      terminate: 2,
                      handle_call: 3,
                      handle_cast: 2,
                      handle_info: 2,
                      became_active: 1,
                      became_passive: 1,
                      handle_invalid_message: 2
  @callback terminate(reason, state) :: term()
  @callback handle_call(term(), GenServer.from(), state) ::
              {:reply, term(), state}
              | {:reply, term(), state, timeout() | :hibernate | {:continue, term()}}
              | {:noreply, state}
              | {:noreply, state, timeout() | :hibernate | {:continue, term()}}
              | {:stop, term(), term(), state}
              | {:stop, term(), state}
  @callback handle_cast(term(), state) ::
              {:noreply, state}
              | {:noreply, state, timeout() | :hibernate | {:continue, term()}}
              | {:stop, term(), state}
  @callback handle_info(term(), state) ::
              {:noreply, state}
              | {:noreply, state, timeout() | :hibernate | {:continue, term()}}
              | {:stop, term(), state}
  @callback became_active(state) ::
              {:noreply, state}
              | {:noreply, state, timeout() | :hibernate | {:continue, term()}}
              | {:stop, term(), state}
  @callback became_passive(state) ::
              {:noreply, state}
              | {:noreply, state, timeout() | :hibernate | {:continue, term()}}
              | {:stop, term(), state}
  @callback handle_invalid_message(message_args, state) ::
              {:ok, state}
              | {:error, reason, state}
              | {:noreply, state}

  defmacro __using__(_opts) do
    quote do
      @behaviour Pulsar.Consumer.Callback

      # Provide default implementations for optional callbacks
      def init(_init_args), do: {:ok, nil}

      def terminate(_reason, _state), do: :ok

      def handle_call(_request, _from, state) do
        {:reply, {:error, :not_implemented}, state}
      end

      def handle_cast(_request, state) do
        {:noreply, state}
      end

      def handle_info(_message, state) do
        {:noreply, state}
      end

      def became_active(state) do
        {:noreply, state}
      end

      def became_passive(state) do
        {:noreply, state}
      end

      def handle_invalid_message(message, state) do
        require Logger

        Logger.warning("Discarding invalid message: #{message.validation_error}")

        {:ok, state}
      end

      defoverridable init: 1,
                     terminate: 2,
                     handle_call: 3,
                     handle_cast: 2,
                     handle_info: 2,
                     became_active: 1,
                     became_passive: 1,
                     handle_invalid_message: 2
    end
  end
end
