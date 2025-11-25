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

  ## Message Format

  `handle_message/2` receives a tuple containing the command, metadata, payload, broker metadata, and message_id_to_ack:

      {command, metadata, payload, broker_metadata, message_id_to_ack}

  Where:
  - `command` - %Pulsar.Protocol.Binary.Pulsar.Proto.CommandMessage{}
  - `metadata` - %Pulsar.Protocol.Binary.Pulsar.Proto.MessageMetadata{}
  - `payload` - A tuple {single_metadata, binary} where:
    - `single_metadata` - Single message metadata (nil for single messages, struct for batch messages)
    - `binary` - The actual message payload
  - `broker_metadata` - Additional broker information
  - `message_id_to_ack` - The message ID to use for ACK/NACK (includes batch_index if from a batch)

  Note: Both single messages and batch messages use the same normalized format.
  For single messages, single_metadata will be nil.
  For batch messages, single_metadata contains the individual message metadata.

  You can extract message information like this:

      def handle_message({command, metadata, {single_metadata, payload}, _broker_metadata, message_id_to_ack}, state) do
        # Use message_id_to_ack for manual ACK/NACK
        # payload is always the binary message content
        {:ok, state}
      end

  ## Example Implementation

      defmodule MyApp.MessageCounter do
        use Pulsar.Consumer.Callback

        def init(opts) do
          max_messages = Keyword.get(opts, :max_messages, 1000)
          {:ok, %{count: 0, max_messages: max_messages, messages: []}}
        end

      def handle_message({command, _metadata, {_single_metadata, payload}, _broker_metadata, _message_id_to_ack}, callback_state) do
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
        
        # Optional: Add custom GenServer calls for external access
        def handle_call(:get_count, _from, callback_state) do
          {:reply, callback_state.count, callback_state}
        end
        
        def handle_call(:get_messages, _from, callback_state) do
          {:reply, Enum.reverse(callback_state.messages), callback_state}
        end
        
        def handle_cast(:reset, callback_state) do
          {:noreply, %{callback_state | count: 0, messages: []}}
        end
      end

  ## Extending the Consumer

  The consumer callback module has default implementations for all optional callbacks.
  You can override these by simply defining the callback in your module:

      defmodule MyApp.MessageCounter do
        use Pulsar.Consumer.Callback

        def init(opts) do
          {:ok, %{count: 0, max: Keyword.get(opts, :max, 100)}}
        end

        def handle_message(message, state) do
          {:ok, %{state | count: state.count + 1}}
        end

        # Override default handle_call to add custom functionality
        def handle_call(:get_count, _from, state) do
          {:reply, state.count, state}
        end

        # Override default handle_cast for custom async operations
        def handle_cast(:reset, state) do
          {:noreply, %{state | count: 0}}
        end
      end

  Example usage:

      # Get current count via custom handle_call
      count = GenServer.call(consumer_pid, :get_count)
      
      # Reset state via custom handle_cast
      GenServer.cast(consumer_pid, :reset)

  ## Multiple Consumers

  For shared or key-shared subscriptions, you can start multiple consumer processes
  to increase throughput:

      # Start 3 consumers for shared processing
      {:ok, consumer_pids} = Pulsar.start_consumer(
        topic,
        subscription,
        :Key_Shared,
        MyCallback,
        consumer_count: 3
      )
      
      # Each consumer maintains its own independent state
      Enum.each(consumer_pids, fn consumer_pid ->
        count = GenServer.call(consumer_pid, :get_count)
        IO.puts("Consumer \#{inspect(consumer_pid)} processed \#{count} messages")
      end)

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

  ## Manual Acknowledgment

  When you return `{:noreply, state}` from `handle_message/2`, the message will NOT be automatically
  acknowledged or negatively acknowledged. This gives you full control over when and how to ACK/NACK messages,
  which is useful for:

  - Broadway pipelines that handle acknowledgment in batches
  - Async processing where acknowledgment happens after the callback returns
  - Custom acknowledgment logic based on downstream processing

  Example with manual ACK:

      def handle_message({command, _metadata, {_single_metadata, payload}, _broker_metadata, message_id_to_ack}, state) do
        # Use message_id_to_ack (not command.message_id) for manual ACK/NACK
        # This ensures batch messages are ACKed with the correct batch_index
        
        # Send to async processor
        Task.async(fn ->
          case process_async(payload) do
            :ok -> Pulsar.Consumer.ack(self(), message_id_to_ack)
            {:error, _} -> Pulsar.Consumer.nack(self(), message_id_to_ack)
          end
        end)
        
        {:noreply, state}
      end
  """

  @type message_args :: {
          command :: struct(),
          metadata :: struct(),
          payload :: {single_metadata :: struct() | nil, binary()},
          broker_metadata :: term(),
          message_id_to_ack :: term()
        }

  @type init_arg :: term()
  @type state :: term()
  @type reason :: term()

  @callback init(init_arg) :: {:ok, state} | {:error, reason}
  @callback handle_message(message_args, state) ::
              {:ok, state}
              | {:error, reason, state}
              | {:noreply, state}
              | {:stop, state}

  @optional_callbacks init: 1, terminate: 2, handle_call: 3, handle_cast: 2, handle_info: 2
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

      defoverridable init: 1, terminate: 2, handle_call: 3, handle_cast: 2, handle_info: 2
    end
  end
end
