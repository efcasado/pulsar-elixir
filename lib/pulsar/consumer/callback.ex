defmodule Pulsar.Consumer.Callback do
  @moduledoc """
  Behaviour for Pulsar consumer callback modules that support internal state.

  This behaviour allows consumer callback modules to maintain their own internal state
  without requiring external state management processes (like Agents or GenServers).

  ## Callback Functions

  - `init/1` - Initialize the callback module state
  - `handle_message/2` - Handle incoming messages with current state
  - `terminate/2` - Optional cleanup when consumer terminates

  ## Message Format

  `handle_message/2` receives a tuple containing the command, metadata, payload, and broker metadata:

      {command, metadata, payload, broker_metadata}

  Where:
  - `command` - %Pulsar.Protocol.Binary.Pulsar.Proto.CommandMessage{}
  - `metadata` - %Pulsar.Protocol.Binary.Pulsar.Proto.MessageMetadata{}
  - `payload` - A tuple {single_metadata, binary} where:
    - `single_metadata` - Single message metadata (nil for single messages, struct for batch messages)
    - `binary` - The actual message payload
  - `broker_metadata` - Additional broker information

  Note: Both single messages and batch messages use the same normalized format.
  For single messages, single_metadata will be nil.
  For batch messages, single_metadata contains the individual message metadata.

  You can extract message information like this:

      def handle_message({command, metadata, {single_metadata, payload}, _broker_metadata}, state) do
        message_id = {command.message_id.ledgerId, command.message_id.entryId}
        # single_metadata is nil for single messages, struct for batch messages
        # payload is always the binary message content
        {:ok, state}
      end

  ## Example Implementation

      defmodule MyApp.MessageCounter do
        @behaviour Pulsar.Consumer.Callback

        def init(opts) do
          max_messages = Keyword.get(opts, :max_messages, 1000)
          {:ok, %{count: 0, max_messages: max_messages, messages: []}}
        end

        def handle_message({_command, _metadata, {_single_metadata, payload}, _broker_metadata}, callback_state) do
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

  You can add custom functionality by implementing optional callbacks:

  - `handle_call/3` - Handle synchronous calls (GenServer.call)
  - `handle_cast/2` - Handle asynchronous casts (GenServer.cast)
  - `handle_info/2` - Handle other messages

  Example usage:

      # Get current count
      count = GenServer.call(consumer_pid, :get_count)
      
      # Reset state
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

  ### `init/1`

  - `{:ok, state}` - Successful initialization with initial state
  - `{:error, reason}` - Initialization failed

  ### `handle_message/2`

  - `{:ok, new_state}` - Message processed successfully, acknowledge message
  - `{:error, reason, new_state}` - Processing failed, don't acknowledge message
  - `{:stop, new_state}` - Message processed successfully, acknowledge, then stop consumer

  ### `terminate/2`

  - `:ok` - Cleanup completed successfully
  - Any other value is ignored
  """

  @type message_args :: {
          command :: struct(),
          metadata :: struct(),
          payload :: {single_metadata :: struct() | nil, binary()},
          broker_metadata :: term()
        }

  @type init_arg :: term()
  @type state :: term()
  @type reason :: term()

  @callback init(init_arg) :: {:ok, state} | {:error, reason}
  @callback handle_message(message_args, state) ::
              {:ok, state}
              | {:error, reason, state}
              | {:stop, state}

  @optional_callbacks terminate: 2, handle_call: 3, handle_cast: 2, handle_info: 2
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
end
