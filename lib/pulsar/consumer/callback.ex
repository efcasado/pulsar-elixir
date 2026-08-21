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

  - `init/2` - Initialize the callback module state, given the consumer's resolved topic and
    subscription (default: `{:ok, nil}`)
  - `terminate/2` - Cleanup when consumer terminates (default: `:ok`)
  - `handle_call/3` - Handle synchronous calls (default: `{:reply, {:error, :not_implemented}, state}`)
  - `handle_cast/2` - Handle asynchronous casts (default: `{:noreply, state}`)
  - `handle_info/2` - Handle other messages (default: `{:noreply, state}`)
  - `became_active/1` - Called when this consumer becomes the active consumer of a `:failover` subscription (default: `{:ok, state}`)
  - `became_passive/1` - Called when this consumer becomes a passive (standby) consumer of a `:failover` subscription (default: `{:ok, state}`)
  - `reached_end_of_topic/1` - Called when this consumer has drained a terminated topic (default: `{:ok, state}`)
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
        producer = Pulsar.Message.producer_name(message)
        {:ok, state}
      end

  ## Example Implementation

      defmodule MyApp.MessageCounter do
        use Pulsar.Consumer.Callback

        def init(_opts, _context) do
          {:ok, %{count: 0, messages: []}}
        end

        def handle_message(%Pulsar.Message{payload: payload}, callback_state) do
          new_state = %{
            callback_state
            | count: callback_state.count + 1,
              messages: [payload | callback_state.messages]
          }

          {:ok, new_state}
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
          subscription_type: :key_shared,
          consumer_count: 3,
          name: :orders]
       ]}

  Each process has independent callback state. The consumer facade intentionally hides
  those short-lived worker processes, so state that must be shared, queried, or aggregated
  across consumers should live in an application process with its own public API.

  ## Return Values

  ### `init/2` (Optional)

  If not implemented, defaults to `{:ok, nil}`.

  - `{:ok, state}` - Successful initialization with initial state
  - `{:error, reason}` - Initialization failed

  Its second argument is the consumer's resolved context; see `## Consumer Context` below.

  ### `handle_message/2`

  - `{:ok, new_state}` - Message processed successfully, acknowledge message automatically
  - `{:error, reason, new_state}` - Processing failed, track for redelivery
  - `{:noreply, new_state}` - Message processed, but don't automatically ACK/NACK. Use `Pulsar.Consumer.ack/2` or `Pulsar.Consumer.nack/2` for manual acknowledgment
  - `{:stop, reason, new_state}` - Message processed successfully, acknowledge it, then finish
    this worker with `reason`

  A worker is transient: `:normal`, `:shutdown`, and `{:shutdown, term}` finish it without a
  restart. Any other stop reason is a failure and supervision restarts it. Stopping one worker
  does not stop the logical consumer; use `Pulsar.Consumer.stop/2` to remove the whole resource.

  Stopping partway through a broker batch follows the normal batch acknowledgement rules. Without
  `:batch_index_ack_enabled`, the processed prefix remains outstanding with the unread suffix and
  may be redelivered to another worker later. With batch-index acknowledgements enabled, each
  processed message is acknowledged before the worker stops.

  `handle_invalid_message/2` accepts the same results. Its `{:ok, ...}` and `{:stop, ...}`
  results acknowledge the invalid message with its validation error; `{:noreply, ...}` leaves
  acknowledgement to the callback, and `{:error, ...}` tracks it for redelivery.

  ### Notification callbacks

  `became_active/1`, `became_passive/1`, and `reached_end_of_topic/1` return `{:ok, state}` to
  carry on or `{:stop, reason, state}` to finish the worker. The former `{:noreply, state}` and
  `{:noreply, state, timeout}` forms remain accepted for compatibility.

  ### `terminate/2` (Optional)

  If not implemented, defaults to `:ok`.

  - `:ok` - Cleanup completed successfully
  - Any other value is ignored

  ## Consumer Context

  A consumer on a partitioned topic subscribes to one partition of it, so several callback
  processes share a configured topic while each handles a different partition. `init/2`
  receives which one this process ended up on:

      %{
        topic: "persistent://public/default/orders-partition-2",
        base_topic: "persistent://public/default/orders",
        partition: 2,
        subscription_name: "order-service",
        subscription_type: :shared,
        consumer_name: "orders-order-service-partition-2-1"
      }

  `:topic` is what this consumer subscribed to and `:base_topic` what it was configured with;
  they are equal, and `:partition` is `nil`, when the topic is not partitioned. Use `:topic`
  as a per-partition key for metrics or flow accounting, and `:base_topic` where business
  logic cares about the logical topic.

  A consumer configured with an already concrete partition topic such as
  `"orders-partition-2"` reports `partition: nil`, because that is what the broker reports:
  metadata for a concrete partition gives a partition count of zero, so the consumer is not a
  partitioned one. The index is not recovered from the name, since a topic legitimately named
  `"events-partition-3"` would otherwise be given a partition it does not have. Such a consumer
  still gets its topic in `:topic`; only the index is unavailable.

  Keep whatever you need in your state rather than looking it up per message:

      defmodule MyApp.PerPartitionMetrics do
        use Pulsar.Consumer.Callback

        def init(_init_args, context) do
          {:ok, %{topic: context.topic, partition: context.partition, count: 0}}
        end

        def handle_message(%Pulsar.Message{}, state) do
          :telemetry.execute([:my_app, :message], %{count: 1}, %{topic: state.topic})
          {:ok, %{state | count: state.count + 1}}
        end
      end

  ## Failover Consumer Events

  For `:failover` subscriptions, `became_active/1` and `became_passive/1`
  notify a consumer when the broker promotes it to (or demotes it from) the
  active role. Useful for metrics/alerting or warming up state before a
  takeover; not meaningful for other subscription types. A passive consumer
  never receives messages, so there is nothing to pause or resume.

  ## End of Topic

  `reached_end_of_topic/1` is called when the consumer has read to the end of a topic that
  has been terminated, meaning no further message will ever be published to it. The default
  implementation keeps the consumer running, which is what a partitioned consumer wants:
  each partition reaches its end separately, and the others still have messages to deliver.

  Every consumer on the subscription is told, not just whichever one read last: each of the
  `:consumer_count` processes on a shared subscription hears it, both the active and the
  passive consumers of a failover one, and each partition's process separately. A shutdown
  that waits for the whole topic waits for every worker to report, not for the first.

  A worker can be told more than once. Unacknowledged messages are redelivered after the
  notification, and draining them reaches the end again; so does every reconnect, since the
  broker sends it to each new session on a terminated topic. Track which workers have
  reported rather than counting notifications, and keep whatever the callback does here
  idempotent.

  This says the topic is finished, not the subscription. A callback that only needs this worker
  can finish it directly:

      def reached_end_of_topic(state) do
        {:stop, :normal, state}
      end

  That leaves the worker absent and the logical consumer running, possibly with fewer workers.
  To remove the whole consumer, tell a coordinator and have it call `Pulsar.Consumer.stop/2`
  once it has heard from every worker it expects - one per partition and `:consumer_count`.

  ## Manual Acknowledgment

  When you return `{:noreply, state}` from `handle_message/2`, the message will NOT be automatically
  acknowledged or negatively acknowledged. This gives you full control over when and how to ACK/NACK messages,
  which is useful for:

  - Broadway pipelines that handle acknowledgment in batches
  - Async processing where acknowledgment happens after the callback returns
  - Custom acknowledgment logic based on downstream processing

  Example with manual ACK:

      def handle_message(%Pulsar.Message{payload: payload, message_id: message_id}, state) do
        consumer = self()

        # Send to async processor
        Task.start(fn ->
          case process_async(payload) do
            :ok -> Pulsar.Consumer.ack(consumer, message_id)
            {:error, _} -> Pulsar.Consumer.nack(consumer, message_id)
          end
        end)

        {:noreply, state}
      end
  """

  @type message_args :: Pulsar.Message.t()

  @type init_arg :: term()
  @type state :: term()
  @type reason :: term()
  @type stop_result :: {:stop, reason, state}
  @type event_result ::
          {:ok, state}
          | {:noreply, state}
          | {:noreply, state, timeout() | :hibernate | {:continue, term()}}
          | stop_result

  @typedoc """
  The consumer's resolved identity, passed to `c:init/2`.

  - `:topic` - the topic this consumer subscribed to, which for a partitioned topic is one
    concrete partition
  - `:base_topic` - the topic the consumer was configured with; equals `:topic` unless the
    topic is partitioned
  - `:partition` - the partition index, or `nil` when the topic is not partitioned
  - `:subscription_name` - the subscription this consumer belongs to
  - `:subscription_type` - how that subscription is shared
  - `:consumer_name` - this worker's broker-visible name, which is its group's name suffixed
    with the worker's position in it, not the `:name` the consumer was configured with
  """
  @type context :: %{
          topic: String.t(),
          base_topic: String.t(),
          partition: non_neg_integer() | nil,
          subscription_name: String.t(),
          subscription_type: atom(),
          consumer_name: String.t() | nil
        }

  @callback init(init_arg, context) :: {:ok, state} | {:error, reason}
  @callback handle_message(message_args, state) ::
              {:ok, state}
              | {:error, reason, state}
              | {:noreply, state}
              | stop_result

  @optional_callbacks init: 2,
                      terminate: 2,
                      handle_call: 3,
                      handle_cast: 2,
                      handle_info: 2,
                      became_active: 1,
                      became_passive: 1,
                      reached_end_of_topic: 1,
                      handle_invalid_message: 2
  @callback terminate(reason, state) :: term()
  @callback handle_call(term(), GenServer.from(), state) ::
              {:reply, term(), state}
              | {:reply, term(), state, timeout() | :hibernate | {:continue, term()}}
              | {:noreply, state}
              | {:noreply, state, timeout() | :hibernate | {:continue, term()}}
              | {:stop, reason, term(), state}
              | stop_result
  @callback handle_cast(term(), state) ::
              {:noreply, state}
              | {:noreply, state, timeout() | :hibernate | {:continue, term()}}
              | stop_result
  @callback handle_info(term(), state) ::
              {:noreply, state}
              | {:noreply, state, timeout() | :hibernate | {:continue, term()}}
              | stop_result
  @callback became_active(state) :: event_result
  @callback became_passive(state) :: event_result
  @callback reached_end_of_topic(state) :: event_result
  @callback handle_invalid_message(message_args, state) ::
              {:ok, state}
              | {:error, reason, state}
              | {:noreply, state}
              | stop_result

  defmacro __using__(_opts) do
    quote do
      @behaviour Pulsar.Consumer.Callback
      @before_compile Pulsar.Consumer.Callback

      # Provide default implementations for optional callbacks
      def init(_init_args, _context), do: {:ok, nil}

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
        {:ok, state}
      end

      def became_passive(state) do
        {:ok, state}
      end

      def reached_end_of_topic(state) do
        {:ok, state}
      end

      def handle_invalid_message(message, state) do
        require Logger

        Logger.warning("Discarding invalid message: #{message.validation_error}")

        {:ok, state}
      end

      defoverridable init: 2,
                     terminate: 2,
                     handle_call: 3,
                     handle_cast: 2,
                     handle_info: 2,
                     became_active: 1,
                     became_passive: 1,
                     reached_end_of_topic: 1,
                     handle_invalid_message: 2
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    if Module.defines?(env.module, {:init, 1}) do
      raise CompileError,
        file: env.file,
        line: env.line,
        description: """
        #{inspect(env.module)} defines init/1, which is no longer a Pulsar.Consumer.Callback \
        callback and would never be called. Take the consumer's context as a second argument:

            def init(init_args, _context) do

        See Pulsar.Consumer.Callback for what the context contains.\
        """
    end
  end
end
