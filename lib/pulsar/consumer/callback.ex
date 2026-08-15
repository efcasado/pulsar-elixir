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
  - `became_active/1` - Called when this consumer becomes the active consumer of a `:failover` subscription (default: `{:noreply, state}`)
  - `became_passive/1` - Called when this consumer becomes a passive (standby) consumer of a `:failover` subscription (default: `{:noreply, state}`)
  - `handle_invalid_message/2` - Called instead of `handle_message/2` for a message whose
    bytes could not be trusted (default: log a warning and acknowledge it, so it is not
    redelivered). Override it to record or divert such messages; see `Pulsar.Message.valid?/1`
    for what they contain. Because they are routed here, `handle_message/2` only ever
    receives messages that arrived intact.
  - `handle_permits/2` - Called after every delivery with the permits it consumed and the
    consumer's flow window before any refill. Its default implementation applies the configured
    automatic refill policy; overriding it replaces that policy. See `## Flow Control`.

  These defaults come from `use Pulsar.Consumer.Callback`. A module taking the behaviour without
  it has to write them itself, since the consumer calls each one directly. `handle_permits/2` is
  the one it cannot skip — every delivery goes through it, so it is left out of
  `@optional_callbacks` and the compiler says so rather than letting the consumer fail on its
  first message.

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

        def init(opts, _context) do
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
  - `{:stop, new_state}` - Message processed successfully, acknowledge, then stop consumer

  ### `handle_permits/2` (Optional)

  Takes a `t:flow_context/0` and the current state. The default implementation grants
  `:refill` when an `:auto` consumer's `:outstanding` permits reach `:threshold`; call
  `super(flow, state)` to observe flow without replacing it. See `## Flow Control`.

  - `{:ok, new_state}` - Grant nothing
  - `{:grant, permits, new_state}` - Grant `permits` more to the broker

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

  ## Flow Control

  After every broker delivery the consumer calls `handle_permits/2` once, with the total
  permits that delivery consumed, however many message callbacks it triggered — including none.
  `:outstanding` is the window after that consumption and before any grant returned here.

  This matters for batches and dead-letter policies. The broker also charges for batch members
  excluded by an `ack_set`, messages compacted away, and deliveries diverted to a dead letter
  topic. Counting only callback-visible messages eventually overestimates the broker's window.

  The default implementation is the `flow_policy: :auto` policy. Override it to replace the
  configured threshold/refill rule:

      defmodule MyApp.PacedConsumer do
        use Pulsar.Consumer.Callback

        def handle_permits(%{outstanding: outstanding}, state) when outstanding <= 20 do
          {:grant, 50, state}
        end

        def handle_permits(_flow, state), do: {:ok, state}

        def handle_message(_message, state), do: {:ok, state}
      end

  Under `flow_policy: :manual` the default grants nothing and every refill is yours. `:flow_initial`
  is still granted on subscribe under either policy, so a manual consumer can start with a window
  and manage it from there, and a replacement worker comes back with the same one. An override can
  answer here with `{:grant, permits, state}`, or forward `flow.consumed` to its owning process,
  return `{:ok, state}`, and have that process grant later with `Pulsar.Consumer.send_flow/2`.

  Grant through the return value rather than `Pulsar.Consumer.send_flow/2`. This callback runs
  in the consumer process, so calling `send_flow/2` on that consumer from here deadlocks.
  Handing the decision to another process, which then calls `send_flow/2`, is fine as long as
  that process is not itself waiting on this one.

  Neither granting from this callback nor `Pulsar.Consumer.send_flow/2` calls it again: it
  reports broker consumption, not grants.
  """

  @type message_args :: Pulsar.Message.t()

  @type init_arg :: term()
  @type state :: term()
  @type reason :: term()

  @typedoc """
  Flow accounting for one broker delivery, passed to `c:handle_permits/2`.

  - `:consumed` - total broker permits consumed by the delivery, including messages no message
    callback received
  - `:outstanding` - permits left after that consumption and before a callback-requested grant
  - `:threshold` - configured automatic refill threshold
  - `:refill` - configured automatic refill amount
  - `:mode` - the consumer's `:flow_policy`, `:auto` or `:manual`
  """
  @type flow_context :: %{
          consumed: non_neg_integer(),
          outstanding: non_neg_integer(),
          threshold: non_neg_integer(),
          refill: non_neg_integer(),
          mode: :auto | :manual
        }

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
              | {:stop, state}

  @optional_callbacks init: 2,
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
  @callback handle_permits(flow_context, state) ::
              {:ok, state}
              | {:grant, pos_integer(), state}

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

      def handle_permits(%{mode: :auto, outstanding: outstanding, threshold: threshold, refill: refill}, state)
          when outstanding <= threshold and refill > 0 do
        {:grant, refill, state}
      end

      def handle_permits(_flow, state) do
        {:ok, state}
      end

      defoverridable init: 2,
                     terminate: 2,
                     handle_call: 3,
                     handle_cast: 2,
                     handle_info: 2,
                     became_active: 1,
                     became_passive: 1,
                     handle_invalid_message: 2,
                     handle_permits: 2
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
