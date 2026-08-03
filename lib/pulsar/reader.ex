defmodule Pulsar.Reader do
  @moduledoc """
  A high-level interface for reading messages from Pulsar topics using
  Elixir's Stream API. The reader uses non-durable subscriptions, meaning
  it doesn't persist its position and starts fresh on each connection.

  ## Usage

  A reader reads through a client, so there has to be one running. In an application it
  belongs in your supervision tree; in a script, start it directly:

      {:ok, _pid} = Pulsar.Client.start_link(host: "pulsar://localhost:6650")

      Pulsar.Reader.stream("persistent://public/default/my-topic", start_position: :earliest)
      |> Stream.take(10)
      |> Stream.each(&IO.inspect(&1.payload))
      |> Stream.run()

      :ok = Pulsar.Client.stop(:default)

  Reading always uses a non-durable subscription, so a reader keeps no position and
  starts fresh each time.

  Selecting a client, when there is more than the default:

      Pulsar.Reader.stream(topic, client: :analytics)
      |> Stream.map(&process/1)
      |> Stream.run()

  With custom flow control:

      Pulsar.Reader.stream(topic, flow_permits: 50) |> Enum.take(100)

  Reading from a specific message:

      # {ledger_id, entry_id}
      Pulsar.Reader.stream(topic, start_message_id: {123, 456}) |> Enum.take(10)

  ## Options

  See `stream/2`.

  ## Partitioned Topics

  The Reader supports partitioned topics. When reading from a partitioned topic,
  messages from all partitions are merged into a single stream. **Note that message
  ordering across partitions is not guaranteed** - messages may arrive interleaved
  from different partitions.

  If you need per-partition ordering, consider using separate Reader streams for
  each partition (e.g., `"persistent://tenant/ns/topic-partition-0"`).

  ## Process Ownership

  The stream is bound to the process that creates it. Messages are delivered to
  the creating process's mailbox, so you cannot pass the stream to another process
  for consumption.

  For multi-process consumption patterns, use the `Pulsar.Consumer` API directly
  or consider [off_broadway_pulsar](https://github.com/efcasado/off_broadway_pulsar)
  for Broadway-based pipelines.

  ## Stream Termination

  The stream terminates when any of these conditions is met:
  - The consumer receives all requested messages (e.g., via `Enum.take/2`)
  - The inactivity timeout is reached (default: 60 seconds)
  - The stream is halted by downstream processing
  """

  alias Pulsar.Consumer
  alias Pulsar.Topology

  @default_startup_timeout 5_000

  @schema [
    client: [
      type: :atom,
      default: :default,
      doc: "The client to read through."
    ],
    start_position: [
      type: {:in, [:earliest, :latest]},
      default: :earliest,
      doc: "Where to start reading when no message id or timestamp is given."
    ],
    start_message_id: [
      type: {:tuple, [:non_neg_integer, :non_neg_integer]},
      doc: "Start from a `{ledger_id, entry_id}`."
    ],
    start_timestamp: [
      type: :non_neg_integer,
      doc: "Start from a publish time, in milliseconds since the epoch."
    ],
    read_compacted: [
      type: :boolean,
      default: false,
      doc: "Read only the latest value per key from a compacted topic."
    ],
    flow_permits: [
      type: :pos_integer,
      default: 100,
      doc: "Messages to request from the broker at a time."
    ],
    timeout: [
      type: :timeout,
      default: 60_000,
      doc: "Milliseconds without a message after which the stream halts."
    ],
    startup_timeout: [
      type: :timeout,
      default: @default_startup_timeout,
      doc: "Milliseconds to wait for topology discovery and consumer initialization."
    ],
    startup_delay_ms: [
      type: :non_neg_integer,
      default: 0,
      doc: "Delay before the underlying consumer subscribes."
    ],
    startup_jitter_ms: [
      type: :non_neg_integer,
      default: 0,
      doc: "Random delay added to `:startup_delay_ms`."
    ]
  ]

  @doc """
  Creates a stream of messages from a Pulsar topic.

  Returns a `Stream` that yields `Pulsar.Message` structs. If initialization
  fails, the stream emits `{:error, reason}` as the first (and only) element.

  ## Options

  #{NimbleOptions.docs(@schema)}

  ## Examples

      Pulsar.Reader.stream("persistent://public/default/topic", start_position: :earliest)
      |> Enum.take(5)

      Pulsar.Reader.stream("persistent://public/default/topic",
        client: :analytics,
        start_position: :latest
      )
      |> Stream.filter(&interesting?/1)
      |> Enum.to_list()

      # Handle errors (emitted as first element if initialization fails)
      Pulsar.Reader.stream("persistent://public/default/topic", client: :not_running)
      |> Enum.take(1)
      |> case do
        [{:error, reason}] -> Logger.error("Failed: \#{inspect(reason)}")
        messages -> process(messages)
      end
  """
  @spec stream(String.t(), keyword()) :: Enumerable.t()
  def stream(topic, opts \\ []) do
    Stream.resource(
      fn -> start_reader(topic, opts) end,
      fn state -> next_message(state) end,
      fn state -> stop_reader(state) end
    )
  end

  defp start_reader(topic, opts) do
    opts = validate_options!(opts)

    case start_consumer(topic, Keyword.fetch!(opts, :client), opts) do
      {:ok, state} -> state
      {:error, _reason} = error -> error
    end
  end

  defp start_consumer(topic, client_name, opts) do
    flow_permits = Keyword.fetch!(opts, :flow_permits)
    start_position = Keyword.fetch!(opts, :start_position)
    start_message_id = Keyword.get(opts, :start_message_id)
    start_timestamp = Keyword.get(opts, :start_timestamp)
    read_compacted = Keyword.fetch!(opts, :read_compacted)
    timeout = Keyword.fetch!(opts, :timeout)
    startup_timeout = Keyword.fetch!(opts, :startup_timeout)
    startup_delay_ms = Keyword.fetch!(opts, :startup_delay_ms)
    startup_jitter_ms = Keyword.fetch!(opts, :startup_jitter_ms)

    subscription_name = "reader-#{System.unique_integer([:positive, :monotonic])}"
    reader_ref = make_ref()

    consumer_opts = [
      client: client_name,
      subscription_type: :Exclusive,
      durable: false,
      consumer_count: 1,
      initial_position: start_position,
      read_compacted: read_compacted,
      flow_initial: 0,
      startup_delay_ms: startup_delay_ms,
      startup_jitter_ms: startup_jitter_ms,
      init_args: [self(), reader_ref]
    ]

    # Absent rather than nil, so they read as "not given" instead of "seek to nil".
    consumer_opts =
      consumer_opts
      |> maybe_put(:start_message_id, start_message_id)
      |> maybe_put(:start_timestamp, start_timestamp)

    case Consumer.start(topic, subscription_name, Pulsar.Reader.Callback, consumer_opts) do
      {:ok, consumer} ->
        case build_reader_state(consumer, client_name, reader_ref, flow_permits, timeout, startup_timeout) do
          {:ok, state} ->
            {:ok, state}

          {:error, _reason} = error ->
            stop_consumer(consumer, client_name)
            error
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp build_reader_state(consumer, client_name, reader_ref, flow_permits, timeout, startup_timeout) do
    startup_deadline = deadline(startup_timeout)

    with :ok <- wait_for_topology(consumer, startup_deadline),
         {:ok, consumer_pids} <- wait_for_consumers_ready(consumer, reader_ref, startup_deadline),
         :ok <- Consumer.send_flow(consumer, flow_permits) do
      permits_by_consumer = Map.new(consumer_pids, fn pid -> {pid, flow_permits} end)

      {:ok,
       %{
         client: client_name,
         consumer_pids: consumer_pids,
         consumer_group_pid: consumer,
         flow_permits: flow_permits,
         permits_by_consumer: permits_by_consumer,
         timeout: timeout,
         reader_ref: reader_ref,
         buffer: :queue.new()
       }}
    end
  end

  defp next_message({:error, reason}) do
    {[{:error, reason}], :halted}
  end

  defp next_message(:halted) do
    {:halt, :halted}
  end

  defp next_message(state) do
    case :queue.out(state.buffer) do
      {{:value, {consumer_pid, message}}, new_buffer} ->
        new_state = %{state | buffer: new_buffer}
        new_state = decrement_permits(new_state, consumer_pid)
        new_state = maybe_refill_flow(new_state, consumer_pid)
        {[message], new_state}

      {:empty, _buffer} ->
        reader_ref = state.reader_ref

        receive do
          {:pulsar_message, ^reader_ref, consumer_pid, message} ->
            new_buffer = :queue.in({consumer_pid, message}, state.buffer)
            next_message(%{state | buffer: new_buffer})
        after
          state.timeout ->
            {:halt, state}
        end
    end
  end

  defp stop_reader(:halted), do: :ok

  defp stop_reader(state) do
    case Consumer.stop(state.consumer_group_pid, client: state.client) do
      :ok -> :ok
      {:error, _reason} -> :ok
    end
  end

  @doc false
  @spec stream_options_docs() :: String.t()
  def stream_options_docs, do: NimbleOptions.docs(@schema)

  defp validate_options!(opts), do: NimbleOptions.validate!(opts, @schema)

  defp maybe_put(keyword, _key, nil), do: keyword
  defp maybe_put(keyword, key, value), do: Keyword.put(keyword, key, value)

  defp decrement_permits(state, consumer_pid) do
    current = Map.get(state.permits_by_consumer, consumer_pid, 0)
    new_permits = Map.put(state.permits_by_consumer, consumer_pid, max(current - 1, 0))
    %{state | permits_by_consumer: new_permits}
  end

  defp maybe_refill_flow(state, consumer_pid) do
    current_permits = Map.get(state.permits_by_consumer, consumer_pid, 0)
    threshold = div(state.flow_permits, 2)

    if current_permits <= threshold do
      :ok = Consumer.send_flow(consumer_pid, state.flow_permits)
      new_permits = Map.put(state.permits_by_consumer, consumer_pid, current_permits + state.flow_permits)
      %{state | permits_by_consumer: new_permits}
    else
      state
    end
  end

  defp wait_for_topology(consumer, startup_deadline) do
    case Topology.await_ready(consumer, remaining(startup_deadline)) do
      :ok -> :ok
      {:error, _reason} -> {:error, :reader_start_timeout}
    end
  end

  defp wait_for_consumers_ready(consumer, reader_ref, startup_deadline) do
    collect_ready_messages(consumer, reader_ref, %{}, startup_deadline)
  end

  defp collect_ready_messages(consumer, reader_ref, ready, startup_deadline) do
    case ready_consumers(consumer, ready) do
      {:ok, consumers} ->
        {:ok, consumers}

      :waiting ->
        receive_ready(consumer, reader_ref, ready, startup_deadline)
    end
  end

  defp ready_consumers(consumer, ready) do
    case expected_consumer_count(consumer) do
      {:ok, current_expected_count} ->
        current_ready_consumers(consumer, current_expected_count, ready)

      {:error, :initializing} ->
        :waiting
    end
  end

  defp expected_consumer_count(consumer) do
    case Topology.groups(consumer) do
      [] -> {:error, :initializing}
      groups -> {:ok, length(groups)}
    end
  end

  defp current_ready_consumers(consumer, expected_count, ready) do
    consumers = Topology.workers(consumer)

    if length(consumers) == expected_count and Enum.all?(consumers, &Map.has_key?(ready, &1)) do
      {:ok, consumers}
    else
      :waiting
    end
  end

  defp receive_ready(consumer, reader_ref, ready, startup_deadline) do
    case remaining(startup_deadline) do
      0 ->
        {:error, :reader_start_timeout}

      timeout ->
        receive do
          {:reader_ready, ^reader_ref, pid} ->
            ready = Map.put(ready, pid, true)
            collect_ready_messages(consumer, reader_ref, ready, startup_deadline)
        after
          timeout -> {:error, :reader_start_timeout}
        end
    end
  end

  defp deadline(:infinity), do: :infinity
  defp deadline(timeout), do: System.monotonic_time(:millisecond) + timeout

  defp remaining(:infinity), do: :infinity

  defp remaining(deadline) do
    max(deadline - System.monotonic_time(:millisecond), 0)
  end

  defp stop_consumer(consumer, client_name) do
    Consumer.stop(consumer, client: client_name)
  catch
    :exit, _reason -> :ok
  end
end
