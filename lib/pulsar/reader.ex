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

  Reading always uses a non-durable subscription, so a reader keeps no position and
  starts fresh each time.

  Selecting a client, when there is more than the default:

      Pulsar.Reader.stream(topic, client: :analytics)
      |> Stream.map(&process/1)
      |> Stream.run()

  With custom flow control:

      # Request 50 messages at a time
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

  require Logger

  @default_flow_permits 100

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

      # Read from the earliest message
      Pulsar.Reader.stream("persistent://public/default/topic", start_position: :earliest)
      |> Enum.take(5)

      # Read through a named client, filtering as you go
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

    with {:ok, state} <- start_consumer(topic, Keyword.fetch!(opts, :client), opts) do
      state
    end
  end

  defp start_consumer(topic, client_name, opts) do
    flow_permits = Keyword.get(opts, :flow_permits, @default_flow_permits)
    start_position = Keyword.get(opts, :start_position, :earliest)
    start_message_id = Keyword.get(opts, :start_message_id)
    start_timestamp = Keyword.get(opts, :start_timestamp)
    read_compacted = Keyword.get(opts, :read_compacted, false)
    timeout = Keyword.get(opts, :timeout, 60_000)
    startup_delay_ms = Keyword.get(opts, :startup_delay_ms, 0)
    startup_jitter_ms = Keyword.get(opts, :startup_jitter_ms, 0)

    subscription_name = "reader-#{System.unique_integer([:positive, :monotonic])}"
    reader_ref = make_ref()

    consumer_opts = [
      client: client_name,
      subscription_type: :Exclusive,
      durable: false,
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
      {:ok, consumer_group_pid} ->
        {:ok, build_reader_state(consumer_group_pid, reader_ref, client_name, flow_permits, timeout)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp build_reader_state(consumer_group_pid, reader_ref, client_name, flow_permits, timeout) do
    consumer_pids = wait_for_consumers_ready(consumer_group_pid, reader_ref)

    Enum.each(consumer_pids, fn pid ->
      :ok = Consumer.send_flow(pid, flow_permits)
    end)

    permits_by_consumer = Map.new(consumer_pids, fn pid -> {pid, flow_permits} end)

    %{
      consumer_pids: consumer_pids,
      consumer_group_pid: consumer_group_pid,
      client_name: client_name,
      flow_permits: flow_permits,
      permits_by_consumer: permits_by_consumer,
      timeout: timeout,
      reader_ref: reader_ref,
      buffer: :queue.new()
    }
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
    case Consumer.stop(state.consumer_group_pid) do
      :ok -> :ok
      {:error, _reason} -> :ok
    end
  end

  @doc false
  @spec stream_options_docs() :: String.t()
  def stream_options_docs, do: NimbleOptions.docs(@schema)

  defp validate_options!(opts) do
    {known, unknown} = Keyword.split(opts, Keyword.keys(@schema))

    if unknown != [] do
      Logger.warning("Pulsar.Reader ignoring unknown options: #{inspect(Keyword.keys(unknown))}")
    end

    NimbleOptions.validate!(known, @schema)
  end

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

  defp wait_for_consumers_ready(consumer_group_pid, reader_ref) do
    expected_count = length(Consumer.workers(consumer_group_pid))
    collect_ready_messages(expected_count, [], 5_000, reader_ref)
  end

  defp collect_ready_messages(0, pids, _timeout, _reader_ref), do: pids

  defp collect_ready_messages(remaining, pids, timeout, reader_ref) do
    receive do
      {:reader_ready, ^reader_ref, pid} ->
        collect_ready_messages(remaining - 1, [pid | pids], timeout, reader_ref)
    after
      timeout ->
        raise "Reader failed to start: expected #{remaining + length(pids)} consumers, got #{length(pids)} within #{timeout}ms"
    end
  end
end
