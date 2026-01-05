defmodule Pulsar.Reader do
  @moduledoc """
  A high-level interface for reading messages from Pulsar topics using
  Elixir's Stream API. The reader uses non-durable subscriptions, meaning
  it doesn't persist its position and starts fresh on each connection.

  ## Usage

  Basic usage with automatic connection:

      # Read 10 messages from earliest
      Pulsar.Reader.stream("persistent://public/default/my-topic",
        host: "pulsar://localhost:6650",
        start_position: :earliest
      )
      |> Stream.take(10)
      |> Enum.each(fn message ->
        IO.inspect(message.payload)
      end)

  Using an external client (recommended for production):

      # In your application supervision tree
      {:ok, _pid} = Pulsar.start(host: "pulsar://localhost:6650")

      # Later, in your code
      Pulsar.Reader.stream("persistent://public/default/my-topic",
        client: :default,
        start_position: :earliest
      )
      |> Stream.map(fn message -> process(message) end)
      |> Stream.run()

  With custom flow control:

      Pulsar.Reader.stream("persistent://public/default/my-topic",
        host: "pulsar://localhost:6650",
        flow_permits: 50  # Request 50 messages at a time
      )
      |> Enum.take(100)

  Reading from a specific message:

      Pulsar.Reader.stream("persistent://public/default/my-topic",
        host: "pulsar://localhost:6650",
        start_message_id: {123, 456}  # {ledger_id, entry_id}
      )
      |> Stream.each(&process/1)
      |> Stream.run()

  ## Options

  - `:host` - Pulsar broker URL (e.g., "pulsar://localhost:6650").
    If provided, creates a temporary client for this stream. Mutually exclusive with `:client`.
  - `:name` - Name for the internal client (default: `:default`). Only used with `:host`.
    Use this to avoid conflicts when running multiple readers with `:host`.
  - `:auth` - Authentication configuration (only used with `:host`)
  - `:socket_opts` - Socket options (only used with `:host`)
  - `:client` - Name of existing Pulsar client to use (default: `:default`).
    Use this when Pulsar is already started in your supervision tree. Mutually exclusive with `:host`.
  - `:start_position` - Where to start reading (`:earliest` or `:latest`, default: `:earliest`)
  - `:start_message_id` - Start from specific message ID as `{ledger_id, entry_id}` tuple
  - `:start_timestamp` - Start from specific timestamp (milliseconds since epoch)
  - `:flow_permits` - Number of messages to request per flow command (default: 100)
  - `:read_compacted` - Only read non-deleted messages from compacted topics (default: false)
  - `:timeout` - Inactivity timeout in milliseconds (default: `60_000`). Stream halts if
    no message is received within this time.

  ## Connection Management

  ### Internal Client Mode (host)
  When `:host` is provided, the stream creates a temporary client that lives
  only for the duration of the stream. The connection is automatically closed when
  the stream completes or is halted.

      Pulsar.Reader.stream(topic, host: "pulsar://localhost:6650")
      |> Enum.take(10)
      # Connection automatically closed after consuming 10 messages

  ### External Client Mode (client)
  When `:client` is provided (or defaulted), the stream uses an existing client
  from your application's supervision tree. The client remains running after
  the stream completes.

      # In your application.ex
      children = [
        {Pulsar, host: "pulsar://localhost:6650"}
      ]

      # Use the existing client
      Pulsar.Reader.stream(topic, client: :default)
      |> Enum.take(10)
      # Client remains running

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

  @default_flow_permits 100

  @doc """
  Creates a stream of messages from a Pulsar topic.

  Returns a `Stream` that yields `Pulsar.Message` structs. The stream handles
  connection lifecycle automatically based on whether you provide `:host`
  or `:client`.

  ## Examples

      # Read with internal connection (closes automatically)
      Pulsar.Reader.stream("persistent://public/default/topic",
        host: "pulsar://localhost:6650",
        start_position: :earliest
      )
      |> Enum.take(5)

      # Read with external client (remains open)
      Pulsar.Reader.stream("persistent://public/default/topic",
        client: :default,
        start_position: :latest
      )
      |> Stream.filter(&interesting?/1)
      |> Enum.to_list()

      # Read from specific message
      Pulsar.Reader.stream("persistent://public/default/topic",
        host: "pulsar://localhost:6650",
        start_message_id: {100, 200}
      )
      |> Stream.take(10)
      |> Enum.to_list()
  """
  @spec stream(String.t(), keyword()) :: Enumerable.t()
  def stream(topic, opts \\ []) do
    Stream.resource(
      fn -> start_reader(topic, opts) end,
      fn state -> next_message(state) end,
      fn state -> stop_reader(state) end
    )
  end

  @supported_options [
    :host,
    :name,
    :client,
    :start_position,
    :start_message_id,
    :start_timestamp,
    :flow_permits,
    :read_compacted,
    :timeout,
    :auth,
    :socket_opts
  ]

  defp start_reader(topic, opts) do
    validate_options!(opts)

    {connection_mode, client_name} = resolve_connection_mode(opts)

    case connection_mode do
      :internal ->
        host = Keyword.fetch!(opts, :host)
        auth = Keyword.get(opts, :auth)
        socket_opts = Keyword.get(opts, :socket_opts)

        client_opts =
          [name: client_name, host: host]
          |> maybe_put(:auth, auth)
          |> maybe_put(:socket_opts, socket_opts)

        {:ok, _pid} = Pulsar.Client.start_link(client_opts)

      :external ->
        :ok
    end

    flow_permits = Keyword.get(opts, :flow_permits, @default_flow_permits)
    start_position = Keyword.get(opts, :start_position, :earliest)
    start_message_id = Keyword.get(opts, :start_message_id)
    start_timestamp = Keyword.get(opts, :start_timestamp)
    read_compacted = Keyword.get(opts, :read_compacted, false)
    timeout = Keyword.get(opts, :timeout, 60_000)

    subscription_name = "reader-#{System.unique_integer([:positive, :monotonic])}"

    reader_ref = make_ref()

    consumer_opts = [
      client: client_name,
      subscription_type: :Exclusive,
      durable: false,
      initial_position: start_position,
      start_message_id: start_message_id,
      start_timestamp: start_timestamp,
      read_compacted: read_compacted,
      flow_initial: 0,
      startup_delay_ms: 0,
      startup_jitter_ms: 0,
      init_args: [self(), reader_ref]
    ]

    {:ok, consumer_group_pid} =
      Pulsar.start_consumer(
        topic,
        subscription_name,
        Pulsar.Reader.Callback,
        consumer_opts
      )

    consumer_pids = wait_for_consumers_ready(consumer_group_pid, reader_ref)

    Enum.each(consumer_pids, fn pid ->
      :ok = Consumer.send_flow(pid, flow_permits)
    end)

    permits_by_consumer =
      Map.new(consumer_pids, fn pid -> {pid, flow_permits} end)

    %{
      consumer_pids: consumer_pids,
      consumer_group_pid: consumer_group_pid,
      client_name: client_name,
      connection_mode: connection_mode,
      flow_permits: flow_permits,
      permits_by_consumer: permits_by_consumer,
      timeout: timeout,
      reader_ref: reader_ref,
      buffer: :queue.new()
    }
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

  defp stop_reader(state) do
    case Pulsar.stop_consumer(state.consumer_group_pid) do
      :ok -> :ok
      {:error, _reason} -> :ok
    end

    case state.connection_mode do
      :internal ->
        Pulsar.Client.stop(state.client_name)

      _ ->
        :ok
    end
  end

  defp resolve_connection_mode(opts) do
    host = Keyword.get(opts, :host)
    name = Keyword.get(opts, :name, :default)
    client = Keyword.get(opts, :client, :default)

    cond do
      host && client != :default ->
        raise ArgumentError, "cannot specify both :host and :client options"

      host ->
        {:internal, name}

      true ->
        {:external, client}
    end
  end

  defp validate_options!(opts) do
    unknown_opts = Keyword.keys(opts) -- @supported_options

    if unknown_opts != [] do
      raise ArgumentError,
            "unknown options #{inspect(unknown_opts)}, supported options are: #{inspect(@supported_options)}"
    end
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
    expected_count = count_consumers(consumer_group_pid)
    collect_ready_messages(expected_count, [], 5_000, reader_ref)
  end

  defp count_consumers(consumer_group_pid) do
    case Supervisor.which_children(consumer_group_pid) do
      # PartitionedConsumer - children are ConsumerGroup supervisors
      [{_id, _pid, :supervisor, [Pulsar.ConsumerGroup]} | _] = children ->
        # Each ConsumerGroup has 1 consumer (we use consumer_count: 1 implicitly)
        length(children)

      # ConsumerGroup - children are Consumer workers
      [{_id, _pid, :worker, [Consumer]} | _] = children ->
        length(children)

      # Empty or unknown structure, assume 1
      _ ->
        1
    end
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
