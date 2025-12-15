defmodule Pulsar.Producer do
  @moduledoc """
  Pulsar producer process that communicates with broker processes.

  This producer uses service discovery to find the appropriate broker
  for the topic and then communicates with that broker process.

  Initialization follows a multi-phase pattern using `{:continue, ...}` to
  avoid blocking the caller during broker discovery and registration.
  """

  use GenServer

  alias Pulsar.Config
  alias Pulsar.ProducerEpochStore
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.ServiceDiscovery

  require Logger

  defstruct [
    :client,
    :topic,
    :producer_id,
    :producer_name,
    :broker_pid,
    :broker_monitor,
    :sequence_id,
    :pending_sends,
    :access_mode,
    :compression,
    :ready,
    :registration_request_id,
    :topic_epoch,
    :chunking_enabled,
    :max_message_size,
    :batch_enabled,
    :batch,
    :batch_size,
    :batch_size_threshold,
    :batch_flush_timer,
    :flush_interval
  ]

  @type t :: %__MODULE__{
          topic: String.t(),
          producer_id: integer(),
          producer_name: String.t() | nil,
          broker_pid: pid(),
          broker_monitor: reference(),
          sequence_id: integer(),
          pending_sends: %{integer() => {GenServer.from(), map()}},
          access_mode: atom(),
          compression: :NONE | :LZ4 | :ZLIB | :SNAPPY | :ZSTD,
          ready: boolean() | nil,
          registration_request_id: integer() | nil,
          topic_epoch: integer() | nil,
          chunking_enabled: boolean(),
          max_message_size: non_neg_integer(),
          batch_enabled: boolean(),
          batch: list({map(), GenServer.from()}),
          batch_size: non_neg_integer(),
          batch_size_threshold: non_neg_integer(),
          batch_flush_timer: reference() | nil,
          flush_interval: non_neg_integer()
        }

  ## Public API

  @doc """
  Starts a producer process.

  ## Parameters

  - `topic` - The topic to publish to
  - `opts` - Additional options:
    - `:name` - Producer name (optional, will be auto-generated if not provided)
    - `:access_mode` - Producer access mode (default: `:Shared`). Available modes:
      - `:Shared` - Multiple producers can publish on the topic (default)
      - `:Exclusive` - Only one producer can publish. If another producer tries to connect,
        it will receive an error immediately. The old producer is evicted if it experiences
        a network partition with the broker.
      - `:WaitForExclusive` - If there is already a producer, wait until exclusive access is granted
      - `:ExclusiveWithFencing` - If there is already a producer, it will be removed (fenced out)
    - `:compression` - Compression algorithm (default: :NONE)
    - `:chunking_enabled` - Enable message chunking for large messages (default: false)
    - `:max_message_size` - Maximum size of each chunk in bytes when chunking is enabled (default: 5_242_880, which is 5MB)
    - `:startup_delay_ms` - Fixed startup delay in milliseconds before producer initialization (default: 1000, matches broker conn_timeout)
    - `:startup_jitter_ms` - Maximum random startup delay in milliseconds to avoid thundering herd (default: 1000)

  The total startup delay is `startup_delay_ms + random(0, startup_jitter_ms)`, applied on every producer start/restart.
  The default `startup_delay_ms` matches the broker's `conn_timeout` to ensure the broker has time to reconnect
  before producers start requesting topic lookups.

  The producer will automatically use service discovery to find the broker.
  If no name is provided, the broker will assign a unique producer name.

  ## Examples

      # Default shared mode
      {:ok, producer} = Producer.start_link("persistent://public/default/my-topic")

      # With custom name and exclusive mode
      {:ok, producer} = Producer.start_link(
        "persistent://public/default/my-topic",
        name: "my-producer",
        access_mode: :Exclusive
      )
  """
  def start_link(topic, opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, nil)
    {access_mode, genserver_opts} = Keyword.pop(opts, :access_mode, :Shared)
    {compression, genserver_opts} = Keyword.pop(genserver_opts, :compression, :NONE)
    {chunking_enabled, genserver_opts} = Keyword.pop(genserver_opts, :chunking_enabled, false)
    {max_message_size, genserver_opts} = Keyword.pop(genserver_opts, :max_message_size, 5_242_880)
    {batch_enabled, genserver_opts} = Keyword.pop(genserver_opts, :batch_enabled, false)
    {batch_size_threshold, genserver_opts} = Keyword.pop(genserver_opts, :batch_size, 100)
    {flush_interval, genserver_opts} = Keyword.pop(genserver_opts, :flush_interval, 10)

    {startup_delay_ms, genserver_opts} =
      Keyword.pop(genserver_opts, :startup_delay_ms, Config.startup_delay())

    {startup_jitter_ms, genserver_opts} =
      Keyword.pop(genserver_opts, :startup_jitter_ms, Config.startup_jitter())

    {client, _genserver_opts} = Keyword.pop(genserver_opts, :client, :default)

    producer_config = %{
      client: client,
      name: name,
      topic: topic,
      access_mode: access_mode,
      compression: compression,
      chunking_enabled: chunking_enabled,
      max_message_size: max_message_size,
      startup_delay_ms: startup_delay_ms,
      startup_jitter_ms: startup_jitter_ms,
      batch_enabled: batch_enabled,
      batch_size_threshold: batch_size_threshold,
      flush_interval: flush_interval
    }

    GenServer.start_link(__MODULE__, producer_config, [])
  end

  @doc """
  Gracefully stops a producer process.
  """
  @spec stop(GenServer.server(), term(), timeout()) :: :ok
  def stop(producer, reason \\ :normal, timeout \\ :infinity) do
    GenServer.stop(producer, reason, timeout)
  end

  @doc """
  Sends a message through this producer.
  Waits for acknowledgment from the broker.
  Returns `{:ok, message_id}` on success or `{:error, reason}` on failure.

  ## Parameters

  - `producer_pid` - The producer process PID
  - `message` - Binary message payload
  - `opts` - Optional parameters:
    - `:timeout` - Timeout in milliseconds (default: 5000)
    - `:partition_key` - Partition routing key (string)
    - `:ordering_key` - Key for ordering in Key_Shared subscriptions (binary)
    - `:properties` - Custom message metadata as a map (e.g., `%{"trace_id" => "abc"}`)
    - `:event_time` - Application event timestamp (DateTime or milliseconds since epoch)
    - `:deliver_at_time` - Absolute delayed delivery time (DateTime or milliseconds since epoch)
    - `:deliver_after` - Relative delayed delivery in milliseconds from now
  """
  @spec send_message(pid(), binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def send_message(producer_pid, message, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5000)
    GenServer.call(producer_pid, {:send_message, message, opts}, timeout)
  end

  ## GenServer Callbacks

  @impl true
  def init(producer_config) do
    # Trap exits so terminate/2 is called on shutdown
    Process.flag(:trap_exit, true)

    %{
      client: client,
      name: name,
      topic: topic,
      access_mode: access_mode,
      compression: compression,
      chunking_enabled: chunking_enabled,
      max_message_size: max_message_size,
      startup_delay_ms: startup_delay_ms,
      startup_jitter_ms: startup_jitter_ms,
      batch_enabled: batch_enabled,
      batch_size_threshold: batch_size_threshold,
      flush_interval: flush_interval
    } = producer_config

    producer_id = System.unique_integer([:positive, :monotonic])

    # Try to restore topic_epoch from ETS if this producer is restarting
    topic_epoch =
      case ProducerEpochStore.get(client, topic, name, access_mode) do
        {:ok, epoch} -> epoch
        :error -> nil
      end

    state = %__MODULE__{
      client: client,
      topic: topic,
      producer_id: producer_id,
      producer_name: name,
      sequence_id: 0,
      pending_sends: %{},
      access_mode: access_mode,
      compression: compression,
      ready: nil,
      registration_request_id: nil,
      topic_epoch: topic_epoch,
      chunking_enabled: chunking_enabled,
      max_message_size: max_message_size,
      batch_enabled: batch_enabled,
      batch: [],
      batch_size: 0,
      batch_size_threshold: batch_size_threshold,
      batch_flush_timer: nil,
      flush_interval: flush_interval
    }

    if is_nil(topic_epoch) do
      Logger.info("Starting producer #{producer_id} for topic #{topic}")
    else
      Logger.info("Starting producer #{producer_id} for topic #{topic} (restoring topic_epoch: #{topic_epoch})")
    end

    total_startup_delay = startup_delay_ms + startup_jitter_ms

    if total_startup_delay > 0 do
      {:ok, state, {:continue, {:startup_delay, startup_delay_ms, startup_jitter_ms}}}
    else
      {:ok, state, {:continue, :register_producer}}
    end
  end

  @impl true
  def handle_continue({:startup_delay, base_delay_ms, jitter_ms}, state) do
    jitter = if jitter_ms > 0, do: :rand.uniform(jitter_ms), else: 0
    total_sleep_ms = base_delay_ms + jitter

    Logger.debug("Producer sleeping for #{total_sleep_ms}ms (base: #{base_delay_ms}ms, jitter: #{jitter}ms)")

    Process.sleep(total_sleep_ms)
    {:noreply, state, {:continue, :register_producer}}
  end

  def handle_continue(:register_producer, state) do
    case ServiceDiscovery.lookup_topic(state.topic, client: state.client) do
      {:ok, broker_pid} ->
        case register_with_broker(state, broker_pid) do
          {:ok, new_state} ->
            {:noreply, new_state, {:continue, :monitor_broker}}

          {:error, {:ProducerFenced, _msg}} ->
            ProducerEpochStore.delete(state.client, state.topic, state.producer_name, state.access_mode)
            {:stop, {:shutdown, :producer_fenced}, state}

          {:error, reason} ->
            {:stop, reason, state}
        end

      {:error, reason} ->
        Logger.error("Topic lookup failed: #{inspect(reason)}")
        {:stop, reason, state}
    end
  end

  @impl true
  def handle_continue(:monitor_broker, state) do
    broker_monitor = Process.monitor(state.broker_pid)
    {:noreply, %{state | broker_monitor: broker_monitor}, {:continue, :start_batch_timer}}
  end

  def handle_continue(:start_batch_timer, state) do
    timer_ref =
      if state.batch_enabled do
        Process.send_after(self(), :flush_batch, state.flush_interval)
      end

    {:noreply, %{state | batch_flush_timer: timer_ref}}
  end

  @impl true
  def handle_call({:send_message, _, _}, _from, %{ready: false} = state) do
    Logger.warning("Producer #{state.producer_name} is waiting, cannot send message")
    {:reply, {:error, :producer_waiting}, state}
  end

  def handle_call({:send_message, payload, opts}, from, %{batch_enabled: true} = state) do
    message = %{
      payload: payload,
      partition_key: Keyword.get(opts, :partition_key),
      ordering_key: Keyword.get(opts, :ordering_key),
      properties: Keyword.get(opts, :properties),
      event_time: Keyword.get(opts, :event_time)
    }

    new_batch = [{message, from} | state.batch]
    new_size = state.batch_size + 1

    if new_size >= state.batch_size_threshold do
      state = cancel_batch_timer(state)
      state = do_flush_batch(%{state | batch: new_batch, batch_size: new_size})
      timer_ref = Process.send_after(self(), :flush_batch, state.flush_interval)
      {:noreply, %{state | batch_flush_timer: timer_ref}}
    else
      {:noreply, %{state | batch: new_batch, batch_size: new_size}}
    end
  end

  def handle_call({:send_message, payload, opts}, from, state) do
    messages = maybe_chunk(payload, opts, state)

    result =
      Enum.reduce_while(messages, {:ok, state}, fn {chunk_payload, chunk_metadata}, {:ok, acc_state} ->
        sequence_id = acc_state.sequence_id + 1
        command_send = %Binary.CommandSend{producer_id: acc_state.producer_id, sequence_id: sequence_id}

        message_metadata = %Binary.MessageMetadata{
          producer_name: acc_state.producer_name,
          sequence_id: sequence_id,
          publish_time: System.system_time(:millisecond),
          uncompressed_size: byte_size(chunk_payload),
          compression: acc_state.compression,
          partition_key: Keyword.get(opts, :partition_key),
          ordering_key: Keyword.get(opts, :ordering_key),
          properties: to_key_value_list(Keyword.get(opts, :properties)),
          event_time: to_timestamp(Keyword.get(opts, :event_time)),
          deliver_at_time: resolve_deliver_at_time(opts),
          uuid: Map.get(chunk_metadata, :uuid),
          chunk_id: Map.get(chunk_metadata, :chunk_id),
          num_chunks_from_msg: Map.get(chunk_metadata, :num_chunks),
          total_chunk_msg_size: Map.get(chunk_metadata, :total_chunk_msg_size)
        }

        compressed_payload = maybe_compress(message_metadata, chunk_payload)

        encoded_message = Pulsar.Protocol.encode_message(command_send, message_metadata, compressed_payload)

        case Pulsar.Broker.publish_message(acc_state.broker_pid, encoded_message) do
          :ok ->
            emit_message_sent(chunk_metadata, chunk_payload, sequence_id, acc_state)

            new_pending = Map.put(acc_state.pending_sends, sequence_id, {from, chunk_metadata})
            new_state = %{acc_state | sequence_id: sequence_id, pending_sends: new_pending}
            {:cont, {:ok, new_state}}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      end)

    case result do
      {:ok, new_state} ->
        {:noreply, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info(
        {:DOWN, monitor_ref, :process, broker_pid, reason},
        %__MODULE__{broker_monitor: monitor_ref, broker_pid: broker_pid} = state
      ) do
    Logger.info("Broker #{inspect(broker_pid)} crashed: #{inspect(reason)}, producer will restart")

    {:stop, :broker_crashed, state}
  end

  @impl true
  def handle_info({:EXIT, broker_pid, reason}, %__MODULE__{broker_pid: broker_pid} = state) do
    Logger.info("Broker #{inspect(broker_pid)} exited: #{inspect(reason)}, producer will restart")

    {:stop, :broker_exited, state}
  end

  @impl true
  def handle_info(:flush_batch, state) do
    state = do_flush_batch(state)
    timer_ref = Process.send_after(self(), :flush_batch, state.flush_interval)
    {:noreply, %{state | batch_flush_timer: timer_ref}}
  end

  @impl true
  def handle_info({:send_receipt, %Binary.CommandSendReceipt{} = receipt}, state) do
    new_state =
      case Map.pop(state.pending_sends, receipt.sequence_id) do
        # Batch case
        {{callers, %{batch: true}}, new_pending} when is_list(callers) ->
          Enum.each(callers, fn from ->
            GenServer.reply(from, {:ok, receipt.message_id})
          end)

          %{state | pending_sends: new_pending}

        # Chunked message case
        {{from, chunk_metadata}, new_pending} when is_map(chunk_metadata) and map_size(chunk_metadata) > 0 ->
          handle_chunk_receipt(receipt, from, chunk_metadata, new_pending, state)

        # Single message case
        {{from, _metadata}, new_pending} ->
          GenServer.reply(from, {:ok, receipt.message_id})
          %{state | pending_sends: new_pending}

        {nil, _} ->
          Logger.warning("Received receipt for unknown sequence_id #{receipt.sequence_id}")
          state
      end

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:send_error, %Binary.CommandSendError{} = error}, state) do
    case Map.pop(state.pending_sends, error.sequence_id) do
      {{callers, %{batch: true}}, new_pending} when is_list(callers) ->
        Enum.each(callers, fn from ->
          GenServer.reply(from, {:error, {error.error, error.message}})
        end)

        {:noreply, %{state | pending_sends: new_pending}}

      {{from, _metadata}, new_pending} ->
        GenServer.reply(from, {:error, {error.error, error.message}})
        {:noreply, %{state | pending_sends: new_pending}}

      {nil, _} ->
        Logger.warning("Received error for unknown sequence_id #{error.sequence_id}")
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:broker_message, %Binary.CommandProducerSuccess{} = command}, state) do
    if state.registration_request_id == command.request_id do
      if not is_nil(command.topic_epoch) do
        ProducerEpochStore.put(state.client, state.topic, state.producer_name, state.access_mode, command.topic_epoch)
      end

      {:noreply, %{state | ready: command.producer_ready, topic_epoch: command.topic_epoch}}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info({:broker_message, %Binary.CommandCloseProducer{}}, state) do
    {:stop, :broker_close_requested, state}
  end

  @impl true
  def terminate(_reason, nil) do
    :ok
  end

  def terminate(reason, state) do
    if state.batch_flush_timer do
      Process.cancel_timer(state.batch_flush_timer)
    end

    # Reply to any pending batch callers with error
    if state.batch_enabled and length(state.batch) > 0 do
      Enum.each(state.batch, fn {_message, from} ->
        GenServer.reply(from, {:error, :producer_terminated})
      end)
    end

    # CloseProducer is sent by the broker's DOWN handler
    Logger.debug("Producer #{inspect(state.producer_name)} terminating: #{inspect(reason)}")

    metadata = %{
      topic: state.topic,
      producer_name: state.producer_name,
      reason: reason
    }

    :telemetry.span(
      [:pulsar, :producer, :closed],
      metadata,
      fn ->
        {:ok, Map.put(metadata, :success, true)}
      end
    )

    :ok
  end

  ## Private Functions

  defp cancel_batch_timer(%{batch_flush_timer: nil} = state), do: state

  defp cancel_batch_timer(%{batch_flush_timer: timer_ref} = state) do
    Process.cancel_timer(timer_ref)
    %{state | batch_flush_timer: nil}
  end

  defp do_flush_batch(%{batch: []} = state), do: state

  defp do_flush_batch(state) do
    batch = Enum.reverse(state.batch)
    messages = Enum.map(batch, fn {message, _from} -> message end)
    callers = Enum.map(batch, fn {_message, from} -> from end)

    sequence_id = state.sequence_id + 1

    command_send = %Binary.CommandSend{
      producer_id: state.producer_id,
      sequence_id: sequence_id,
      num_messages: length(messages)
    }

    single_messages_payload =
      messages
      |> Enum.with_index()
      |> Enum.map(fn {msg, index} ->
        encode_single_message(msg, sequence_id + index)
      end)
      |> :erlang.iolist_to_binary()

    uncompressed_size = byte_size(single_messages_payload)

    message_metadata = %Binary.MessageMetadata{
      producer_name: state.producer_name,
      sequence_id: sequence_id,
      publish_time: System.system_time(:millisecond),
      compression: state.compression,
      uncompressed_size: uncompressed_size,
      num_messages_in_batch: length(messages)
    }

    compressed_payload = maybe_compress(message_metadata, single_messages_payload)

    encoded_frame = Pulsar.Protocol.encode_message(command_send, message_metadata, compressed_payload)

    case Pulsar.Broker.publish_message(state.broker_pid, encoded_frame) do
      :ok ->
        :telemetry.execute(
          [:pulsar, :producer, :batch, :published],
          %{count: length(messages)},
          %{
            topic: state.topic,
            producer_name: state.producer_name,
            producer_id: state.producer_id,
            sequence_id: sequence_id
          }
        )

        new_pending = Map.put(state.pending_sends, sequence_id, {callers, %{batch: true}})

        %{state | sequence_id: sequence_id, pending_sends: new_pending, batch: [], batch_size: 0}

      {:error, reason} ->
        Logger.error("Failed to send batch: #{inspect(reason)}")
        Enum.each(callers, fn from -> GenServer.reply(from, {:error, reason}) end)
        %{state | batch: [], batch_size: 0}
    end
  end

  defp handle_chunk_receipt(receipt, from, chunk_metadata, new_pending, state) do
    uuid = chunk_metadata.uuid
    num_chunks = chunk_metadata.num_chunks

    updated_chunk_meta = Map.put(chunk_metadata, :message_id, receipt.message_id)
    updated_pending = Map.put(new_pending, receipt.sequence_id, {from, updated_chunk_meta})

    chunks_with_receipts =
      Enum.filter(updated_pending, fn {_seq_id, {_from, meta}} ->
        is_map(meta) and Map.get(meta, :uuid) == uuid and Map.has_key?(meta, :message_id)
      end)

    if length(chunks_with_receipts) == num_chunks do
      complete_chunked_message(from, uuid, num_chunks, chunks_with_receipts, updated_pending, state)
    else
      %{state | pending_sends: updated_pending}
    end
  end

  defp complete_chunked_message(from, uuid, num_chunks, chunks_with_receipts, updated_pending, state) do
    sorted_chunks =
      chunks_with_receipts
      |> Enum.sort_by(fn {_seq_id, {_from, meta}} -> meta.chunk_id end)
      |> Enum.map(fn {_seq_id, {_from, meta}} -> meta.message_id end)

    :telemetry.execute(
      [:pulsar, :producer, :chunk, :complete],
      %{num_chunks: num_chunks},
      %{uuid: uuid, producer_id: state.producer_id}
    )

    chunked_msg_id = %{
      first_chunk_message_id: List.first(sorted_chunks),
      last_chunk_message_id: List.last(sorted_chunks),
      uuid: uuid,
      num_chunks: num_chunks
    }

    GenServer.reply(from, {:ok, chunked_msg_id})

    chunk_seq_ids = Enum.map(chunks_with_receipts, fn {seq_id, _} -> seq_id end)

    final_pending =
      updated_pending
      |> Enum.reject(fn {seq_id, _} -> seq_id in chunk_seq_ids end)
      |> Map.new()

    %{state | pending_sends: final_pending}
  end

  defp register_with_broker(state, broker_pid) do
    start_metadata = %{
      topic: state.topic,
      producer_name: state.producer_name
    }

    :telemetry.span(
      [:pulsar, :producer, :opened],
      start_metadata,
      fn ->
        result =
          with :ok <- Pulsar.Broker.register_producer(broker_pid, state.producer_id, self()),
               {:ok, response} <- create_producer(broker_pid, state) do
            if not is_nil(response.topic_epoch) do
              ProducerEpochStore.put(
                state.client,
                state.topic,
                response.producer_name,
                state.access_mode,
                response.topic_epoch
              )
            end

            state =
              state
              |> Map.put(:broker_pid, broker_pid)
              |> Map.put(:producer_name, response.producer_name)
              |> Map.put(:registration_request_id, response.request_id)
              |> Map.put(:ready, Map.get(response, :producer_ready, true))
              |> Map.put(:topic_epoch, response.topic_epoch)

            {:ok, state}
          else
            {:error, reason} = error ->
              Logger.error("Producer registration failed: #{inspect(reason)}")
              error
          end

        stop_metadata_extra =
          case result do
            {:ok, state} ->
              %{success: true, producer_name: state.producer_name}

            {:error, {:ProducerFenced, _msg}} ->
              %{
                success: false,
                error: :producer_fenced,
                producer_id: state.producer_id,
                access_mode: state.access_mode,
                topic: state.topic
              }

            {:error, reason} ->
              %{
                success: false,
                error: reason,
                producer_id: state.producer_id,
                access_mode: state.access_mode,
                topic: state.topic
              }
          end

        stop_metadata = Map.merge(start_metadata, stop_metadata_extra)
        {result, stop_metadata}
      end
    )
  end

  defp create_producer(broker_pid, state) do
    producer_name = if state.producer_name, do: to_string(state.producer_name)

    producer_command = %Binary.CommandProducer{
      topic: state.topic,
      producer_id: state.producer_id,
      producer_name: producer_name,
      producer_access_mode: state.access_mode,
      topic_epoch: state.topic_epoch
    }

    Pulsar.Broker.send_request(broker_pid, producer_command)
  end

  defp maybe_compress(%Binary.MessageMetadata{compression: :NONE}, payload) do
    payload
  end

  defp maybe_compress(%Binary.MessageMetadata{compression: :ZLIB}, compressed_payload) do
    :zlib.compress(compressed_payload)
  end

  defp maybe_compress(%Binary.MessageMetadata{compression: :LZ4}, compressed_payload) do
    NimbleLZ4.compress(compressed_payload)
  end

  defp maybe_compress(%Binary.MessageMetadata{compression: :ZSTD}, compressed_payload) do
    :ezstd.compress(compressed_payload)
  end

  defp maybe_compress(%Binary.MessageMetadata{compression: :SNAPPY}, compressed_payload) do
    {:ok, payload} = :snappyer.compress(compressed_payload)
    payload
  end

  defp encode_single_message(msg, sequence_id) do
    payload = msg.payload

    single_metadata = %Binary.SingleMessageMetadata{
      payload_size: byte_size(payload),
      partition_key: Map.get(msg, :partition_key),
      ordering_key: Map.get(msg, :ordering_key),
      properties: to_key_value_list(Map.get(msg, :properties)),
      event_time: to_timestamp(Map.get(msg, :event_time)),
      sequence_id: sequence_id
    }

    encoded_metadata = Binary.SingleMessageMetadata.encode(single_metadata)
    metadata_size = byte_size(encoded_metadata)

    <<metadata_size::32, encoded_metadata::binary, payload::binary>>
  end

  defp to_key_value_list(nil), do: []

  defp to_key_value_list(props) when is_map(props) do
    Enum.map(props, fn {k, v} ->
      %Binary.KeyValue{key: to_string(k), value: to_string(v)}
    end)
  end

  defp to_timestamp(%DateTime{} = dt), do: DateTime.to_unix(dt, :millisecond)
  defp to_timestamp(ms), do: ms

  defp resolve_deliver_at_time(opts) do
    case {Keyword.get(opts, :deliver_at_time), Keyword.get(opts, :deliver_after)} do
      {nil, nil} -> nil
      {dt, _} when not is_nil(dt) -> to_timestamp(dt)
      {nil, ms} when is_integer(ms) -> System.system_time(:millisecond) + ms
    end
  end

  defp emit_message_sent(%{uuid: uuid, chunk_id: chunk_id} = _chunk_metadata, chunk_payload, sequence_id, state) do
    :telemetry.execute(
      [:pulsar, :producer, :chunk, :sent],
      %{chunk_id: chunk_id, chunk_size: byte_size(chunk_payload)},
      %{uuid: uuid, producer_id: state.producer_id, sequence_id: sequence_id}
    )
  end

  defp emit_message_sent(_chunk_metadata, _chunk_payload, sequence_id, state) do
    :telemetry.execute(
      [:pulsar, :producer, :message, :published],
      %{count: 1},
      %{
        topic: state.topic,
        producer_name: state.producer_name,
        producer_id: state.producer_id,
        sequence_id: sequence_id
      }
    )
  end

  defp maybe_chunk(payload, _opts, state) do
    payload_size = byte_size(payload)

    if state.chunking_enabled and payload_size > state.max_message_size do
      uuid = Uniq.UUID.uuid4()
      chunk_size = state.max_message_size
      num_chunks = div(payload_size + chunk_size - 1, chunk_size)

      Logger.debug("Chunking message: #{payload_size} bytes into #{num_chunks} chunks (max: #{chunk_size} bytes each)")

      :telemetry.execute(
        [:pulsar, :producer, :chunk, :start],
        %{total_size: payload_size, num_chunks: num_chunks, chunk_size: chunk_size},
        %{uuid: uuid, producer_id: state.producer_id, topic: state.topic}
      )

      Enum.map(0..(num_chunks - 1), fn chunk_id ->
        offset = chunk_id * chunk_size
        remaining = payload_size - offset
        current_chunk_size = min(remaining, chunk_size)
        <<_skip::binary-size(offset), chunk_data::binary-size(current_chunk_size), _rest::binary>> = payload

        chunk_metadata = %{
          uuid: uuid,
          chunk_id: chunk_id,
          num_chunks: num_chunks,
          total_chunk_msg_size: payload_size
        }

        {chunk_data, chunk_metadata}
      end)
    else
      [{payload, %{}}]
    end
  end
end
