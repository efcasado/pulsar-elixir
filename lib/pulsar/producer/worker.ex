defmodule Pulsar.Producer.Worker do
  @moduledoc false

  # The GenServer behind a single producer. Pulsar.Producer starts these through
  # Pulsar.Topology.Group, one per partition and per :producer_count.

  use GenServer

  alias Pulsar.Backoff
  alias Pulsar.Producer.EpochStore
  alias Pulsar.Protocol
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.Schema
  alias Pulsar.Topology.Resolver

  require Logger

  @terminal_errors [
    :AuthenticationError,
    :AuthorizationError,
    :IncompatibleSchema,
    :InvalidTopicName,
    :NotAllowedError,
    :TopicTerminatedError,
    :UnsupportedVersionError
  ]

  # Checksum and size prefixes around the metadata, the CommandSend framing, and slack for the
  # chunk counters once their real values are known.
  @chunk_framing_overhead 64

  # Stands in while the budget is measured, so a message that turns out not to need chunking
  # never generates a uuid. Only its encoded length matters here.
  @uuid_placeholder "00000000-0000-0000-0000-000000000000"

  # The broker's -1, as it arrives over an unsigned 64-bit field.
  @unpersisted 18_446_744_073_709_551_615

  defstruct [
    :client,
    :topic,
    :base_topic,
    :partition,
    :producer_id,
    :producer_name,
    :broker_pid,
    :broker_monitor,
    {:sequence_id, 0},
    {:pending_frames, %{}},
    {:pending_messages, 0},
    :send_timeout,
    :send_timeout_timer,
    :max_pending_messages,
    :access_mode,
    :compression,
    {:ready, false},
    :registration_request_id,
    :topic_epoch,
    :chunking_enabled,
    :max_message_size,
    :broker_max_message_size,
    :batch_enabled,
    {:batch, []},
    {:batched, 0},
    :batch_started_at,
    :batch_size,
    :batch_builder,
    :batch_flush_timer,
    :flush_interval,
    :schema,
    :schema_version
  ]

  @type t :: %__MODULE__{
          topic: String.t(),
          base_topic: String.t(),
          partition: non_neg_integer() | nil,
          producer_id: integer(),
          producer_name: String.t() | nil,
          broker_pid: pid(),
          broker_monitor: reference(),
          sequence_id: integer(),
          pending_frames: %{integer() => {[GenServer.from()], map() | nil, integer()}},
          pending_messages: non_neg_integer(),
          send_timeout: pos_integer() | false | nil,
          send_timeout_timer: reference() | nil,
          max_pending_messages: pos_integer() | false | nil,
          access_mode: atom(),
          compression: :none | :lz4 | :zlib | :snappy | :zstd,
          ready: boolean(),
          registration_request_id: integer() | nil,
          topic_epoch: integer() | nil,
          chunking_enabled: boolean(),
          max_message_size: non_neg_integer(),
          broker_max_message_size: pos_integer() | nil,
          batch_enabled: boolean(),
          batch: list({map(), GenServer.from()}),
          batched: non_neg_integer(),
          batch_started_at: integer() | nil,
          batch_size: non_neg_integer(),
          batch_builder: :default | :key_based,
          batch_flush_timer: reference() | nil,
          flush_interval: non_neg_integer(),
          schema: Schema.t() | nil,
          schema_version: binary() | nil
        }

  ## Public API

  @doc """
  Starts one producer process.

  Takes `Pulsar.Producer`'s options, already validated against `Pulsar.Producer.Options`
  and given the `:name` of this worker within its group.
  """
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc """
  Gracefully stops a producer process.
  """
  @spec stop(GenServer.server(), term(), timeout()) :: :ok
  def stop(producer, reason \\ :normal, timeout \\ :infinity) do
    GenServer.stop(producer, reason, timeout)
  end

  @doc false
  @spec ready?(pid(), timeout()) :: boolean()
  def ready?(producer, timeout), do: GenServer.call(producer, :ready?, timeout)

  ## GenServer Callbacks

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    client = Keyword.fetch!(opts, :client)
    topic = Keyword.fetch!(opts, :topic)
    name = Keyword.get(opts, :name)
    producer_id = System.unique_integer([:positive, :monotonic])

    # Restored from ETS when this producer is restarting.
    topic_epoch =
      case EpochStore.get(client, topic, name, Keyword.fetch!(opts, :access_mode)) do
        {:ok, epoch} -> epoch
        :error -> nil
      end

    # Option names and struct field names are the same, so struct/2 carries them across
    # and ignores the group-level options that are not part of a producer's state.
    state = %{
      struct(__MODULE__, opts)
      | producer_id: producer_id,
        producer_name: name,
        topic_epoch: topic_epoch,
        schema: build_schema(Keyword.get(opts, :schema))
    }

    if is_nil(topic_epoch) do
      Logger.debug("Starting producer #{producer_id} for topic #{topic}")
    else
      Logger.debug("Starting producer #{producer_id} for topic #{topic} (restoring topic_epoch: #{topic_epoch})")
    end

    startup_delay_ms = Keyword.fetch!(opts, :startup_delay_ms)
    startup_jitter_ms = Keyword.fetch!(opts, :startup_jitter_ms)

    if startup_delay_ms + startup_jitter_ms > 0 do
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
    case Backoff.run(fn -> register(state) end) do
      {:ok, new_state} ->
        {:noreply, new_state, {:continue, :monitor_broker}}

      {:error, {:ProducerFenced, _msg}} ->
        EpochStore.delete(state.client, state.topic, state.producer_name, state.access_mode)
        {:stop, {:shutdown, :producer_fenced}, state}

      # Errors a second attempt cannot change; see Pulsar.Consumer.Worker.
      {:error, {code, _msg} = reason} when code in @terminal_errors ->
        {:stop, {:shutdown, reason}, state}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

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

  defp register(state) do
    case Resolver.lookup_topic(state.topic, client: state.client) do
      {:ok, broker_pid} ->
        register_with_broker(state, broker_pid)

      {:error, reason} = error ->
        Logger.error("Topic lookup failed: #{inspect(reason)}")
        error
    end
  end

  @impl true
  def handle_call(:ready?, _from, state), do: {:reply, state.ready, state}

  @impl true
  def handle_cast({:send_message, payload, opts, from}, state) do
    {:noreply, do_send(payload, opts, from, state)}
  end

  defp do_send(_payload, _opts, from, %__MODULE__{ready: false} = state) do
    Logger.warning("Producer #{state.producer_name} is waiting, cannot send message")

    refuse(state, from, :producer_waiting)
  end

  defp do_send(payload, opts, from, state) do
    if queue_full?(state) do
      refuse(state, from, :producer_queue_full)
    else
      publish_or_batch(payload, opts, from, state)
    end
  end

  # Answered without `answer/3`: a refused send was never parked, so there is nothing to count off.
  defp refuse(state, from, reason) do
    GenServer.reply(from, {:error, reason})

    state
  end

  defp queue_full?(%__MODULE__{max_pending_messages: limit}) when limit in [false, nil], do: false

  defp queue_full?(%__MODULE__{} = state), do: state.pending_messages >= state.max_pending_messages

  # Parked, not accepted: a send answered synchronously never waits. A chunked message parks once.
  defp park_send(%__MODULE__{} = state), do: %{state | pending_messages: state.pending_messages + 1}

  # The only decrement, so the count cannot drift. `terminate/2` is the exception: no state left.
  defp answer(%__MODULE__{} = state, callers, reply) do
    Enum.each(callers, fn from -> GenServer.reply(from, reply) end)

    %{state | pending_messages: state.pending_messages - length(callers)}
  end

  # Answers everyone a pending send owes and stops tracking it. A chunk takes the rest of its
  # message with it: nothing can complete it now.
  defp resolve_send(%__MODULE__{} = state, sequence_id, reply) do
    case Map.pop(state.pending_frames, sequence_id) do
      {nil, _pending} ->
        :error

      {{callers, metadata, _sent_at}, pending} ->
        pending = drop_chunks_of(pending, metadata)
        {:ok, callers, answer(%{state | pending_frames: pending}, callers, reply)}
    end
  end

  defp drop_chunks_of(pending, %{uuid: uuid}), do: Map.reject(pending, &chunk_of?(&1, uuid))
  defp drop_chunks_of(pending, _metadata), do: pending

  defp publish_or_batch(payload, opts, from, %{batch_enabled: true} = state) do
    if delayed?(opts) do
      # A delay names the entry and SingleMessageMetadata has no field for one, so this cannot
      # ride in a batch. What is pending goes first, or it would overtake earlier messages.
      send_unbatched(payload, opts, from, do_flush_batch(state))
    else
      add_to_batch(payload, opts, from, state)
    end
  end

  defp publish_or_batch(payload, opts, from, state), do: send_unbatched(payload, opts, from, state)

  defp add_to_batch(payload, opts, from, state) do
    message = %{
      payload: payload,
      partition_key: Keyword.get(opts, :partition_key),
      ordering_key: Keyword.get(opts, :ordering_key),
      properties: Keyword.get(opts, :properties),
      event_time: Keyword.get(opts, :event_time)
    }

    state = park_send(state)
    new_batch = [{message, from} | state.batch]
    new_batched = state.batched + 1
    started_at = state.batch_started_at || System.monotonic_time(:millisecond)

    state = %{state | batch: new_batch, batched: new_batched, batch_started_at: started_at}

    if new_batched >= state.batch_size do
      do_flush_batch(state)
    else
      maybe_schedule_send_timeout(state)
    end
  end

  defp send_unbatched(payload, opts, from, state) do
    # Compression covers the whole message and the split comes after it, so a chunk is a slice
    # of compressed bytes rather than compressed on its own.
    base_metadata = build_message_metadata(payload, opts, state)
    compressed_payload = maybe_compress(base_metadata, payload)

    case maybe_chunk(compressed_payload, base_metadata, state) do
      {:ok, messages} -> publish_messages(messages, base_metadata, from, state)
      {:error, reason} -> refuse(state, from, reason)
    end
  end

  defp publish_messages(messages, base_metadata, from, state) do
    # One send time for the whole message: its chunks share a deadline, as they share a caller.
    sent_at = System.monotonic_time(:millisecond)

    result =
      Enum.reduce_while(messages, {:ok, state}, fn {chunk_payload, chunk_metadata}, {:ok, acc_state} ->
        sequence_id = acc_state.sequence_id + 1

        command_send = %Binary.CommandSend{
          producer_id: acc_state.producer_id,
          sequence_id: sequence_id,
          is_chunk: not is_nil(chunk_metadata)
        }

        message_metadata = apply_chunk_metadata(base_metadata, sequence_id, chunk_metadata)
        encoded_message = Protocol.encode(command_send, message_metadata, chunk_payload)

        case Pulsar.Broker.publish_message(acc_state.broker_pid, encoded_message) do
          :ok ->
            emit_message_sent(chunk_metadata, chunk_payload, sequence_id, acc_state)

            new_pending = Map.put(acc_state.pending_frames, sequence_id, {[from], chunk_metadata, sent_at})
            new_state = %{acc_state | sequence_id: sequence_id, pending_frames: new_pending}
            {:cont, {:ok, new_state}}

          {:error, reason} ->
            {:halt, {:error, reason, acc_state}}
        end
      end)

    case result do
      {:ok, new_state} ->
        new_state |> park_send() |> maybe_schedule_send_timeout()

      # Rolling the counter back would reissue sequence ids still in flight, which the
      # broker's deduplication reads as repeats. The pending entries do go: a message missing
      # chunks can never complete, and its caller already has an error.
      {:error, reason, acc_state} ->
        refuse(%{acc_state | pending_frames: state.pending_frames}, from, reason)
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
  def handle_info(:expire_sends, state) do
    state = %{state | send_timeout_timer: nil}

    {:noreply, state |> expire_due_sends() |> maybe_schedule_send_timeout()}
  end

  @impl true
  def handle_info(:flush_batch, state) do
    state = do_flush_batch(state)
    timer_ref = Process.send_after(self(), :flush_batch, state.flush_interval)
    {:noreply, %{state | batch_flush_timer: timer_ref}}
  end

  # A topic with deduplication enabled acknowledges a send it discarded, using this in place of
  # the message id it never assigned. The send did reach the broker, so the caller is told so.
  @impl true
  def handle_info(
        {:send_receipt,
         %Binary.CommandSendReceipt{message_id: %{ledgerId: @unpersisted, entryId: @unpersisted}} = receipt},
        state
      ) do
    {:noreply, report_deduplicated(receipt, state)}
  end

  @impl true
  def handle_info({:send_receipt, %Binary.CommandSendReceipt{} = receipt}, state) do
    new_state =
      case Map.pop(state.pending_frames, receipt.sequence_id) do
        # A chunk resolves nothing on its own: the message is owed the rest of its receipts.
        {{callers, %{uuid: _} = chunk_metadata, sent_at}, new_pending} ->
          handle_chunk_receipt(receipt, callers, chunk_metadata, sent_at, new_pending, state)

        {{callers, _metadata, _sent_at}, new_pending} ->
          answer(%{state | pending_frames: new_pending}, callers, {:ok, receipt.message_id})

        # A chunk of a message that was given up on, which a partial send and a discarded chunk
        # both leave behind: the entry is gone, but what reached the broker is still answered.
        {nil, _} ->
          Logger.debug("Received receipt for untracked sequence_id #{receipt.sequence_id}")
          state
      end

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:send_error, %Binary.CommandSendError{} = error}, state) do
    reply = {:error, {error.error, error.message}}

    case resolve_send(state, error.sequence_id, reply) do
      {:ok, _callers, new_state} ->
        {:noreply, new_state}

      :error ->
        Logger.warning("Received error for unknown sequence_id #{error.sequence_id}")
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:broker_message, %Binary.CommandProducerSuccess{} = command}, state) do
    if state.registration_request_id == command.request_id do
      if not is_nil(command.topic_epoch) do
        EpochStore.put(state.client, state.topic, state.producer_name, state.access_mode, command.topic_epoch)
      end

      new_state =
        state
        |> Map.put(:ready, command.producer_ready)
        |> Map.put(:topic_epoch, command.topic_epoch)
        |> Map.put(:schema_version, command.schema_version)

      {:noreply, new_state}
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
    if state.batch_enabled and not Enum.empty?(state.batch) do
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

  defp do_flush_batch(%{batch: []} = state), do: state

  defp do_flush_batch(state) do
    new_state =
      state.batch
      |> Enum.reverse()
      |> batch_entries(state.batch_builder)
      |> Enum.reduce(state, &publish_batch/2)

    %{new_state | batch: [], batched: 0, batch_started_at: nil}
  end

  # Grouping on the ordering key ahead of the partition key is the order the broker resolves a
  # sticky key in, so messages bound for one consumer stay in one entry.
  defp batch_entries(batch, :key_based) do
    grouped = Enum.group_by(batch, &batch_key/1)

    batch
    |> Enum.map(&batch_key/1)
    |> Enum.uniq()
    |> Enum.map(&Map.fetch!(grouped, &1))
  end

  defp batch_entries(batch, :default), do: [batch]

  defp batch_key({message, _from}), do: message.ordering_key || message.partition_key

  defp publish_batch(batch, state) do
    messages = Enum.map(batch, fn {message, _from} -> message end)
    callers = Enum.map(batch, fn {_message, from} -> from end)
    messages_count = length(messages)
    [first_message | _] = messages

    # A batch spends one sequence id per message it carries, so the next one starts past the
    # whole range. Advertising only the first repeats the ids consumers see and leaves the
    # broker's high-water mark short of what was handed out.
    sequence_id = state.sequence_id + 1
    highest_sequence_id = sequence_id + messages_count - 1

    command_send = %Binary.CommandSend{
      producer_id: state.producer_id,
      sequence_id: sequence_id,
      highest_sequence_id: highest_sequence_id,
      num_messages: messages_count
    }

    single_messages_payload =
      messages
      |> Enum.with_index()
      |> Enum.map(fn {msg, index} ->
        encode_single_message(msg, sequence_id + index)
      end)
      |> :erlang.iolist_to_binary()

    uncompressed_size = byte_size(single_messages_payload)

    # Key_Shared reads the sticky key off the entry rather than the messages inside it, and
    # invents one per entry when it finds none, scattering a key across consumers.
    message_metadata = %Binary.MessageMetadata{
      producer_name: state.producer_name,
      sequence_id: sequence_id,
      highest_sequence_id: highest_sequence_id,
      publish_time: System.system_time(:millisecond),
      compression: Protocol.to_compression(state.compression),
      uncompressed_size: uncompressed_size,
      num_messages_in_batch: messages_count,
      partition_key: first_message.partition_key,
      ordering_key: first_message.ordering_key,
      schema_version: state.schema_version
    }

    compressed_payload = maybe_compress(message_metadata, single_messages_payload)

    encoded_frame = Protocol.encode(command_send, message_metadata, compressed_payload)

    case Pulsar.Broker.publish_message(state.broker_pid, encoded_frame) do
      :ok ->
        :telemetry.execute(
          [:pulsar, :producer, :batch, :published],
          %{count: messages_count},
          Map.put(producer_metadata(state), :sequence_id, sequence_id)
        )

        # Keyed on the first id, which is the one the receipt comes back on. It keeps the clock
        # it started in the batch, or flushing would hand every message it carries a fresh one.
        new_pending = Map.put(state.pending_frames, sequence_id, {callers, nil, state.batch_started_at})

        maybe_schedule_send_timeout(%{state | sequence_id: highest_sequence_id, pending_frames: new_pending})

      # Only this entry's callers hear about it; the ones published before it are still owed
      # their receipts.
      {:error, reason} ->
        Logger.error("Failed to send batch: #{inspect(reason)}")
        answer(state, callers, {:error, reason})
    end
  end

  # One timer, aimed at the oldest send: every send shares the timeout, so that one expires first.
  defp maybe_schedule_send_timeout(%__MODULE__{send_timeout: timeout} = state) when timeout in [false, nil], do: state

  defp maybe_schedule_send_timeout(%__MODULE__{send_timeout_timer: timer} = state) when is_reference(timer), do: state

  defp maybe_schedule_send_timeout(%__MODULE__{} = state) do
    case oldest_send(state) do
      nil ->
        state

      sent_at ->
        due_in = max(sent_at + state.send_timeout - System.monotonic_time(:millisecond), 0)
        %{state | send_timeout_timer: Process.send_after(self(), :expire_sends, due_in)}
    end
  end

  # A message waiting to be batched is owed an answer too, counted from when it was taken.
  defp oldest_send(%__MODULE__{} = state) do
    sent_ats = for {_sequence_id, {_callers, _metadata, sent_at}} <- state.pending_frames, do: sent_at

    Enum.min(sent_ats ++ List.wrap(state.batch_started_at), fn -> nil end)
  end

  defp expire_due_sends(%__MODULE__{} = state) do
    cutoff = System.monotonic_time(:millisecond) - state.send_timeout

    due =
      for {sequence_id, {_callers, _metadata, sent_at}} <- state.pending_frames,
          sent_at <= cutoff,
          do: sequence_id

    due
    |> Enum.sort()
    |> Enum.reduce(state, &expire_send/2)
    |> expire_due_batch(cutoff)
  end

  defp expire_due_batch(%__MODULE__{batch_started_at: nil} = state, _cutoff), do: state

  defp expire_due_batch(%__MODULE__{batch_started_at: started_at} = state, cutoff) when started_at > cutoff, do: state

  defp expire_due_batch(%__MODULE__{} = state, _cutoff) do
    callers = Enum.map(state.batch, fn {_message, from} -> from end)
    report_batch_timeout(length(callers), state)

    answer(%{state | batch: [], batched: 0, batch_started_at: nil}, callers, {:error, :send_timeout})
  end

  defp expire_send(sequence_id, state) do
    case resolve_send(state, sequence_id, {:error, :send_timeout}) do
      {:ok, callers, new_state} ->
        report_send_timeout(length(callers), sequence_id, state)
        new_state

      # Already answered, by the chunk cascade that dropped this one.
      :error ->
        state
    end
  end

  defp report_send_timeout(count, sequence_id, state) do
    Logger.warning(
      "Broker did not acknowledge sequence_id #{sequence_id} on #{state.topic} within " <>
        "#{state.send_timeout}ms, failing #{count} caller(s)"
    )

    :telemetry.execute(
      [:pulsar, :producer, :send, :timeout],
      %{count: count},
      Map.put(producer_metadata(state), :sequence_id, sequence_id)
    )
  end

  defp report_batch_timeout(count, state) do
    Logger.warning(
      "#{count} message(s) on #{state.topic} were still waiting to be batched after " <>
        "#{state.send_timeout}ms, failing their callers"
    )

    :telemetry.execute(
      [:pulsar, :producer, :batch, :timeout],
      %{count: count},
      producer_metadata(state)
    )
  end

  defp report_deduplicated(receipt, state) do
    case resolve_send(state, receipt.sequence_id, {:ok, :deduplicated}) do
      {:ok, callers, new_state} ->
        report_discarded(receipt, length(callers), state)
        new_state

      # Answered and reported already, by the discarded chunk that dropped this one.
      :error ->
        Logger.debug("Received deduplicated receipt for untracked sequence_id #{receipt.sequence_id}")
        state
    end
  end

  defp report_discarded(receipt, count, state) do
    Logger.warning(
      "Broker discarded sequence_id #{receipt.sequence_id} on #{state.topic} as already stored " <>
        "under producer name #{state.producer_name}"
    )

    :telemetry.execute(
      [:pulsar, :producer, :message, :deduplicated],
      %{count: count},
      Map.put(producer_metadata(state), :sequence_id, receipt.sequence_id)
    )
  end

  defp chunk_of?({_sequence_id, {_callers, %{uuid: uuid}, _sent_at}}, uuid), do: true
  defp chunk_of?(_pending_send, _uuid), do: false

  defp handle_chunk_receipt(receipt, callers, chunk_metadata, sent_at, new_pending, state) do
    uuid = chunk_metadata.uuid
    num_chunks = chunk_metadata.num_chunks

    updated_chunk_meta = Map.put(chunk_metadata, :message_id, receipt.message_id)
    updated_pending = Map.put(new_pending, receipt.sequence_id, {callers, updated_chunk_meta, sent_at})

    chunks_with_receipts =
      Enum.filter(updated_pending, fn {_seq_id, {_callers, meta, _sent_at}} ->
        match?(%{uuid: ^uuid, message_id: _}, meta)
      end)

    if length(chunks_with_receipts) == num_chunks do
      complete_chunked_message(callers, uuid, num_chunks, chunks_with_receipts, updated_pending, state)
    else
      %{state | pending_frames: updated_pending}
    end
  end

  defp complete_chunked_message(callers, uuid, num_chunks, chunks_with_receipts, updated_pending, state) do
    sorted_chunks =
      chunks_with_receipts
      |> Enum.sort_by(fn {_seq_id, {_callers, meta, _sent_at}} -> meta.chunk_id end)
      |> Enum.map(fn {_seq_id, {_callers, meta, _sent_at}} -> meta.message_id end)

    :telemetry.execute(
      [:pulsar, :producer, :chunk, :complete],
      %{num_chunks: num_chunks},
      Map.put(producer_metadata(state), :uuid, uuid)
    )

    chunked_msg_id = %{
      first_chunk_message_id: List.first(sorted_chunks),
      last_chunk_message_id: List.last(sorted_chunks),
      uuid: uuid,
      num_chunks: num_chunks
    }

    state = answer(state, callers, {:ok, chunked_msg_id})

    chunk_seq_ids = Enum.map(chunks_with_receipts, fn {seq_id, _} -> seq_id end)

    final_pending =
      updated_pending
      |> Enum.reject(fn {seq_id, _} -> seq_id in chunk_seq_ids end)
      |> Map.new()

    %{state | pending_frames: final_pending}
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
              EpochStore.put(
                state.client,
                state.topic,
                state.producer_name,
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
              |> Map.put(:schema_version, response.schema_version)
              |> Map.put(:broker_max_message_size, broker_max_message_size(broker_pid))
              |> Map.put(:sequence_id, max(state.sequence_id, response.last_sequence_id))

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

  # Shared by every event this producer emits, so they all group the same way.
  defp producer_metadata(state) do
    %{
      topic: state.topic,
      base_topic: state.base_topic,
      partition: state.partition,
      producer_id: state.producer_id,
      producer_name: state.producer_name
    }
  end

  defp broker_max_message_size(broker_pid) do
    case Pulsar.Broker.get_max_message_size(broker_pid) do
      size when is_integer(size) and size > 0 -> size
      _ -> nil
    end
  end

  defp create_producer(broker_pid, state) do
    producer_name = if state.producer_name, do: to_string(state.producer_name)

    producer_command = %Binary.CommandProducer{
      topic: state.topic,
      producer_id: state.producer_id,
      producer_name: producer_name,
      producer_access_mode: Protocol.to_producer_access_mode(state.access_mode),
      topic_epoch: state.topic_epoch,
      schema: Schema.to_binary(state.schema)
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
      properties: Protocol.to_key_value_list(Map.get(msg, :properties)),
      event_time: to_timestamp(Map.get(msg, :event_time)),
      sequence_id: sequence_id
    }

    encoded_metadata = Binary.SingleMessageMetadata.encode(single_metadata)
    metadata_size = byte_size(encoded_metadata)

    <<metadata_size::32, encoded_metadata::binary, payload::binary>>
  end

  defp to_timestamp(%DateTime{} = dt), do: DateTime.to_unix(dt, :millisecond)
  defp to_timestamp(ms), do: ms

  defp delayed?(opts), do: not is_nil(resolve_deliver_at_time(opts))

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
      Map.merge(producer_metadata(state), %{uuid: uuid, sequence_id: sequence_id})
    )
  end

  defp emit_message_sent(_chunk_metadata, _chunk_payload, sequence_id, state) do
    :telemetry.execute(
      [:pulsar, :producer, :message, :published],
      %{count: 1},
      Map.put(producer_metadata(state), :sequence_id, sequence_id)
    )
  end

  defp apply_chunk_metadata(base_metadata, sequence_id, nil) do
    %{base_metadata | sequence_id: sequence_id}
  end

  defp apply_chunk_metadata(base_metadata, sequence_id, chunk_metadata) do
    %{uuid: uuid, chunk_id: chunk_id, num_chunks: num_chunks, total_chunk_msg_size: total} = chunk_metadata

    %{
      base_metadata
      | sequence_id: sequence_id,
        uuid: uuid,
        chunk_id: chunk_id,
        num_chunks_from_msg: num_chunks,
        total_chunk_msg_size: total
    }
  end

  defp build_message_metadata(payload, opts, state) do
    %Binary.MessageMetadata{
      producer_name: state.producer_name,
      # Replaced per chunk, but not left nil: the budget below encodes this to measure it,
      # and proto2 rejects a nil on a required field.
      sequence_id: 0,
      publish_time: System.system_time(:millisecond),
      uncompressed_size: byte_size(payload),
      compression: Protocol.to_compression(state.compression),
      partition_key: Keyword.get(opts, :partition_key),
      ordering_key: Keyword.get(opts, :ordering_key),
      properties: Protocol.to_key_value_list(Keyword.get(opts, :properties)),
      event_time: to_timestamp(Keyword.get(opts, :event_time)),
      deliver_at_time: resolve_deliver_at_time(opts),
      schema_version: state.schema_version
    }
  end

  defp maybe_chunk(payload, base_metadata, %{chunking_enabled: true} = state) do
    payload_size = byte_size(payload)

    case chunk_payload_budget(base_metadata, payload_size, state) do
      # The metadata on its own fills the broker's limit, and smaller chunks would only
      # repeat it, so no split can rescue this message.
      chunk_size when chunk_size < 1 ->
        {:error, :metadata_too_large}

      chunk_size when payload_size > chunk_size ->
        {:ok, split_into_chunks(payload, payload_size, chunk_size, state)}

      _ ->
        {:ok, [{payload, nil}]}
    end
  end

  defp maybe_chunk(payload, _base_metadata, _state), do: {:ok, [{payload, nil}]}

  defp split_into_chunks(payload, payload_size, chunk_size, state) do
    uuid = Uniq.UUID.uuid4()
    num_chunks = div(payload_size + chunk_size - 1, chunk_size)

    Logger.debug("Chunking message: #{payload_size} bytes into #{num_chunks} chunks (max: #{chunk_size} bytes each)")

    :telemetry.execute(
      [:pulsar, :producer, :chunk, :start],
      %{total_size: payload_size, num_chunks: num_chunks, chunk_size: chunk_size},
      Map.put(producer_metadata(state), :uuid, uuid)
    )

    Enum.map(0..(num_chunks - 1), fn chunk_id ->
      offset = chunk_id * chunk_size
      chunk_data = binary_part(payload, offset, min(payload_size - offset, chunk_size))

      chunk_metadata = %{
        uuid: uuid,
        chunk_id: chunk_id,
        num_chunks: num_chunks,
        total_chunk_msg_size: payload_size
      }

      {chunk_data, chunk_metadata}
    end)
  end

  # The broker's limit covers the metadata each chunk repeats as well as its payload.
  # :max_message_size counts payload bytes only, so the deduction applies to the broker's
  # limit alone.
  defp chunk_payload_budget(base_metadata, total_size, state) do
    case state.broker_max_message_size do
      broker_limit when is_integer(broker_limit) and broker_limit > 0 ->
        metadata = %{
          base_metadata
          | uuid: @uuid_placeholder,
            chunk_id: 0,
            num_chunks_from_msg: 0,
            total_chunk_msg_size: total_size
        }

        overhead = byte_size(Binary.MessageMetadata.encode(metadata)) + @chunk_framing_overhead
        min(state.max_message_size, broker_limit - overhead)

      _ ->
        state.max_message_size
    end
  end

  defp build_schema(nil), do: nil

  defp build_schema(opts) when is_list(opts) do
    case Schema.new(opts) do
      {:ok, schema} -> schema
      {:error, reason} -> raise ArgumentError, "invalid schema: #{inspect(reason)}"
    end
  end
end
