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
    :topic_epoch
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
          topic_epoch: integer() | nil
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
      startup_delay_ms: startup_delay_ms,
      startup_jitter_ms: startup_jitter_ms
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
    - `:key` - Partition routing key (string)
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
      startup_delay_ms: startup_delay_ms,
      startup_jitter_ms: startup_jitter_ms
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
      topic_epoch: topic_epoch
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
        register_with_broker(state, broker_pid)

      {:error, reason} ->
        Logger.error("Topic lookup failed: #{inspect(reason)}")
        {:stop, reason, state}
    end
  end

  @impl true
  def handle_continue(:monitor_broker, state) do
    broker_monitor = Process.monitor(state.broker_pid)
    {:noreply, %{state | broker_monitor: broker_monitor}}
  end

  @impl true
  def handle_call({:send_message, _, _}, _from, %{ready: false} = state) do
    Logger.warning("Producer #{state.producer_name} is waiting, cannot send message")
    {:reply, {:error, :producer_waiting}, state}
  end

  def handle_call({:send_message, payload, opts}, from, state) do
    sequence_id = state.sequence_id + 1

    command_send = %Binary.CommandSend{producer_id: state.producer_id, sequence_id: sequence_id}

    message_metadata = %Binary.MessageMetadata{
      producer_name: state.producer_name,
      sequence_id: sequence_id,
      publish_time: System.system_time(:millisecond),
      uncompressed_size: byte_size(payload),
      compression: state.compression,
      partition_key: Keyword.get(opts, :key),
      ordering_key: Keyword.get(opts, :ordering_key),
      properties: to_key_value_list(Keyword.get(opts, :properties)),
      event_time: to_timestamp(Keyword.get(opts, :event_time)),
      deliver_at_time: resolve_deliver_at_time(opts)
    }

    payload = maybe_compress(message_metadata, payload)

    case Pulsar.Broker.publish_message(state.broker_pid, command_send, message_metadata, payload) do
      :ok ->
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

        # Store pending send to correlate with receipt
        new_waiting = Map.put(state.pending_sends, sequence_id, {from, %{}})
        new_state = %{state | sequence_id: sequence_id, pending_sends: new_waiting}
        {:noreply, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  # Handle broker crashes - stop so supervisor can restart us with fresh lookup
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
  def handle_info({:send_receipt, %Binary.CommandSendReceipt{} = receipt}, state) do
    case Map.pop(state.pending_sends, receipt.sequence_id) do
      {{from, _metadata}, new_pending} ->
        GenServer.reply(from, {:ok, receipt.message_id})
        {:noreply, %{state | pending_sends: new_pending}}

      {nil, _} ->
        Logger.warning("Received receipt for unknown sequence_id #{receipt.sequence_id}")
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:send_error, %Binary.CommandSendError{} = error}, state) do
    case Map.pop(state.pending_sends, error.sequence_id) do
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

  defp register_with_broker(state, broker_pid) do
    start_metadata = %{
      topic: state.topic,
      producer_name: state.producer_name
    }

    [:pulsar, :producer, :opened]
    |> :telemetry.span(
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
    |> case do
      {:ok, new_state} ->
        {:noreply, new_state, {:continue, :monitor_broker}}

      {:error, {:ProducerFenced, _msg}} ->
        ProducerEpochStore.delete(state.client, state.topic, state.producer_name, state.access_mode)
        {:stop, {:shutdown, :producer_fenced}, state}

      {:error, reason} ->
        {:stop, reason, state}
    end
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

  # Convert a map of properties to a list of KeyValue structs
  defp to_key_value_list(nil), do: []

  defp to_key_value_list(props) when is_map(props) do
    Enum.map(props, fn {k, v} ->
      %Binary.KeyValue{key: to_string(k), value: to_string(v)}
    end)
  end

  # Convert DateTime or milliseconds to milliseconds timestamp
  defp to_timestamp(nil), do: nil
  defp to_timestamp(%DateTime{} = dt), do: DateTime.to_unix(dt, :millisecond)
  defp to_timestamp(ms) when is_integer(ms), do: ms

  # Resolve deliver_at_time from either absolute or relative delay option
  defp resolve_deliver_at_time(opts) do
    case {Keyword.get(opts, :deliver_at_time), Keyword.get(opts, :deliver_after)} do
      {nil, nil} -> nil
      {dt, _} when not is_nil(dt) -> to_timestamp(dt)
      {nil, ms} when is_integer(ms) -> System.system_time(:millisecond) + ms
    end
  end
end
