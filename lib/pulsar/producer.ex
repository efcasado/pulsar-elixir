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
    :compression
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
          compression: :NONE | :LZ4 | :ZLIB | :SNAPPY | :ZSTD
        }

  ## Public API

  @doc """
  Starts a producer process.

  ## Parameters

  - `topic` - The topic to publish to
  - `opts` - Additional options:
    - `:access_mode` - Producer access mode (default: :Shared)
    - `:compression` - Compression algorithm (default: :NONE)
    - `:startup_delay_ms` - Fixed startup delay in milliseconds before producer initialization (default: 1000, matches broker conn_timeout)
    - `:startup_jitter_ms` - Maximum random startup delay in milliseconds to avoid thundering herd (default: 1000)

  The total startup delay is `startup_delay_ms + random(0, startup_jitter_ms)`, applied on every producer start/restart.
  The default `startup_delay_ms` matches the broker's `conn_timeout` to ensure the broker has time to reconnect
  before producers start requesting topic lookups.

  The producer will automatically use service discovery to find the broker.
  The broker will assign a unique producer name.
  """
  def start_link(topic, opts \\ []) do
    {access_mode, genserver_opts} = Keyword.pop(opts, :access_mode, :Shared)
    {compression, genserver_opts} = Keyword.pop(genserver_opts, :compression, :NONE)
    {startup_delay_ms, genserver_opts} = Keyword.pop(genserver_opts, :startup_delay_ms, Config.startup_delay())
    {startup_jitter_ms, genserver_opts} = Keyword.pop(genserver_opts, :startup_jitter_ms, Config.startup_jitter())
    {client, _genserver_opts} = Keyword.pop(genserver_opts, :client, :default)

    producer_config = %{
      client: client,
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
  """
  @spec send_message(pid(), binary(), timeout()) :: {:ok, map()} | {:error, term()}
  def send_message(producer_pid, payload, timeout \\ 5000) do
    GenServer.call(producer_pid, {:send_message, payload}, timeout)
  end

  ## GenServer Callbacks

  @impl true
  def init(producer_config) do
    # Trap exits so terminate/2 is called on shutdown
    Process.flag(:trap_exit, true)

    %{
      client: client,
      topic: topic,
      access_mode: access_mode,
      compression: compression,
      startup_delay_ms: startup_delay_ms,
      startup_jitter_ms: startup_jitter_ms
    } = producer_config

    producer_id = System.unique_integer([:positive, :monotonic])

    state = %__MODULE__{
      client: client,
      topic: topic,
      producer_id: producer_id,
      producer_name: nil,
      sequence_id: 0,
      pending_sends: %{},
      access_mode: access_mode,
      compression: compression
    }

    Logger.info("Starting producer for topic #{topic}")

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
  def handle_call({:send_message, payload}, from, state) do
    sequence_id = state.sequence_id + 1

    # Create CommandSend
    command_send = %Binary.CommandSend{
      producer_id: state.producer_id,
      sequence_id: sequence_id
    }

    # Create MessageMetadata
    message_metadata = %Binary.MessageMetadata{
      producer_name: state.producer_name,
      sequence_id: sequence_id,
      publish_time: System.system_time(:millisecond),
      uncompressed_size: byte_size(payload),
      compression: state.compression
    }

    payload = maybe_compress(message_metadata, payload)

    case Pulsar.Broker.publish_message(state.broker_pid, command_send, message_metadata, payload) do
      :ok ->
        # Emit telemetry event for message published
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
        new_pending = Map.put(state.pending_sends, sequence_id, {from, %{}})
        new_state = %{state | sequence_id: sequence_id, pending_sends: new_pending}
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
  def terminate(_reason, nil) do
    :ok
  end

  @impl true
  def terminate(_reason, state) do
    Logger.debug("Terminating producer: #{inspect(state.producer_name)}")

    start_metadata = %{
      topic: state.topic,
      producer_id: state.producer_id
    }

    :telemetry.span(
      [:pulsar, :producer, :closed],
      start_metadata,
      fn ->
        result =
          case close_producer(state.broker_pid, state.producer_id) do
            {:ok, _response} ->
              :ok

            {:error, reason} = error ->
              Logger.error("Producer close failed: #{inspect(reason)}")
              error
          end

        stop_metadata =
          Map.merge(start_metadata, %{
            success: match?(:ok, result),
            producer_name: state.producer_name
          })

        {result, stop_metadata}
      end
    )

    :ok
  end

  ## Private Functions

  defp register_with_broker(state, broker_pid) do
    start_metadata = %{
      topic: state.topic,
      producer_id: state.producer_id
    }

    [:pulsar, :producer, :opened]
    |> :telemetry.span(
      start_metadata,
      fn ->
        result =
          with :ok <- Pulsar.Broker.register_producer(broker_pid, state.producer_id, self()),
               {:ok, response} <- create_producer(broker_pid, state) do
            producer_name = response.producer_name
            {:ok, %{state | broker_pid: broker_pid, producer_name: producer_name}}
          else
            {:error, reason} = error ->
              Logger.error("Producer registration failed: #{inspect(reason)}")
              error
          end

        stop_metadata =
          Map.merge(start_metadata, %{
            success: match?({:ok, _}, result),
            producer_name: if(match?({:ok, _}, result), do: elem(result, 1).producer_name)
          })

        {result, stop_metadata}
      end
    )
    |> case do
      {:ok, new_state} ->
        {:noreply, new_state, {:continue, :monitor_broker}}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  defp create_producer(broker_pid, state) do
    request_id = System.unique_integer([:positive, :monotonic])

    producer_command = %Binary.CommandProducer{
      topic: state.topic,
      producer_id: state.producer_id,
      request_id: request_id
    }

    Pulsar.Broker.send_request(broker_pid, producer_command)
  end

  defp close_producer(nil, _producer_id) do
    {:ok, :skipped}
  end

  defp close_producer(broker_pid, producer_id) do
    close_producer_command = %Binary.CommandCloseProducer{
      producer_id: producer_id
    }

    Pulsar.Broker.send_request(broker_pid, close_producer_command)
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
end
