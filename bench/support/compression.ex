defmodule Pulsar.Bench.Compression do
  @moduledoc false

  alias Pulsar.Consumer.Ack
  alias Pulsar.Consumer.Worker, as: Consumer
  alias Pulsar.Producer.Worker, as: Producer
  alias Pulsar.Protocol
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

  # Keep broker handoff synchronous, like production, without a socket or unbounded mailbox.
  defmodule Sink do
    @moduledoc false
    use GenServer

    def init(mode), do: {:ok, mode}

    def handle_call({:publish_message, frame}, _from, {:capture, owner} = mode) do
      send(owner, {:frame, frame})
      {:reply, :ok, mode}
    end

    def handle_call({:publish_message, _frame}, _from, :discard), do: {:reply, :ok, :discard}
  end

  defmodule Callback do
    @moduledoc false
    use Pulsar.Consumer.Callback

    # Validation runs before timing. Timed runs count messages without retaining their payloads.
    def handle_message(message, {:validate, [expected | remaining], count}) do
      if message.payload != expected, do: raise("payload changed during compression round trip")
      {:noreply, {:validate, remaining, count + 1}}
    end

    def handle_message(_message, count), do: {:noreply, count + 1}

    def handle_invalid_message(message, _state) do
      raise "invalid benchmark message: #{inspect(message.validation_error)}"
    end
  end

  def inputs do
    for size <- integers("BENCH_SIZES", "100,1024,10240,102400,1048576,5242880"),
        kind <- ["text", "entropy"],
        count <- integers("BENCH_BATCHES", "1,100"),
        codec <- [:none, :zstd],
        into: %{} do
      # Size is the total application payload per operation, split over count messages.
      message_size = max(div(size, count), 1)
      payloads = Enum.map(1..count, &payload(kind, message_size, &1))
      label = "#{codec}/#{kind}/#{count} x #{message_size}B"
      {label, %{payloads: payloads, count: count, codec: codec}}
    end
  end

  def prepare(input, sink) do
    {:ok, capture} = GenServer.start_link(Sink, {:capture, self()})

    try do
      producer = producer_state(capture, input)
      produce(Map.put(input, :producer, producer))

      frame =
        receive do
          {:frame, frame} -> frame
        after
          1000 -> raise "producer did not publish a complete frame"
        end

      {:ok, {_send, metadata, compressed, nil}} = Protocol.decode(frame)
      command = %Binary.CommandMessage{consumer_id: 1, message_id: %Binary.MessageIdData{ledgerId: 1, entryId: 1}}
      delivery = {:broker_message, {command, metadata, compressed, nil}}
      consumer = consumer_state(input.count)
      validating = %{consumer | callback_state: {:validate, input.payloads, 0}}
      {:noreply, checked} = Consumer.handle_info(delivery, validating)
      {:validate, [], count} = checked.callback_state
      if count != input.count, do: raise("consumer did not deliver every message")

      Map.merge(input, %{
        producer: producer_state(sink, input),
        consumer: consumer,
        delivery: delivery,
        uncompressed_bytes: metadata.uncompressed_size,
        compressed_bytes: byte_size(compressed)
      })
    after
      GenServer.stop(capture)
    end
  end

  def produce(input) do
    from = {self(), make_ref()}

    state =
      Enum.reduce(input.payloads, input.producer, fn payload, state ->
        {:noreply, next} = Producer.handle_cast({:send_message, payload, [], from}, state)
        next
      end)

    # Every invocation starts from empty state; pending receipts cannot accumulate.
    if state.pending_messages != input.count or map_size(state.pending_frames) != 1 do
      raise "producer failed to publish one complete entry"
    end

    state
  end

  def consume(input) do
    {:noreply, state} = Consumer.handle_info(input.delivery, input.consumer)
    if state.callback_state != input.count, do: raise("consumer dropped messages")
    state
  end

  defp producer_state(broker, input) do
    %Producer{
      topic: "persistent://public/default/bench",
      base_topic: "persistent://public/default/bench",
      producer_id: 1,
      producer_name: "bench",
      broker_pid: broker,
      ready: true,
      compression: input.codec,
      chunking_enabled: false,
      batch_enabled: input.count > 1,
      batch_size: input.count,
      batch_builder: :default,
      send_timeout: false,
      max_pending_messages: false
    }
  end

  defp consumer_state(count) do
    %Consumer{
      topic: "persistent://public/default/bench",
      base_topic: "persistent://public/default/bench",
      consumer_id: 1,
      consumer_name: "bench",
      subscription_name: "bench",
      subscription_type: :shared,
      ready: true,
      callback_module: Callback,
      callback_state: 0,
      acks: Ack.new(),
      flow_policy: :auto,
      flow_outstanding_permits: count + 1,
      flow_threshold: 0,
      flow_refill: count
    }
  end

  defp payload("text", size, _message_index) do
    record = ~s({"event":"order.updated","account":"customer-1234","amount":19.95,"currency":"EUR"}\n)
    binary_part(:binary.copy(record, div(size, byte_size(record)) + 1), 0, size)
  end

  defp payload("entropy", size, message_index) do
    # Stable across branches and runs; generation is outside the measured functions.
    bytes = for index <- 1..(div(size, 32) + 1), into: <<>>, do: :crypto.hash(:sha256, <<message_index::32, index::64>>)
    binary_part(bytes, 0, size)
  end

  defp integers(name, default) do
    name |> System.get_env(default) |> String.split(",") |> Enum.map(&String.to_integer/1)
  end
end
