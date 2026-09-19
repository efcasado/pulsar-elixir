defmodule Pulsar.Bench.Compression do
  @moduledoc "Builders and worker callbacks for the native Zstandard benchmark."

  alias Pulsar.Consumer.Ack
  alias Pulsar.Consumer.Worker, as: Consumer
  alias Pulsar.Producer.Worker, as: Producer
  alias Pulsar.Protocol
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

  # The producer's own default, so a plain run measures what a producer publishes at.
  @default_level 3

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

  def input(size, kind, count, sink, level \\ @default_level, compression \\ :zstd) do
    payloads = Enum.map(1..count, &payload(kind, div(size, count), &1))
    input = %{payloads: payloads, count: count, level: level, compression: compression}
    {:ok, capture} = GenServer.start_link(Sink, {:capture, self()})

    try do
      producer = producer_state(capture, input)
      produce(Map.put(input, :producer, producer), level)

      frame =
        receive do
          {:frame, frame} -> frame
        after
          1000 -> raise "producer did not publish a complete frame"
        end

      {:ok, {_send, metadata, compressed, nil}} = Protocol.decode(IO.iodata_to_binary(frame))
      command = %Binary.CommandMessage{consumer_id: 1, message_id: %Binary.MessageIdData{ledgerId: 1, entryId: 1}}
      delivery = {:broker_message, {command, metadata, compressed, nil}}
      consumer = %{consumer_state(input.count) | zstd_context: own_context()}
      validating = %{consumer | callback_state: {:validate, input.payloads, 0}}
      {:noreply, checked} = Consumer.handle_info(delivery, validating)
      {:validate, [], count} = checked.callback_state
      if count != input.count, do: raise("consumer did not deliver every message")

      Map.merge(input, %{
        producer: producer_state(sink, input),
        consumer: consumer,
        delivery: delivery
      })
    after
      GenServer.stop(capture)
    end
  end

  def produce(input, level) do
    from = {self(), make_ref()}

    state =
      Enum.reduce(input.payloads, %{input.producer | compression_level: level}, fn payload, state ->
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
    {:noreply, state} = Consumer.handle_info(input.delivery, worker(input))
    if state.callback_state != input.count, do: raise("consumer dropped messages")
    state
  end

  # A decompression context belongs to the process that created it, and Benchee measures time,
  # memory and reductions from three processes of its own. So each is given a worker of its own
  # the first time it runs one, which is what a worker holds anyway: one context, reused.
  defp worker(input) do
    case Process.get(:bench_consumer) do
      nil ->
        state = %{input.consumer | zstd_context: own_context()}
        Process.put(:bench_consumer, state)
        state

      state ->
        state
    end
  end

  defp producer_state(broker, input) do
    %Producer{
      topic: "persistent://public/default/bench",
      base_topic: "persistent://public/default/bench",
      producer_id: 1,
      producer_name: "bench",
      broker_pid: broker,
      ready: true,
      compression: input.compression,
      compression_level: input.level,
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

  defp own_context do
    {:ok, context} = :zstd.context(:decompress)
    context
  end

  # A CloudEvents-shaped domain event, which is what these systems mostly carry: field names
  # and enum values repeat across records, while ids, amounts and timestamps do not. A payload
  # of one repeated record compresses to almost nothing and one of random bytes not at all, so
  # neither shows what a codec or a level is worth on real traffic.
  defp payload("json", size, message_index) do
    :rand.seed(:exsss, {message_index, 20_260_919, size})
    records(size, message_index * 1_000_000, [])
  end

  defp payload("entropy", size, message_index) do
    # Stable across branches and runs; generation is outside the measured functions.
    bytes = for index <- 1..(div(size, 32) + 1), into: <<>>, do: :crypto.hash(:sha256, <<message_index::32, index::64>>)
    binary_part(bytes, 0, size)
  end

  @statuses ~w(created paid shipped delivered refunded cancelled)
  @currencies ~w(EUR USD GBP SEK)
  @countries ~w(ES DE FR SE US GB)

  defp records(size, index, acc) do
    if IO.iodata_length(acc) >= size do
      acc |> IO.iodata_to_binary() |> binary_part(0, size)
    else
      records(size, index + 1, [acc, record(index)])
    end
  end

  defp record(index) do
    id = :sha256 |> :crypto.hash(<<index::64>>) |> Base.encode16(case: :lower)
    order = binary_part(id, 0, 12)
    customer = binary_part(id, 12, 10)
    minute = rem(index, 60)
    second = rem(index * 7, 60)

    items =
      Enum.map_join(1..:rand.uniform(3), ",", fn item ->
        ~s({"sku":"SKU-#{:rand.uniform(99_999)}","qty":#{:rand.uniform(5)},"price":#{:rand.uniform(19_999) / 100},"line":#{item}})
      end)

    ~s({"specversion":"1.0","type":"com.example.order.updated","source":"/orders/api",) <>
      ~s("id":"#{binary_part(id, 22, 32)}","time":"2026-09-19T10:#{pad(minute)}:#{pad(second)}.#{rem(index, 1000)}Z",) <>
      ~s("subject":"order-#{order}","datacontenttype":"application/json",) <>
      ~s("data":{"orderId":"ord_#{order}","customerId":"cus_#{customer}",) <>
      ~s("status":"#{Enum.random(@statuses)}","currency":"#{Enum.random(@currencies)}",) <>
      ~s("total":#{:rand.uniform(250_000) / 100},"items":[#{items}],) <>
      ~s("shipping":{"country":"#{Enum.random(@countries)}","postalCode":"#{:rand.uniform(89_999) + 10_000}"}}}\n)
  end

  defp pad(value), do: value |> Integer.to_string() |> String.pad_leading(2, "0")
end
