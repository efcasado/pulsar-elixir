defmodule Pulsar.Benchmarks.ElixirProducer do
  @moduledoc false
  import Bitwise

  @default_url "pulsar://localhost:6650"
  @default_topic "persistent://public/default/benchmark-producer"
  @default_messages 100_000
  @default_size 1_024
  @default_partitions 1
  @default_in_flight 1_000
  @default_timeout_ms 30_000
  @high_entropy_seed 0xA5A5A5A5

  def run(opts) do
    validate!(opts)

    {:ok, _client} = Pulsar.Client.start_link(name: :benchmark, host: opts.url)

    outcome =
      try do
        producer = start_producer(opts)
        payload = payload(opts.size, opts.payload_mode)

        started_at = System.monotonic_time(:microsecond)
        {latencies, acked, errors} = publish(producer, opts.messages, payload, opts)

        duration_us = max(System.monotonic_time(:microsecond) - started_at, 1)

        result = %{
          "schema_version" => 1,
          "operation" => "producer",
          "client" => "elixir",
          "topic" => opts.topic,
          "messages_requested" => opts.messages,
          "messages_acked" => acked,
          "payload_bytes" => opts.size,
          "payload_mode" => payload_mode_name(opts.payload_mode),
          "compression" => compression_name(opts.compression),
          "partitions" => opts.partitions,
          "in_flight" => opts.in_flight,
          "batching" => opts.batching,
          "batch_size" => opts.batch_size,
          "batch_delay_ms" => opts.batch_delay_ms,
          "duration_us" => duration_us,
          "messages_per_second" => rate(acked, duration_us),
          "bytes_per_second" => rate(acked * opts.size, duration_us),
          "latency_type" => "producer_ack",
          "p50_us" => percentile(latencies, 0.50),
          "p95_us" => percentile(latencies, 0.95),
          "p99_us" => percentile(latencies, 0.99),
          "errors" => errors
        }

        if acked != opts.messages or errors > 0 do
          {:error, result}
        else
          {:ok, result}
        end
      after
        Pulsar.Client.stop(:benchmark)
      end

    case outcome do
      {:ok, result} ->
        IO.puts(Jason.encode!(result))

      {:error, result} ->
        IO.puts(Jason.encode!(result))
        System.halt(1)
    end
  end

  def parse_args(args) do
    args = if List.first(args) == "--", do: tl(args), else: args

    {opts, []} =
      OptionParser.parse!(args,
        strict: [
          url: :string,
          topic: :string,
          messages: :integer,
          size: :integer,
          partitions: :integer,
          in_flight: :integer,
          timeout_ms: :integer,
          batching: :boolean,
          batch_size: :integer,
          batch_delay_ms: :integer,
          compression: :string,
          payload: :string
        ]
      )

    %{
      url: Keyword.get(opts, :url, System.get_env("PULSAR_URL", @default_url)),
      topic: Keyword.get(opts, :topic, System.get_env("BENCHMARK_TOPIC", @default_topic)),
      messages: Keyword.get(opts, :messages, @default_messages),
      size: Keyword.get(opts, :size, @default_size),
      partitions: Keyword.get(opts, :partitions, @default_partitions),
      in_flight: Keyword.get(opts, :in_flight, @default_in_flight),
      timeout_ms: Keyword.get(opts, :timeout_ms, @default_timeout_ms),
      batching: Keyword.get(opts, :batching, false),
      batch_size: Keyword.get(opts, :batch_size, 100),
      batch_delay_ms: Keyword.get(opts, :batch_delay_ms, 10),
      compression: parse_compression(Keyword.get(opts, :compression, "none")),
      payload_mode: parse_payload_mode(Keyword.get(opts, :payload, "zero"))
    }
  end

  defp start_producer(opts) do
    producer_opts = [
      client: :benchmark,
      topic: opts.topic,
      name: "benchmark-producer",
      compression: opts.compression,
      batch_enabled: opts.batching,
      batch_size: opts.batch_size,
      flush_interval: opts.batch_delay_ms,
      max_pending_messages: max(opts.in_flight, 1)
    ]

    {:ok, producer} = Pulsar.Producer.start(producer_opts)

    case Pulsar.Producer.await_ready(producer, timeout: opts.timeout_ms) do
      :ok -> producer
      {:error, reason} -> raise "producer did not become ready: #{inspect(reason)}"
    end
  end

  defp publish(_producer, 0, _payload, _opts), do: {[], 0, 0}

  defp publish(producer, count, payload, opts) do
    1..count
    |> Stream.chunk_every(opts.in_flight)
    |> Enum.reduce({[], 0, 0}, fn window, {latencies, acked, errors} ->
      pending =
        Enum.reduce(window, [], fn _message, pending ->
          started_at = System.monotonic_time(:microsecond)

          case Pulsar.Producer.send_async(producer, payload) do
            {:ok, ref} -> [{:pending, ref, started_at} | pending]
            {:error, _reason} -> [{:error} | pending]
          end
        end)

      await_window(pending, opts.timeout_ms, {latencies, acked, errors})
    end)
  end

  defp await_window([], _timeout_ms, result), do: result

  defp await_window([{:error} | pending], timeout_ms, {latencies, acked, errors}) do
    await_window(pending, timeout_ms, {latencies, acked, errors + 1})
  end

  defp await_window([{:pending, ref, started_at} | pending], timeout_ms, {latencies, acked, errors}) do
    case Pulsar.Producer.await(ref, timeout_ms) do
      {:ok, _message_id} ->
        latency = System.monotonic_time(:microsecond) - started_at
        await_window(pending, timeout_ms, {[latency | latencies], acked + 1, errors})

      {:error, _reason} ->
        await_window(pending, timeout_ms, {latencies, acked, errors + 1})
    end
  end

  defp rate(value, duration_us), do: Float.round(value * 1_000_000 / duration_us, 3)

  defp percentile([], _quantile), do: nil

  defp percentile(values, quantile) do
    sorted = Enum.sort(values)
    index = max(trunc(Float.ceil(length(sorted) * quantile)) - 1, 0)
    Enum.at(sorted, index)
  end

  defp payload(size, :zero), do: :binary.copy(<<0>>, size)

  defp payload(0, :high_entropy), do: <<>>

  defp payload(size, :high_entropy) do
    {bytes, _state} =
      Enum.map_reduce(1..size, @high_entropy_seed, fn _index, state ->
        state = xorshift32(state)
        {state >>> 24, state}
      end)

    :erlang.list_to_binary(bytes)
  end

  defp xorshift32(state) do
    state = bxor(state, band(state <<< 13, 0xFFFFFFFF))
    state = bxor(state, state >>> 17)
    band(bxor(state, band(state <<< 5, 0xFFFFFFFF)), 0xFFFFFFFF)
  end

  defp parse_compression("none"), do: :none
  defp parse_compression("zstd"), do: :zstd

  defp parse_compression(value), do: raise(ArgumentError, "compression must be one of: none, zstd (got #{value})")

  defp parse_payload_mode("zero"), do: :zero
  defp parse_payload_mode("high-entropy"), do: :high_entropy

  defp parse_payload_mode(value), do: raise(ArgumentError, "payload must be one of: zero, high-entropy (got #{value})")

  defp compression_name(:none), do: "none"
  defp compression_name(:zstd), do: "zstd"

  defp payload_mode_name(:zero), do: "zero"
  defp payload_mode_name(:high_entropy), do: "high-entropy"

  defp validate!(opts) do
    validations = [
      {opts.messages > 0, "messages must be greater than zero"},
      {opts.size >= 0, "size must not be negative"},
      {opts.partitions > 0, "partitions must be greater than zero"},
      {opts.in_flight > 0, "in_flight must be greater than zero"},
      {opts.timeout_ms > 0, "timeout_ms must be greater than zero"},
      {opts.batch_size > 0, "batch_size must be greater than zero"},
      {opts.batch_delay_ms > 0, "batch_delay_ms must be greater than zero"}
    ]

    case Enum.find(validations, fn {valid, _message} -> not valid end) do
      nil -> :ok
      {_valid, message} -> raise ArgumentError, message
    end
  end
end

Pulsar.Benchmarks.ElixirProducer.run(Pulsar.Benchmarks.ElixirProducer.parse_args(System.argv()))
