defmodule Pulsar.Benchmarks.ElixirConsumerCallback do
  @moduledoc false
  use Pulsar.Consumer.Callback

  def init(opts, _context) do
    {:ok, %{forward_to: Keyword.fetch!(opts, :forward_to)}}
  end

  def handle_message(%Pulsar.Message{} = message, state) do
    properties = Pulsar.Message.properties(message)
    publish_ns = parse_integer(Map.get(properties, "benchmark-publish-ns"))

    send(
      state.forward_to,
      {:benchmark_consumer_message, System.system_time(:nanosecond), publish_ns}
    )

    {:ok, state}
  end

  defp parse_integer(nil), do: nil

  defp parse_integer(value) do
    case Integer.parse(value) do
      {parsed, ""} -> parsed
      _invalid -> nil
    end
  end
end

defmodule Pulsar.Benchmarks.ElixirConsumer do
  @moduledoc false

  @default_url "pulsar://localhost:6650"
  @default_topic "persistent://public/default/benchmark-consumer"
  @default_messages 100_000
  @default_size 1_024
  @default_partitions 1
  @default_timeout_ms 30_000

  def run(opts) do
    validate!(opts)
    {:ok, _client} = Pulsar.Client.start_link(name: :benchmark, host: opts.url)

    outcome =
      try do
        {:ok, consumer} =
          Pulsar.Consumer.start(
            topic: opts.topic,
            subscription_name: opts.subscription_name,
            callback_module: Pulsar.Benchmarks.ElixirConsumerCallback,
            client: :benchmark,
            subscription_type: opts.subscription_type,
            consumer_count: opts.consumers_per_partition,
            initial_position: :earliest,
            init_args: [forward_to: self()]
          )

        try do
          case Pulsar.Consumer.await_ready(consumer, timeout: opts.timeout_ms) do
            :ok ->
              started_at = System.monotonic_time(:microsecond)
              deadline = started_at + opts.timeout_ms * 1_000
              collect(consumer, opts, started_at, deadline)

            {:error, reason} ->
              raise "consumer did not become ready: #{inspect(reason)}"
          end
        after
          Pulsar.Consumer.stop(consumer, client: :benchmark)
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
          timeout_ms: :integer,
          subscription_type: :string,
          subscription_name: :string,
          consumers_per_partition: :integer
        ]
      )

    %{
      url: Keyword.get(opts, :url, System.get_env("PULSAR_URL", @default_url)),
      topic: Keyword.get(opts, :topic, System.get_env("BENCHMARK_TOPIC", @default_topic)),
      messages: Keyword.get(opts, :messages, @default_messages),
      size: Keyword.get(opts, :size, @default_size),
      partitions: Keyword.get(opts, :partitions, @default_partitions),
      timeout_ms: Keyword.get(opts, :timeout_ms, @default_timeout_ms),
      subscription_type: parse_subscription_type(Keyword.get(opts, :subscription_type, "shared")),
      subscription_name: Keyword.get(opts, :subscription_name, "benchmark-consumer"),
      consumers_per_partition: Keyword.get(opts, :consumers_per_partition, 1)
    }
  end

  defp collect(consumer, opts, started_at, deadline) do
    collect(consumer, opts, opts.messages, started_at, deadline, [], 0, nil)
  end

  defp collect(_consumer, opts, 0, started_at, _deadline, latencies, received, last_received) do
    duration_us = max((last_received || started_at) - started_at, 1)
    {:ok, result(opts, duration_us, latencies, started_at, last_received, received)}
  end

  defp collect(consumer, opts, remaining, started_at, deadline, latencies, received, _last_received) do
    wait_started = System.monotonic_time(:microsecond)
    timeout_ms = max(div(deadline - wait_started, 1_000), 1)

    receive do
      {:benchmark_consumer_message, received_at_ns, publish_ns} ->
        received_at = System.monotonic_time(:microsecond)
        latency = e2e_latency_us(received_at_ns, publish_ns)

        collect(
          consumer,
          opts,
          remaining - 1,
          started_at,
          deadline,
          [latency | latencies],
          received + 1,
          received_at
        )
    after
      timeout_ms ->
        result =
          result(
            opts,
            max(wait_started - started_at, 1),
            latencies,
            started_at,
            wait_started,
            received
          )

        {:error, %{result | "errors" => 1, "timeout" => true}}
    end
  end

  defp e2e_latency_us(_received_at_ns, nil), do: nil

  defp e2e_latency_us(received_at_ns, publish_ns) do
    max(div(received_at_ns - publish_ns, 1_000), 0)
  end

  defp result(opts, duration_us, latencies, _started_at, _last_received, received) do
    messages_per_second = rate(received, duration_us)

    %{
      "schema_version" => 1,
      "operation" => "consumer",
      "client" => "elixir",
      "topic" => opts.topic,
      "messages_requested" => opts.messages,
      "messages_received" => received,
      "messages_acked" => received,
      "payload_bytes" => opts.size,
      "partitions" => opts.partitions,
      "subscription_type" => subscription_type_name(opts.subscription_type),
      "subscription_name" => opts.subscription_name,
      "consumers_per_partition" => opts.consumers_per_partition,
      "duration_us" => duration_us,
      "messages_per_second" => messages_per_second,
      "bytes_per_second" => rate(received * opts.size, duration_us),
      "latency_type" => "consumer_e2e",
      "p50_us" => percentile(latencies, 0.50),
      "p95_us" => percentile(latencies, 0.95),
      "p99_us" => percentile(latencies, 0.99),
      "errors" => 0
    }
  end

  defp rate(value, duration_us), do: Float.round(value * 1_000_000 / duration_us, 3)

  defp percentile([], _quantile), do: nil

  defp percentile(values, quantile) do
    values = values |> Enum.reject(&is_nil/1) |> Enum.sort()

    case values do
      [] ->
        nil

      values ->
        index = max(trunc(Float.ceil(length(values) * quantile)) - 1, 0)
        Enum.at(values, index)
    end
  end

  defp parse_subscription_type("shared"), do: :shared
  defp parse_subscription_type("exclusive"), do: :exclusive
  defp parse_subscription_type("failover"), do: :failover
  defp parse_subscription_type("key-shared"), do: :key_shared

  defp parse_subscription_type(value) do
    raise ArgumentError,
          "subscription_type must be one of: shared, exclusive, failover, key-shared (got #{value})"
  end

  defp subscription_type_name(:shared), do: "shared"
  defp subscription_type_name(:exclusive), do: "exclusive"
  defp subscription_type_name(:failover), do: "failover"
  defp subscription_type_name(:key_shared), do: "key-shared"

  defp validate!(opts) do
    validations = [
      {opts.messages > 0, "messages must be greater than zero"},
      {opts.size >= 0, "size must not be negative"},
      {opts.partitions > 0, "partitions must be greater than zero"},
      {opts.timeout_ms > 0, "timeout_ms must be greater than zero"},
      {opts.consumers_per_partition > 0, "consumers_per_partition must be greater than zero"}
    ]

    case Enum.find(validations, fn {valid, _message} -> not valid end) do
      nil -> :ok
      {_valid, message} -> raise ArgumentError, message
    end
  end
end

Pulsar.Benchmarks.ElixirConsumer.run(Pulsar.Benchmarks.ElixirConsumer.parse_args(System.argv()))
