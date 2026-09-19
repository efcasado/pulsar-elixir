alias Pulsar.Bench.Compression

Code.require_file("support/compression.ex", __DIR__)

{:ok, sink} = GenServer.start_link(Compression.Sink, :discard)

try do
  inputs = Map.new(Compression.inputs(), fn {label, input} -> {label, Compression.prepare(input, sink)} end)

  IO.puts("\nPayload bytes (before/after compression; batches include message metadata):")

  inputs
  |> Enum.sort_by(&elem(&1, 0))
  |> Enum.each(fn {label, input} ->
    ratio = Float.round(input.compressed_bytes / input.uncompressed_bytes, 6)
    IO.puts("#{label}: #{input.uncompressed_bytes} -> #{input.compressed_bytes} (#{ratio}x)")
  end)

  seconds = fn name, default -> name |> System.get_env(default) |> Float.parse() |> elem(0) end

  options = [
    inputs: inputs,
    warmup: seconds.("BENCH_WARMUP", "1"),
    time: seconds.("BENCH_TIME", "2"),
    memory_time: seconds.("BENCH_MEMORY_TIME", "0.5"),
    reduction_time: seconds.("BENCH_REDUCTION_TIME", "0.5"),
    parallel: "BENCH_PARALLEL" |> System.get_env("1") |> String.to_integer(),
    print: [fast_warning: false]
  ]

  options =
    case System.get_env("BENCH_SAVE") do
      nil -> options
      path -> Keyword.put(options, :save, path: path, tag: System.get_env("BENCH_TAG", "compression"))
    end

  options =
    case System.get_env("BENCH_LOAD") do
      nil -> options
      path -> Keyword.put(options, :load, path)
    end

  Benchee.run(%{"producer" => &Compression.produce/1, "consumer" => &Compression.consume/1}, options)
after
  GenServer.stop(sink)
end
