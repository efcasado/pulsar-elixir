alias Pulsar.Bench.Compression

Code.require_file("support/compression.ex", __DIR__)

# Measure native Zstandard through the workers: frame encoding and broker handoff
# for producers, decompression and callback dispatch for consumers. The broker is
# a sink, so network and persistence are outside the measurement. Each input is
# round-tripped before timing to verify that every payload reaches the callback.
{:ok, sink} = GenServer.start_link(Compression.Sink, :discard)

try do
  inputs =
    for size <- [100, 1024, 10_240, 102_400, 1_048_576, 5_242_880],
        kind <- ["text", "entropy"],
        count <- [1, 100],
        into: %{} do
      {"#{kind}: #{count} x #{div(size, count)}B", Compression.input(size, kind, count, sink)}
    end

  Benchee.run(
    %{
      "producer" => &Compression.produce/1,
      "consumer" => &Compression.consume/1
    },
    inputs: inputs,
    warmup: 1,
    time: 2,
    memory_time: 0.5,
    reduction_time: 0.5,
    print: [fast_warning: false]
  )
after
  GenServer.stop(sink)
end
