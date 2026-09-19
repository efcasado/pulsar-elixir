alias Pulsar.Bench.Compression
alias Pulsar.Bench.Transport

Code.require_file("support/compression.ex", __DIR__)
Code.require_file("support/transport.ex", __DIR__)

# Framing a message as iodata removes the copies that building it as one binary needs, but
# it only pays off if the socket takes iodata as cheaply as a binary. A sink measures the
# framing alone and would report the whole saving; the loopback transport sends the frame
# for real and waits for it to arrive, which is where a socket that flattens hands it back.
#
# Uncompressed inputs are the ones to watch: with no codec in the way, framing is all the
# work a publish does. Under zstd the codec dominates and the copies are a rounding error.
#
# The consumer is here because the receive path is not part of this change and should not
# move. It reads a frame that was already reassembled, so it is measured once rather than
# per transport.
{:ok, sink} = GenServer.start_link(Compression.Sink, :discard)
{:ok, tcp} = GenServer.start_link(Transport, {:gen_tcp, []})

try do
  inputs =
    for {size, count} <- [{470, 1}, {47_000, 100}, {1_048_576, 1}],
        kind <- ["json", "entropy"],
        codec <- [:none, :zstd],
        into: %{} do
      input = Compression.input(size, kind, count, sink, 3, codec)

      {"#{codec}: #{kind} #{count} x #{div(size, count)}B",
       Map.put(input, :over_tcp, %{input.producer | broker_pid: tcp})}
    end

  Benchee.run(
    %{
      "producer (sink)" => &Compression.produce(&1, 3),
      "producer (tcp)" => fn input -> Compression.produce(%{input | producer: input.over_tcp}, 3) end,
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
  GenServer.stop(tcp)
  GenServer.stop(sink)
end
