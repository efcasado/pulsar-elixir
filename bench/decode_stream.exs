Code.require_file("support/frames.ex", __DIR__)

alias Pulsar.Bench.Frames

# A frame larger than one delivery arrives in pieces, and the broker process
# reassembles it. Every consumer and producer on the connection waits while it
# does, so the cost of reassembly is the cost of the whole connection stalling.
#
# gen_tcp in active mode hands over whatever the kernel has buffered, not one
# segment at a time: sending 4MB over loopback with this client's socket options
# arrives in ~460 deliveries with a 9KB median. 1400 bytes is near the smallest
# delivery worth expecting, so the two sizes bracket the realistic range.
max_frame_size = Pulsar.Protocol.default_max_frame_size()

inputs =
  for kb <- [64, 512, 4096], delivery <- [1400, 9216], into: %{} do
    frame = Frames.message(kb * 1024)
    packets = Frames.packets(frame, delivery)

    {"#{kb}KB payload in #{length(packets)} x #{delivery}B deliveries", packets}
  end

feed = fn packets ->
  Enum.reduce(packets, <<>>, fn packet, buffer ->
    {:ok, _commands, rest} = Pulsar.Protocol.decode_stream(buffer, packet, max_frame_size)
    rest
  end)
end

# A frame that never completes is never reassembled, checksummed or decoded, so
# the benchmark would report the cost of accumulating packets and nothing else.
for {label, packets} <- inputs do
  case feed.(packets) do
    {<<>>, [], 0} -> :ok
    leftover -> raise "#{label} left #{inspect(leftover, limit: 3)} unparsed; the frame did not complete"
  end
end

Benchee.run(
  %{
    "decode_stream" => feed
  },
  inputs: inputs,
  warmup: 1,
  time: 2,
  memory_time: 0.5,
  print: [fast_warning: false]
)
