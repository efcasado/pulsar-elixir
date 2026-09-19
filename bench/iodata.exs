alias Pulsar.Bench.Compression
alias Pulsar.Bench.Transport

Code.require_file("support/compression.ex", __DIR__)
Code.require_file("support/transport.ex", __DIR__)

{opts, []} =
  OptionParser.parse!(System.argv(),
    strict: [output: :string, time: :float, warmup: :float, cert: :string, key: :string]
  )

{:ok, sink} = GenServer.start_link(Compression.Sink, :discard)
{:ok, tcp} = GenServer.start_link(Transport, {:gen_tcp, []})

transports =
  case {opts[:cert], opts[:key]} do
    {nil, nil} ->
      [{"sink", sink}, {"tcp", tcp}]

    {cert, key} when is_binary(cert) and is_binary(key) ->
      {:ok, tls} = GenServer.start_link(Transport, {:ssl, [certfile: cert, keyfile: key]})
      [{"sink", sink}, {"tcp", tcp}, {"tls", tls}]
  end

try do
  inputs =
    for {transport, broker} <- transports,
        {size, count} <- [{470, 1}, {47_000, 100}, {1_048_576, 1}],
        kind <- ["json", "entropy"],
        codec <- [:none, :zstd],
        into: %{} do
      name = "#{transport}/#{codec}/#{kind}/#{count}x#{div(size, count)}"
      {name, Compression.input(size, kind, count, broker, 3, codec)}
    end

  suite =
    Benchee.run(%{"produce" => &Compression.produce(&1, 3)},
      inputs: inputs,
      warmup: Keyword.get(opts, :warmup, 0.5),
      time: Keyword.get(opts, :time, 2.0),
      memory_time: 0.2,
      reduction_time: 0.2,
      percentiles: [50, 95, 99],
      print: [fast_warning: false]
    )

  rows =
    Enum.map(suite.scenarios, fn scenario ->
      runtime = scenario.run_time_data.statistics

      %{
        scenario: scenario.input_name,
        median_ns: runtime.median,
        p95_ns: runtime.percentiles[95],
        p99_ns: runtime.percentiles[99],
        entries_per_second: runtime.ips,
        producer_allocated_bytes: scenario.memory_usage_data.statistics.average,
        producer_reductions: scenario.reductions_data.statistics.average,
        samples: runtime.sample_size
      }
    end)

  if output = opts[:output] do
    File.write!(
      output,
      Jason.encode!(%{otp: System.otp_release(), elixir: System.version(), results: rows}, pretty: true)
    )
  end
after
  for {_name, broker} <- transports, do: GenServer.stop(broker)
end
