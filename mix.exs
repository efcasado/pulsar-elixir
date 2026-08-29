defmodule Pulsar.MixProject do
  use Mix.Project

  def project do
    [
      app: :pulsar,
      version: "3.1.1",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      dialyzer: [
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
        plt_add_apps: [:mix, :ex_unit]
      ],
      test_coverage: [tool: ExCoveralls],
      aliases: aliases(),
      name: "Pulsar",
      description: description(),
      package: package(),
      source_url: "https://github.com/efcasado/pulsar-elixir",
      docs: [
        main: "Pulsar",
        extras: [
          "README.md",
          "CHANGELOG.md",
          "docs/architecture.md",
          "docs/acknowledgements.md",
          "docs/batching.md",
          "docs/chunking.md",
          "docs/dead_letter_policies.md",
          "docs/schemas.md",
          "docs/reader.md",
          "docs/upgrading_to_3.0.md"
        ],
        groups_for_extras: [
          Guides: ~r/docs\/.*/
        ],
        skip_code_autolink_to: [
          # 2.x entry points, named by the upgrade guide but gone from 3.x
          "Pulsar.ack/3",
          "Pulsar.get_consumers/2",
          "Pulsar.get_producers/2",
          "Pulsar.nack/3",
          "Pulsar.send/3",
          "Pulsar.send_flow/2",
          "Pulsar.start/1",
          "Pulsar.start_broker/2",
          "Pulsar.start_client/1",
          "Pulsar.start_consumer/4",
          "Pulsar.start_producer/2",
          "Pulsar.stop/1",
          "Pulsar.stop_consumer/2",
          "Pulsar.stop_producer/2",
          "Pulsar.Backoff",
          "Pulsar.Client.Bootstrap",
          "Pulsar.Consumer.Worker",
          "Pulsar.Producer.Worker",
          "Pulsar.Topology",
          "Pulsar.Topology.Root",
          "Pulsar.Topology.Controller",
          "Pulsar.Topology.Group",
          "Pulsar.Topology.Resolver",
          "Pulsar.Topology.groups/1",
          "Pulsar.Topology.kind/1",
          "Pulsar.Topology.workers/1"
        ]
      ]
    ]
  end

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.cobertura": :test,
        "coveralls.github": :test,
        "test.unit": :test,
        "test.integration": :test
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:crypto, :logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:crc32cer, "~> 1.1"},
      {:castore, "~> 1.0"},
      {:ezstd, "~> 1.2"},
      {:jason, "~> 1.4"},
      {:nimble_lz4, "~> 1.1"},
      {:nimble_options, "~> 1.1"},
      {:oauth2, "~> 2.1"},
      {:protobuf, "~> 0.17.0"},
      {:snappyer, "~> 1.2"},
      {:telemetry, "~> 1.0"},
      {:uniq, "~> 0.6.2"},
      {:benchee, "~> 1.5", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.0", only: :dev, runtime: false},
      {:excoveralls, "~> 0.18.5", only: :test},
      {:junit_formatter, "~> 3.4", only: :test},
      {:styler, "~> 1.2", only: [:dev, :test], runtime: false},
      {:telemetry_test, "~> 0.1.0", only: :test}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp aliases do
    [
      bench: ["run bench/decode_stream.exs"],
      test: ["test"],
      "test.unit": ["test --exclude integration"],
      "test.integration": ["test --only integration"]
    ]
  end

  defp description do
    "An Elixir client for Apache Pulsar."
  end

  defp package do
    [
      name: "pulsar_elixir",
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md),
      licenses: ~w(MIT),
      links: %{
        "GitHub" => "https://github.com/efcasado/pulsar-elixir",
        "Changelog" => "https://github.com/efcasado/pulsar-elixir/blob/main/CHANGELOG.md"
      }
    ]
  end
end
