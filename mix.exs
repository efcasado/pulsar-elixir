defmodule Pulsar.MixProject do
  use Mix.Project

  def project do
    [
      app: :pulsar,
      version: "2.3.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      dialyzer: [
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
        plt_add_apps: [:mix, :ex_unit]
      ],
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.cobertura": :test,
        "coveralls.github": :test
      ],
      aliases: aliases(),
      name: "Pulsar",
      description: description(),
      package: package(),
      source_url: "https://github.com/efcasado/pulsar-elixir",
      docs: [
        main: "Pulsar",
        extras: ["README.md"]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Pulsar, []}
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
      {:oauth2, "~> 2.1"},
      {:protobuf, "~> 0.15.0"},
      {:snappyer, "~> 1.2"},
      {:telemetry, "~> 1.0"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.39.1", only: :dev, runtime: false},
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
      test: ["test"]
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
