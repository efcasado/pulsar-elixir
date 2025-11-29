defmodule Pulsar.Config do
  @moduledoc false
  @doc """
  Helper module to simplify working with configuration values.
  """

  @default_cleanup_interval 30_000
  @default_client_version "Pulsar Elixir Client"
  @default_max_backoff 60_000
  @default_ping_interval 60_000
  @default_request_timeout 60_000
  @default_startup_delay_ms 1000
  @default_startup_jitter_ms 1000

  def cleanup_interval do
    Application.get_env(:pulsar, :cleanup_interval, @default_cleanup_interval)
  end

  def client_version do
    Application.get_env(:pulsar, :client_version, @default_client_version)
  end

  def max_backoff do
    Application.get_env(:pulsar, :max_backoff, @default_max_backoff)
  end

  def ping_interval do
    Application.get_env(:pulsar, :ping_interval, @default_ping_interval)
  end

  def protocol_version do
    Application.get_env(:pulsar, :protocol_version, Pulsar.Protocol.latest_version())
  end

  def request_timeout do
    Application.get_env(:pulsar, :request_timeout, @default_request_timeout)
  end

  def startup_delay do
    Application.get_env(:pulsar, :startup_delay_ms, @default_startup_delay_ms)
  end

  def startup_jitter do
    Application.get_env(:pulsar, :startup_jitter_ms, @default_startup_jitter_ms)
  end
end
