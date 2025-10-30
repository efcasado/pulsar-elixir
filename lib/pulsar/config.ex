defmodule Pulsar.Config do
  @moduledoc false
  @doc """
  Helper module to simplify working with configuration values.
  """

  @client_version "Pulsar Elixir Client"
  @max_backoff 60_000
  @ping_interval 60_000
  @cleanup_interval 30_000
  @request_timeout 60_000

  def client_version do
    Application.get_env(:pulsar, :client_version, @client_version)
  end

  def max_backoff do
    Application.get_env(:pulsar, :max_backoff, @max_backoff)
  end

  def ping_interval do
    Application.get_env(:pulsar, :ping_interval, @ping_interval)
  end

  def cleanup_interval do
    Application.get_env(:pulsar, :cleanup_interval, @cleanup_interval)
  end

  def request_timeout do
    Application.get_env(:pulsar, :request_timeout, @request_timeout)
  end

  def protocol_version do
    Application.get_env(:pulsar, :protocol_version, Pulsar.Protocol.latest_version())
  end
end
