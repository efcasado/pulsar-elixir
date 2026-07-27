defmodule Pulsar.Client.Bootstrap do
  @moduledoc false

  use GenServer

  alias Pulsar.Client

  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    client = Keyword.fetch!(opts, :name)

    case Client.start_broker(Keyword.fetch!(opts, :host), client: client) do
      {:ok, _broker} ->
        start_all(Pulsar.Producer, Keyword.fetch!(opts, :producers), client)
        start_all(Pulsar.Consumer, Keyword.fetch!(opts, :consumers), client)
        :ignore

      {:error, reason} ->
        {:stop, {:broker_startup_failed, reason}}
    end
  end

  defp start_all(module, entries, client) do
    Enum.each(entries, fn opts ->
      case module.start(opts) do
        {:ok, _pid} ->
          :ok

        {:ok, _pid, _info} ->
          :ok

        {:error, {:already_started, _pid}} ->
          :ok

        {:error, reason} ->
          Logger.error(
            "Pulsar client #{inspect(client)} could not start #{inspect(module)} " <>
              "for #{inspect(Keyword.get(opts, :topic))}: #{inspect(reason)}"
          )
      end
    end)
  end
end
