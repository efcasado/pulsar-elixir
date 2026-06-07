defmodule Pulsar.Application do
  @moduledoc false

  use Application

  @default_client :default
  @app_supervisor Pulsar.Supervisor

  @impl true
  def start(_type, opts) do
    consumers =
      Keyword.get(opts, :consumers, Application.get_env(:pulsar, :consumers, []))

    producers =
      Keyword.get(opts, :producers, Application.get_env(:pulsar, :producers, []))

    # Get client configurations - support both :clients (multi-client) and :host (single client)
    clients_config =
      case Keyword.get(opts, :clients, Application.get_env(:pulsar, :clients)) do
        nil ->
          # Fallback to legacy single-client mode with :host
          bootstrap_host = Keyword.get(opts, :host, Application.get_env(:pulsar, :host))
          if bootstrap_host, do: [{@default_client, [host: bootstrap_host] ++ opts}], else: []

        clients when is_list(clients) ->
          clients
      end

    sup_opts = [strategy: :one_for_one, name: @app_supervisor]
    {:ok, pid} = Supervisor.start_link([], sup_opts)

    # Start clients (brokers are now started within each client)
    Enum.each(clients_config, fn {client_name, client_opts} ->
      opts = Keyword.put(client_opts, :name, client_name)
      Pulsar.start_client(opts)
    end)

    # Start consumers
    Enum.each(consumers, fn {consumer_name, consumer_opts} ->
      topic = Keyword.fetch!(consumer_opts, :topic)
      subscription_name = Keyword.fetch!(consumer_opts, :subscription_name)
      callback_module = Keyword.fetch!(consumer_opts, :callback_module)
      client = Keyword.get(consumer_opts, :client, @default_client)

      opts =
        consumer_opts
        |> Keyword.delete(:topic)
        |> Keyword.delete(:subscription_name)
        |> Keyword.delete(:callback_module)
        |> Keyword.put_new(:name, Atom.to_string(consumer_name))
        |> Keyword.put(:client, client)

      Pulsar.start_consumer(topic, subscription_name, callback_module, opts)
    end)

    # Start producers
    Enum.each(producers, fn {producer_name, producer_opts} ->
      topic = Keyword.fetch!(producer_opts, :topic)
      client = Keyword.get(producer_opts, :client, @default_client)

      opts =
        producer_opts
        |> Keyword.delete(:topic)
        |> Keyword.put(:name, producer_name)
        |> Keyword.put(:client, client)

      Pulsar.start_producer(topic, opts)
    end)

    {:ok, pid}
  end

  @impl true
  def stop(pid) when is_pid(pid) do
    Supervisor.stop(pid, :normal)
  end
end
