defmodule Pulsar.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    consumers = Application.get_env(:pulsar, :consumers, [])
    bootstrap_host = Application.get_env(:pulsar, :host)

    children =
      [
        # Registry for broker processes
        {Registry, keys: :unique, name: Pulsar.BrokerRegistry},

        # DynamicSupervisor for broker processes
        {DynamicSupervisor, strategy: :one_for_one, name: Pulsar.BrokerSupervisor},

        # Bootstrap broker (if configured)
        bootstrap_broker_spec(bootstrap_host),

        # Consumer processes based on configuration
        consumer_supervisor_spec(consumers)
      ]
      |> List.flatten()
      |> Enum.reject(&is_nil/1)

    opts = [strategy: :one_for_one, name: Pulsar.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp bootstrap_broker_spec(nil), do: nil

  defp bootstrap_broker_spec(bootstrap_host) do
    broker_opts = [
      name: {:via, Registry, {Pulsar.BrokerRegistry, broker_key(bootstrap_host)}},
      socket_opts: Application.get_env(:pulsar, :socket_opts, []),
      conn_timeout: Application.get_env(:pulsar, :conn_timeout, 5_000),
      auth: Application.get_env(:pulsar, :auth, [])
    ]

    %{
      id: :bootstrap_broker,
      start: {Pulsar.Broker, :start_link, [bootstrap_host, broker_opts]},
      restart: :permanent,
      type: :worker
    }
  end

  defp consumer_supervisor_spec([]), do: nil

  defp consumer_supervisor_spec(consumers) do
    consumer_children =
      consumers
      |> Enum.map(fn {consumer_name, consumer_opts} ->
        topic = Keyword.fetch!(consumer_opts, :topic)
        subscription_name = Keyword.fetch!(consumer_opts, :subscription_name)
        subscription_type = Keyword.fetch!(consumer_opts, :subscription_type)
        callback_module = Keyword.fetch!(consumer_opts, :callback)

        %{
          id: {:consumer, consumer_name},
          start:
            {Pulsar.Consumer, :start_link,
             [
               topic,
               subscription_name,
               subscription_type,
               callback_module
             ]},
          restart: :permanent,
          type: :worker
        }
      end)

    %{
      id: :consumer_supervisor,
      start: {Supervisor, :start_link, [consumer_children, [strategy: :one_for_one]]},
      restart: :permanent,
      type: :supervisor
    }
  end

  defp broker_key(broker_url) do
    %URI{host: host, port: port} = URI.parse(broker_url)
    "#{host}:#{port}"
  end
end
