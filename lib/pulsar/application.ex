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

        # Registry for consumer group processes
        {Registry, keys: :unique, name: Pulsar.ConsumerGroupRegistry},

        # DynamicSupervisor for broker processes
        {DynamicSupervisor, strategy: :one_for_one, name: Pulsar.BrokerSupervisor},

        # DynamicSupervisor for consumer group processes (started via API)
        {DynamicSupervisor, strategy: :one_for_one, name: Pulsar.ConsumerSupervisor},

        # Bootstrap broker (if configured)
        bootstrap_broker_spec(bootstrap_host),

        # Consumer group processes based on configuration
        consumer_group_supervisor_spec(consumers)
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

  defp consumer_group_supervisor_spec([]), do: nil

  defp consumer_group_supervisor_spec(consumers) do
    consumer_group_children =
      consumers
      |> Enum.map(fn {consumer_name, consumer_opts} ->
        topic = Keyword.fetch!(consumer_opts, :topic)
        subscription_name = Keyword.fetch!(consumer_opts, :subscription_name)
        subscription_type = Keyword.fetch!(consumer_opts, :subscription_type)
        callback_module = Keyword.fetch!(consumer_opts, :callback)
        consumer_count = Keyword.get(consumer_opts, :consumer_count, 1)
        init_args = Keyword.get(consumer_opts, :init_args, [])

        consumer_group_opts = [
          topic: topic,
          subscription_name: subscription_name,
          subscription_type: subscription_type,
          callback_module: callback_module,
          consumer_count: consumer_count,
          init_args: init_args
        ]

        %{
          id: {:consumer_group, consumer_name},
          start: {Pulsar.ConsumerGroup, :start_link, [consumer_group_opts]},
          restart: :permanent,
          type: :supervisor
        }
      end)

    %{
      id: :consumer_group_supervisor,
      start: {Supervisor, :start_link, [consumer_group_children, [strategy: :one_for_one]]},
      restart: :permanent,
      type: :supervisor
    }
  end

  defp broker_key(broker_url) do
    %URI{host: host, port: port} = URI.parse(broker_url)
    "#{host}:#{port}"
  end
end
