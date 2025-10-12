defmodule Pulsar.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    consumers = Application.get_env(:pulsar, :consumers, [])
    bootstrap_host = Application.get_env(:pulsar, :host)

    broker_opts = [
      socket_opts: Application.get_env(:pulsar, :socket_opts, []),
      conn_timeout: Application.get_env(:pulsar, :conn_timeout, 5_000),
      auth: Application.get_env(:pulsar, :auth, [])
    ]

    children =
      [
        {Registry, keys: :unique, name: Pulsar.BrokerRegistry},
        {Registry, keys: :unique, name: Pulsar.ConsumerRegistry},
        {DynamicSupervisor, strategy: :one_for_one, name: Pulsar.BrokerSupervisor},
        {DynamicSupervisor, strategy: :one_for_one, name: Pulsar.ConsumerSupervisor}
      ]

    opts = [strategy: :one_for_one, name: Pulsar.Supervisor]
    {:ok, pid} = Supervisor.start_link(children, opts)

    # a bootstrap host is required
    {:ok, _} = Pulsar.start_broker(bootstrap_host, broker_opts)

    consumers
    |> Enum.each(fn {_, consumer_opts} -> {:ok, _} = Pulsar.start_consumer(consumer_opts) end)

    {:ok, pid}
  end

  # Helper functions for testing and programmatic control

  @doc """
  Start the Pulsar application with optional configuration.

  This is useful for testing where you want to start the application
  with specific configuration without modifying the global application environment.

  ## Examples

      # Start with default configuration from Application environment
      {:ok, pid} = Pulsar.Application.start()
      
      # Start with custom configuration
      {:ok, pid} = Pulsar.Application.start(
        host: "pulsar://localhost:6650",
        consumers: [
          {:my_consumer, [
            topic: "my-topic",
            subscription_name: "my-subscription",
            subscription_type: :Shared,
            callback: MyConsumerCallback
          ]}
        ]
      )
      
      # Later, stop it
      :ok = Pulsar.Application.stop(pid)
  """
  def start(config \\ []) do
    old_config = Application.get_all_env(:pulsar)

    :ok = Application.put_all_env(pulsar: config)

    resp = start(:normal, [])

    :ok = Application.put_all_env(pulsar: old_config)

    resp
  end

  @doc """
  Stop the Pulsar application supervisor.

  ## Examples

      {:ok, pid} = Pulsar.Application.start()
      :ok = Pulsar.Application.stop(pid)
  """
  @impl true
  def stop(pid) when is_pid(pid) do
    Supervisor.stop(pid, :normal)
  end
end
