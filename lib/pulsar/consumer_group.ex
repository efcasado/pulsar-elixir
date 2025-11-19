defmodule Pulsar.ConsumerGroup do
  @moduledoc """
  A supervisor that manages a group of consumer processes for a single topic.

  This module provides a reusable abstraction for creating and managing
  consumer groups, whether for regular topics or individual partitions
  within a partitioned topic.

  Each consumer group manages multiple consumer processes (configurable via
  `consumer_count`) that all subscribe to the same topic with the same
  subscription configuration.
  """

  use Supervisor

  require Logger

  @default_client :default

  @doc """
  Starts a consumer group supervisor.

  ## Parameters

  - `name` - Unique name for this consumer group
  - `topic` - The topic to subscribe to
  - `subscription_name` - Name of the subscription
  - `subscription_type` - Type of subscription (e.g., :Exclusive, :Shared, :Key_Shared)
  - `callback_module` - Module that implements `Pulsar.Consumer.Callback` behaviour
  - `opts` - Additional options:
    - `:consumer_count` - Number of consumer processes in this group (default: 1)
    - Other options passed to individual consumer processes

  ## Returns

  `{:ok, pid}` - The consumer group supervisor PID
  `{:error, reason}` - Error if the supervisor failed to start
  """
  def start_link(name, topic, subscription_name, subscription_type, callback_module, opts \\ []) do
    client = Keyword.get(opts, :client, @default_client)
    consumer_registry = Pulsar.Client.consumer_registry(client)

    Supervisor.start_link(
      __MODULE__,
      {name, topic, subscription_name, subscription_type, callback_module, opts},
      name: {:via, Registry, {consumer_registry, name}}
    )
  end

  @doc """
  Stops a consumer group supervisor and all its child consumer processes.
  """
  def stop(supervisor_pid, reason \\ :normal, timeout \\ :infinity) do
    Supervisor.stop(supervisor_pid, reason, timeout)
  end

  @doc """
  Gets all consumer process PIDs managed by this consumer group.

  Returns a list of consumer PIDs.
  """
  def get_consumers(supervisor_pid) do
    supervisor_pid
    |> Supervisor.which_children()
    |> Enum.map(fn {_id, child_pid, :worker, _modules} -> child_pid end)
  end

  @impl true
  def init({name, topic, subscription_name, subscription_type, callback_module, opts}) do
    consumer_count = Keyword.get(opts, :consumer_count, 1)

    Logger.info("Starting consumer group #{name} for topic #{topic} with #{consumer_count} consumers")

    # Create child specs for each consumer in the group
    children =
      create_consumer_children(
        name,
        topic,
        subscription_name,
        subscription_type,
        callback_module,
        opts,
        consumer_count
      )

    supervisor_opts = [
      strategy: :one_for_one,
      max_restarts: Keyword.get(opts, :max_restarts, 10)
    ]

    Supervisor.init(children, supervisor_opts)
  end

  # Private functions

  defp create_consumer_children(
         group_name,
         topic,
         subscription_name,
         subscription_type,
         callback_module,
         opts,
         consumer_count
       ) do
    for i <- 1..consumer_count do
      consumer_id = "#{group_name}-consumer-#{i}"

      %{
        id: consumer_id,
        start: {
          Pulsar.Consumer,
          :start_link,
          [topic, subscription_name, subscription_type, callback_module, opts]
        },
        restart: :transient,
        type: :worker
      }
    end
  end
end
