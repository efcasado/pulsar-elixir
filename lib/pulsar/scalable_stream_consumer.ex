defmodule Pulsar.ScalableStreamConsumer do
  @moduledoc """
  A supervisor that consumes a scalable topic (PIP-460/466/468) as a **Stream**
  consumer: ordered, with the controller assigning each consumer a subset of
  segments.

  Where `Pulsar.ScalableConsumer` (the queue model) watches the full DAG and
  consumes every segment, this registers with the controller via a single
  `Pulsar.ScalableAssignment` coordinator and fans out one `Pulsar.ConsumerGroup`
  per *assigned* segment. The coordinator reconciles those children as the
  controller pushes assignment updates (peer join/leave, split/merge).

  Ordering across split/merge is guaranteed by the broker (it withholds a child
  segment until its parent is drained), so this layer only needs to track its
  assignment. Multiple stream consumers sharing the same subscription split the
  segments between them.
  """

  use Supervisor

  alias Pulsar.ScalableAssignment

  require Logger

  @default_client :default

  @doc """
  Starts a scalable stream consumer supervisor.

  Blocks until the initial assignment has been applied, so the initial segment
  consumers exist when this returns.
  """
  def start_link(name, topic, subscription_name, callback_module, opts \\ []) do
    client = Keyword.get(opts, :client, @default_client)
    consumer_registry = Pulsar.Client.consumer_registry(client)

    case Supervisor.start_link(
           __MODULE__,
           {name, topic, subscription_name, callback_module, opts},
           name: {:via, Registry, {consumer_registry, name}}
         ) do
      {:ok, supervisor} = ok ->
        await_ready(supervisor, opts)
        ok

      other ->
        other
    end
  end

  @doc """
  Stops a scalable stream consumer supervisor and all its segment consumer groups.
  """
  def stop(supervisor_pid, reason \\ :normal, timeout \\ :infinity) do
    Supervisor.stop(supervisor_pid, reason, timeout)
  end

  @doc """
  Returns `{segment_id, group_pid}` tuples for the assigned segment consumer groups.
  """
  def get_segment_groups(supervisor_pid) do
    supervisor_pid
    |> Supervisor.which_children()
    |> Enum.filter(fn {_id, _pid, type, _modules} -> type == :supervisor end)
    |> Enum.map(fn {segment_id, group_pid, _type, _modules} -> {segment_id, group_pid} end)
  end

  @doc """
  Returns all consumer processes across every assigned segment group.
  """
  def get_consumers(supervisor_pid) do
    supervisor_pid
    |> get_segment_groups()
    |> Enum.flat_map(fn {_segment_id, group_pid} ->
      Pulsar.ConsumerGroup.get_consumers(group_pid)
    end)
  end

  @impl true
  def init({name, topic, subscription_name, callback_module, opts}) do
    client = Keyword.get(opts, :client, @default_client)

    Logger.info("Starting scalable stream consumer for #{topic}")

    # Each assigned segment is consumed by exactly one consumer (this one), so a
    # Failover subscription gives clean handover when a segment is reassigned.
    build_child_spec = fn segment_id, segment_topic ->
      group_name = "#{name}-segment-#{segment_id}"

      %{
        id: segment_id,
        start: {
          Pulsar.ConsumerGroup,
          :start_link,
          [group_name, segment_topic, subscription_name, :Failover, callback_module, opts]
        },
        restart: :permanent,
        type: :supervisor
      }
    end

    assignment_spec = %{
      id: ScalableAssignment,
      start:
        {ScalableAssignment, :start_link,
         [
           [
             client: client,
             topic: topic,
             subscription: subscription_name,
             consumer_name: Keyword.get(opts, :consumer_name),
             consumer_type: :STREAM,
             supervisor: self(),
             build_child_spec: build_child_spec
           ]
         ]},
      restart: :permanent,
      type: :worker
    }

    Supervisor.init([assignment_spec], strategy: :one_for_one)
  end

  defp await_ready(supervisor, opts) do
    timeout = Keyword.get(opts, :timeout, 5_000)

    case find_assignment(supervisor) do
      nil ->
        :ok

      pid ->
        try do
          ScalableAssignment.await_ready(pid, timeout)
        catch
          :exit, _reason -> :ok
        end

        :ok
    end
  end

  defp find_assignment(supervisor) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {ScalableAssignment, pid, _type, _modules} when is_pid(pid) -> pid
      _ -> nil
    end)
  end
end
