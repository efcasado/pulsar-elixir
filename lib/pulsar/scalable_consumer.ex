defmodule Pulsar.ScalableConsumer do
  @moduledoc """
  A supervisor that fans out one consumer group per segment of a scalable topic
  (PIP-460/466/468).

  This is the scalable-topic analogue of `Pulsar.PartitionedConsumer`: where the
  partitioned variant runs one `Pulsar.ConsumerGroup` per partition, this runs
  one per segment. The segment set is driven by a single `Pulsar.ScalableTopology`
  watcher started in reconcile mode — it adds a consumer group child to this
  supervisor for each active segment as the topology changes (analogous to how
  `Pulsar.PartitionDiscovery` grows a `PartitionedConsumer`).

  Splits that add segments are picked up automatically, and segments that leave
  the DAG (GC'd once drained) have their consumer groups torn down. Both legacy
  segments (a regular topic resolved through the scalable lookup) and native
  segments (addressed by a computed `segment://…` URI) are consumed via the
  classic subscribe path.
  """

  use Supervisor

  alias Pulsar.ScalableTopology

  require Logger

  @default_client :default

  @doc """
  Starts a scalable consumer supervisor.

  Mirrors `Pulsar.PartitionedConsumer.start_link/7`, but the segment set is
  driven by the topic's DAG rather than a partition count. Blocks until the
  first topology update has been applied, so the initial segment consumers exist
  when this returns.
  """
  def start_link(name, topic, subscription_name, subscription_type, callback_module, opts \\ []) do
    client = Keyword.get(opts, :client, @default_client)
    consumer_registry = Pulsar.Client.consumer_registry(client)

    case Supervisor.start_link(
           __MODULE__,
           {name, topic, subscription_name, subscription_type, callback_module, opts},
           name: {:via, Registry, {consumer_registry, name}}
         ) do
      {:ok, supervisor} = ok ->
        await_initial_segments(supervisor, opts)
        ok

      other ->
        other
    end
  end

  @doc """
  Stops a scalable consumer supervisor and all its child consumer groups.
  """
  def stop(supervisor_pid, reason \\ :normal, timeout \\ :infinity) do
    Supervisor.stop(supervisor_pid, reason, timeout)
  end

  @doc """
  Returns `{segment_id, group_pid}` tuples for the segment consumer groups.
  """
  def get_segment_groups(supervisor_pid) do
    supervisor_pid
    |> Supervisor.which_children()
    |> Enum.filter(fn {_id, _pid, type, _modules} -> type == :supervisor end)
    |> Enum.map(fn {segment_id, group_pid, _type, _modules} -> {segment_id, group_pid} end)
  end

  @doc """
  Returns all consumer processes across every segment group.
  """
  def get_consumers(supervisor_pid) do
    supervisor_pid
    |> get_segment_groups()
    |> Enum.flat_map(fn {_segment_id, group_pid} ->
      Pulsar.ConsumerGroup.get_consumers(group_pid)
    end)
  end

  @impl true
  def init({name, topic, subscription_name, subscription_type, callback_module, opts}) do
    client = Keyword.get(opts, :client, @default_client)

    Logger.info("Starting scalable consumer for #{topic}")

    build_child_spec = fn segment_id, consume_topic ->
      segment_child_spec(
        segment_id,
        name,
        consume_topic,
        subscription_name,
        subscription_type,
        callback_module,
        opts
      )
    end

    topology_spec = %{
      id: ScalableTopology,
      start:
        {ScalableTopology, :start_link, [topic, [client: client, supervisor: self(), build_child_spec: build_child_spec]]},
      restart: :permanent,
      type: :worker
    }

    Supervisor.init([topology_spec], strategy: :one_for_one)
  end

  # Blocks until the watcher applies its first DAG (which reconciles the initial
  # segment children). Tolerates a timeout — children will still appear as
  # updates arrive.
  defp await_initial_segments(supervisor, opts) do
    timeout = Keyword.get(opts, :timeout, 5_000)

    case find_topology(supervisor) do
      nil ->
        :ok

      pid ->
        try do
          ScalableTopology.await_dag(pid, timeout)
        catch
          :exit, _reason -> :ok
        end

        :ok
    end
  end

  defp find_topology(supervisor) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {ScalableTopology, pid, _type, _modules} when is_pid(pid) -> pid
      _ -> nil
    end)
  end

  defp segment_child_spec(segment_id, name, topic, subscription_name, subscription_type, callback_module, opts) do
    group_name = "#{name}-segment-#{segment_id}"

    %{
      id: segment_id,
      start: {
        Pulsar.ConsumerGroup,
        :start_link,
        [group_name, topic, subscription_name, subscription_type, callback_module, opts]
      },
      restart: :permanent,
      type: :supervisor
    }
  end
end
