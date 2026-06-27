defmodule Pulsar.ScalableConsumer do
  @moduledoc """
  A supervisor that consumes a scalable topic (PIP-460/466/468) by fanning out
  one `Pulsar.ConsumerGroup` per segment.

  This is the scalable-topic analogue of `Pulsar.PartitionedConsumer`. The
  segment set is driven by a single `Pulsar.ScalableWatcher` started in one of
  two modes via `:consumer_type`:

    * `:queue` (default) — consume *every* segment of the DAG (Shared, individual
      acks). The analogue of how `Pulsar.PartitionDiscovery` grows a
      `PartitionedConsumer`, but push-based and bidirectional (segments are also
      removed when GC'd).
    * `:stream` — register with the controller and consume only the *assigned*
      subset of segments (Failover per segment, ordered). The broker guarantees
      per-key ordering across split/merge by withholding child segments until
      their parent has drained.

  In both cases the watcher reconciles the per-segment consumer groups as the
  topology/assignment changes.
  """

  use Supervisor

  alias Pulsar.ScalableWatcher

  require Logger

  @default_client :default

  @doc """
  Starts a scalable consumer supervisor. Blocks until the first segment set has
  been applied, so the initial consumers exist when this returns.

  `opts`:
    * `:consumer_type` - `:queue` (default) or `:stream`
    * `:subscription_type` - per-segment subscription type for `:queue`
      (default `:Shared`); ignored for `:stream` (which uses `:Failover`)
    * other options are forwarded to the segment consumer groups
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
  Stops a scalable consumer supervisor and all its segment consumer groups.
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
  def init({name, topic, subscription_name, callback_module, opts}) do
    client = Keyword.get(opts, :client, @default_client)
    {source, source_opts, segment_subscription_type} = source_config(opts, subscription_name, client)

    Logger.info("Starting scalable #{Keyword.get(opts, :consumer_type, :queue)} consumer for #{topic}")

    build_child_spec = fn segment_id, segment_topic ->
      segment_child_spec(
        segment_id,
        name,
        segment_topic,
        subscription_name,
        segment_subscription_type,
        callback_module,
        opts
      )
    end

    watcher_opts =
      [source: source, topic: topic, supervisor: self(), build_child_spec: build_child_spec] ++ source_opts

    watcher_spec = %{
      id: ScalableWatcher,
      start: {ScalableWatcher, :start_link, [watcher_opts]},
      restart: :permanent,
      type: :worker
    }

    Supervisor.init([watcher_spec], strategy: :one_for_one)
  end

  defp source_config(opts, subscription_name, client) do
    case Keyword.get(opts, :consumer_type, :queue) do
      :stream ->
        {ScalableWatcher.Assignment,
         [client: client, subscription: subscription_name, consumer_name: Keyword.get(opts, :consumer_name)], :Failover}

      :queue ->
        {ScalableWatcher.Topology, [client: client], Keyword.get(opts, :subscription_type, :Shared)}
    end
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

  defp await_ready(supervisor, opts) do
    timeout = Keyword.get(opts, :timeout, 5_000)

    case find_watcher(supervisor) do
      nil ->
        :ok

      pid ->
        try do
          ScalableWatcher.await_ready(pid, timeout)
        catch
          :exit, _reason -> :ok
        end

        :ok
    end
  end

  defp find_watcher(supervisor) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {ScalableWatcher, pid, _type, _modules} when is_pid(pid) -> pid
      _ -> nil
    end)
  end
end
