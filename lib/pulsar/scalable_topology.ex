defmodule Pulsar.ScalableTopology do
  @moduledoc """
  Watches the segment topology (DAG) of a single scalable topic
  (PIP-460/466/468).

  This is the scalable-topic analogue of `Pulsar.PartitionDiscovery`: rather
  than polling a partition count, it opens a server-pushed watch session and
  keeps the latest `ScalableTopicDAG` for the topic.

  It runs in one of two modes:

    * **read-only** (no `:supervisor`/`:build_child_spec`) — just tracks which
      segments exist and their state (`:ACTIVE` / `:SEALED`); used for the
      one-shot `fetch/2` and introspection.
    * **reconcile** (given a parent `:supervisor` and a `:build_child_spec`
      fun) — like a push-based `PartitionDiscovery`, it reconciles a child per
      segment into the parent supervisor as the topology changes: a child is
      started for every segment in the DAG (active *and* sealed — matching the
      queue-consumer model, which drains sealed segments) and torn down once a
      segment leaves the DAG (the controller GC's it after it has drained). The
      fun keeps it agnostic to whether the children are consumers or producers.

  The watch is a session keyed by a client-assigned `session_id`. We register
  the session with the broker (so pushed `CommandScalableTopicUpdate`s are
  routed here), then send a `CommandScalableTopicLookup`. The broker replies
  with an initial update and keeps pushing further updates on split/merge. On
  termination we send `CommandScalableTopicClose`.

  A regular (non-scalable) topic resolves to a synthetic single-segment layout
  whose segment carries `legacy_topic_name`, so this works against ordinary
  topics too.
  """

  use GenServer

  alias Pulsar.Broker
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.ServiceDiscovery

  require Logger

  @default_client :default

  defstruct [
    :client,
    :topic,
    :session_id,
    :broker_pid,
    :broker_monitor,
    :resolved_topic_name,
    :dag,
    # Reconcile mode config: %{supervisor: pid, build_child_spec: (segment_id,
    # consume_topic -> child_spec)}, or nil for read-only.
    :reconcile,
    # `from`s parked in await_dag/2 until the first DAG arrives.
    waiters: []
  ]

  ## Public API

  @doc """
  Starts a topology watcher for `topic`.

  Options:
    * `:client` - client name (default: `:default`)
    * `:supervisor` - parent supervisor to reconcile segment children into
    * `:build_child_spec` - `(segment_id, consume_topic -> child_spec)` fun used
      to build a child for each active segment; reconcile mode is enabled only
      when both `:supervisor` and `:build_child_spec` are given
    * any other option is forwarded to `GenServer.start_link/3` (e.g. `:name`)
  """
  def start_link(topic, opts \\ []) do
    {client, opts} = Keyword.pop(opts, :client, @default_client)
    {supervisor, opts} = Keyword.pop(opts, :supervisor)
    {build_child_spec, genserver_opts} = Keyword.pop(opts, :build_child_spec)
    reconcile = reconcile_config(supervisor, build_child_spec)
    GenServer.start_link(__MODULE__, {topic, client, reconcile}, genserver_opts)
  end

  @doc "Returns the latest `ScalableTopicDAG`, or `nil` before the first update."
  def dag(watcher), do: GenServer.call(watcher, :dag)

  @doc "Returns all segments from the latest DAG (empty list before the first update)."
  def segments(watcher), do: GenServer.call(watcher, :segments)

  @doc "Returns only the `:ACTIVE` segments from the latest DAG."
  def active_segments(watcher) do
    watcher |> segments() |> Enum.filter(&(&1.state == :ACTIVE))
  end

  @doc "Returns the broker-resolved canonical topic name, or `nil` before the first update."
  def resolved_topic_name(watcher), do: GenServer.call(watcher, :resolved_topic_name)

  @doc """
  Blocks until the watcher has received its first DAG, returning `{:ok, dag}`.

  Returns immediately if a DAG is already present. Exits (via `GenServer.call`)
  if no DAG arrives within `timeout`.
  """
  def await_dag(watcher, timeout \\ 5_000), do: GenServer.call(watcher, :await_dag, timeout)

  @doc """
  One-shot helper: starts a temporary watcher, waits for the first DAG, stops the
  watcher, and returns `{:ok, dag}` or `{:error, reason}`.

  Started unlinked so a failed topic lookup doesn't crash the caller. Used to
  resolve the initial segment set before fanning out consumers.
  """
  def fetch(topic, opts \\ []) do
    {timeout, start_opts} = Keyword.pop(opts, :timeout, 5_000)
    client = Keyword.get(start_opts, :client, @default_client)

    case GenServer.start(__MODULE__, {topic, client, nil}) do
      {:ok, watcher} ->
        try do
          await_dag(watcher, timeout)
        catch
          :exit, reason -> {:error, reason}
        after
          if Process.alive?(watcher), do: GenServer.stop(watcher)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Resolves the consume target (a topic name) for a segment.

  Legacy segments (a regular topic not yet migrated to a scalable topic) carry
  the backing `persistent://` topic in `legacy_topic_name`. Native segments are
  addressed by a computed `segment://<topic>/<hash_start:04x>-<hash_end:04x>-<id>`
  URI, derived from the canonical (`topic://…`) topic name, which classic
  lookup/subscribe accept.
  """
  def consume_target(segment, resolved_topic_name)

  def consume_target(%Binary.SegmentInfoProto{legacy_topic_name: name}, _resolved) when is_binary(name) and name != "" do
    {:ok, name}
  end

  def consume_target(%Binary.SegmentInfoProto{} = segment, resolved_topic_name) when is_binary(resolved_topic_name) do
    {:ok, segment_topic(resolved_topic_name, segment)}
  end

  def consume_target(%Binary.SegmentInfoProto{segment_id: id}, _resolved) do
    {:error, {:unresolved_segment, id}}
  end

  defp segment_topic(resolved_topic_name, segment) do
    base = String.replace_prefix(resolved_topic_name, "topic://", "")
    descriptor = "#{hex4(segment.hash_start)}-#{hex4(segment.hash_end)}-#{segment.segment_id}"
    "segment://#{base}/#{descriptor}"
  end

  defp hex4(n), do: n |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(4, "0")

  ## GenServer callbacks

  @impl true
  def init({topic, client, reconcile}) do
    state = %__MODULE__{
      client: client,
      topic: topic,
      reconcile: reconcile,
      session_id: System.unique_integer([:positive, :monotonic])
    }

    Logger.info("Starting scalable topology watch for #{topic} (session #{state.session_id})")
    {:ok, state, {:continue, :watch}}
  end

  @impl true
  def handle_continue(:watch, state) do
    with {:ok, broker_pid} <- ServiceDiscovery.lookup_topic(state.topic, client: state.client),
         :ok <- Broker.register_scalable_session(broker_pid, state.session_id, self()) do
      # Fire-and-forget: the initial reply and all later updates arrive as
      # routed CommandScalableTopicUpdate messages, not a synchronous response.
      Broker.send_command(broker_pid, %Binary.CommandScalableTopicLookup{
        session_id: state.session_id,
        topic: state.topic
      })

      broker_monitor = Process.monitor(broker_pid)

      {:noreply, %{state | broker_pid: broker_pid, broker_monitor: broker_monitor}}
    else
      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  @impl true
  def handle_call(:dag, _from, state), do: {:reply, state.dag, state}

  def handle_call(:segments, _from, state) do
    segments = if state.dag, do: state.dag.segments, else: []
    {:reply, segments, state}
  end

  def handle_call(:resolved_topic_name, _from, state) do
    {:reply, state.resolved_topic_name, state}
  end

  def handle_call(:await_dag, from, state) do
    case state.dag do
      nil -> {:noreply, %{state | waiters: [from | state.waiters]}}
      dag -> {:reply, {:ok, dag}, state}
    end
  end

  @impl true
  def handle_info({:broker_message, %Binary.CommandScalableTopicUpdate{} = update}, state) do
    {:noreply, apply_update(update, state)}
  end

  # The broker connection died. Stop so a supervisor can restart us and
  # re-establish the watch session against a fresh connection.
  def handle_info({:DOWN, monitor_ref, :process, _pid, reason}, %{broker_monitor: monitor_ref} = state) do
    Logger.warning("Scalable topology watch lost broker for #{state.topic}: #{inspect(reason)}")
    {:stop, :broker_disconnected, state}
  end

  def handle_info(message, state) do
    Logger.debug("ScalableTopology ignoring unexpected message: #{inspect(message)}")
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    if state.broker_pid && Process.alive?(state.broker_pid) do
      Broker.send_command(state.broker_pid, %Binary.CommandScalableTopicClose{
        session_id: state.session_id
      })
    end

    :ok
  end

  ## Internal

  defp apply_update(%Binary.CommandScalableTopicUpdate{error: error} = update, state) when error != nil do
    Logger.warning("Scalable topology watch error for #{state.topic}: #{error} #{update.message}")
    state
  end

  defp apply_update(%Binary.CommandScalableTopicUpdate{dag: nil}, state), do: state

  defp apply_update(%Binary.CommandScalableTopicUpdate{dag: dag} = update, state) do
    if stale?(state.dag, dag) do
      Logger.debug("Ignoring stale topology epoch #{dag.epoch} for #{state.topic}")
      state
    else
      Logger.debug("Topology update for #{state.topic}: epoch #{dag.epoch}, #{length(dag.segments)} segment(s)")

      resolved = update.resolved_topic_name || state.resolved_topic_name

      # Reconcile before replying so a caller blocked in await_dag/2 observes
      # the segment children once it returns.
      if state.reconcile, do: reconcile(state.reconcile, dag, resolved, state.topic)
      Enum.each(state.waiters, &GenServer.reply(&1, {:ok, dag}))

      %{state | dag: dag, resolved_topic_name: resolved, waiters: []}
    end
  end

  defp stale?(nil, _new), do: false
  defp stale?(%{epoch: current}, %{epoch: incoming}), do: incoming < current

  defp reconcile_config(nil, _build_child_spec), do: nil
  defp reconcile_config(_supervisor, nil), do: nil

  defp reconcile_config(supervisor, build_child_spec) do
    %{supervisor: supervisor, build_child_spec: build_child_spec}
  end

  # Reconcile the running segment children to match the DAG: add children for
  # segments we don't have yet, remove children for segments that have left the
  # DAG. Sealed segments stay in the DAG (and so keep their consumer) until the
  # controller GC's them once drained, so removal-on-absence respects drain.
  defp reconcile(%{supervisor: supervisor} = config, dag, resolved_topic_name, topic) do
    desired = desired_segments(dag, resolved_topic_name, topic)
    existing = existing_segment_ids(supervisor)

    desired
    |> Enum.reject(fn {segment_id, _consume_topic} -> MapSet.member?(existing, segment_id) end)
    |> Enum.each(fn {segment_id, consume_topic} -> add_segment(config, segment_id, consume_topic, topic) end)

    existing
    |> Enum.reject(&Map.has_key?(desired, &1))
    |> Enum.each(&remove_segment(supervisor, &1, topic))
  end

  # %{segment_id => consume_topic} for every consumable segment in the DAG.
  defp desired_segments(dag, resolved_topic_name, topic) do
    Enum.reduce(dag.segments, %{}, fn segment, acc ->
      case consume_target(segment, resolved_topic_name) do
        {:ok, consume_topic} ->
          Map.put(acc, segment.segment_id, consume_topic)

        {:error, reason} ->
          Logger.warning("Scalable topology: skipping segment #{segment.segment_id} for #{topic}: #{inspect(reason)}")
          acc
      end
    end)
  end

  defp add_segment(%{supervisor: supervisor, build_child_spec: build_child_spec}, segment_id, consume_topic, topic) do
    spec = build_child_spec.(segment_id, consume_topic)

    case Supervisor.start_child(supervisor, spec) do
      {:ok, _pid} ->
        Logger.info("Scalable topology: added segment #{segment_id} (#{consume_topic}) for #{topic}")

      {:ok, _pid, _info} ->
        :ok

      {:error, {:already_started, _pid}} ->
        :ok

      {:error, reason} ->
        Logger.error("Scalable topology: failed to add segment #{segment_id} for #{topic}: #{inspect(reason)}")
    end
  end

  defp remove_segment(supervisor, segment_id, topic) do
    Logger.info("Scalable topology: removing segment #{segment_id} for #{topic}")

    with :ok <- Supervisor.terminate_child(supervisor, segment_id),
         :ok <- Supervisor.delete_child(supervisor, segment_id) do
      :ok
    else
      {:error, reason} ->
        Logger.warning("Scalable topology: failed to remove segment #{segment_id} for #{topic}: #{inspect(reason)}")
    end
  end

  defp existing_segment_ids(supervisor) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.filter(fn {id, _pid, type, _modules} -> type == :supervisor and is_integer(id) end)
    |> MapSet.new(fn {id, _pid, _type, _modules} -> id end)
  end
end
