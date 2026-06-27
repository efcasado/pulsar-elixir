defmodule Pulsar.ScalableAssignment do
  @moduledoc """
  Coordinates a STREAM consumer's segment assignment for a scalable topic
  (PIP-460/466/468).

  This is the Stream-consumer counterpart of `Pulsar.ScalableTopology`. Where the
  queue model watches the full DAG and consumes every segment, a Stream consumer
  *registers* with the controller (`CommandScalableTopicSubscribe`, type STREAM)
  and is assigned a *subset* of segments. The controller pushes a new
  `CommandScalableTopicAssignmentUpdate` whenever the set changes — a peer
  joining/leaving the subscription, or a split/merge.

  Ordering is the broker's job: the controller only hands over a child segment
  once its parent has been drained on the subscription, so the client simply
  reconciles the per-segment consumer groups against whatever it is assigned —
  no client-side parent-before-child sequencing. Each assigned segment arrives as
  a ready-made `segment://…` topic (`ScalableAssignedSegment.segment_topic`).

  Like `ScalableTopology` in reconcile mode, this runs as a worker child of a
  `Pulsar.ScalableStreamConsumer` supervisor and adds/removes segment children on
  it via a `build_child_spec` fun, keeping it agnostic to the child type.
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
    :subscription,
    :consumer_name,
    :consumer_id,
    :consumer_type,
    :supervisor,
    :build_child_spec,
    :broker_pid,
    :broker_monitor,
    :layout_epoch,
    ready: false,
    waiters: []
  ]

  @doc """
  Starts the assignment coordinator.

  Options: `:topic`, `:subscription`, `:supervisor`, `:build_child_spec`
  (required); `:client`, `:consumer_name`, `:consumer_type` (default `:STREAM`).
  """
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc """
  Blocks until the initial assignment has been received and reconciled, so the
  caller observes the segment consumers once it returns.
  """
  def await_ready(coordinator, timeout \\ 5_000), do: GenServer.call(coordinator, :await_ready, timeout)

  @impl true
  def init(opts) do
    consumer_id = System.unique_integer([:positive, :monotonic])

    state = %__MODULE__{
      client: Keyword.get(opts, :client, @default_client),
      topic: Keyword.fetch!(opts, :topic),
      subscription: Keyword.fetch!(opts, :subscription),
      # The controller keys each consumer by name in a metadata path, so it must
      # be non-empty and unique within the subscription.
      consumer_name: Keyword.get(opts, :consumer_name) || "consumer-#{consumer_id}",
      consumer_id: consumer_id,
      consumer_type: Keyword.get(opts, :consumer_type, :STREAM),
      supervisor: Keyword.fetch!(opts, :supervisor),
      build_child_spec: Keyword.fetch!(opts, :build_child_spec)
    }

    Logger.info("Starting scalable #{state.consumer_type} assignment for #{state.topic} (consumer #{state.consumer_id})")
    {:ok, state, {:continue, :subscribe}}
  end

  @impl true
  def handle_continue(:subscribe, state) do
    with {:ok, broker_pid} <- ServiceDiscovery.lookup_topic(state.topic, client: state.client),
         :ok <- Broker.register_scalable_consumer(broker_pid, state.consumer_id, self()),
         {:ok, %Binary.CommandScalableTopicSubscribeResponse{error: nil} = response} <-
           Broker.send_request(broker_pid, %Binary.CommandScalableTopicSubscribe{
             topic: state.topic,
             subscription: state.subscription,
             consumer_name: state.consumer_name,
             consumer_id: state.consumer_id,
             consumer_type: state.consumer_type
           }) do
      broker_monitor = Process.monitor(broker_pid)

      state =
        %{state | broker_pid: broker_pid, broker_monitor: broker_monitor}
        |> apply_assignment(response.assignment)
        |> mark_ready()

      {:noreply, state}
    else
      {:ok, %Binary.CommandScalableTopicSubscribeResponse{error: error, message: message}} ->
        {:stop, {:scalable_subscribe_failed, error, message}, state}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  @impl true
  def handle_call(:await_ready, _from, %{ready: true} = state), do: {:reply, :ok, state}
  def handle_call(:await_ready, from, state), do: {:noreply, %{state | waiters: [from | state.waiters]}}

  @impl true
  def handle_info({:broker_message, %Binary.CommandScalableTopicAssignmentUpdate{} = update}, state) do
    {:noreply, apply_assignment(state, update.assignment)}
  end

  # The broker connection died. Stop so the supervisor restarts us and we
  # re-register and re-subscribe against a fresh connection.
  def handle_info({:DOWN, monitor_ref, :process, _pid, reason}, %{broker_monitor: monitor_ref} = state) do
    Logger.warning("Scalable stream assignment lost broker for #{state.topic}: #{inspect(reason)}")
    {:stop, :broker_disconnected, state}
  end

  def handle_info(message, state) do
    Logger.debug("ScalableAssignment ignoring unexpected message: #{inspect(message)}")
    {:noreply, state}
  end

  ## Internal

  defp mark_ready(state) do
    Enum.each(state.waiters, &GenServer.reply(&1, :ok))
    %{state | ready: true, waiters: []}
  end

  # Accept both arg orders so it composes in the `with` pipeline above.
  defp apply_assignment(%__MODULE__{} = state, assignment), do: apply_assignment(assignment, state)

  defp apply_assignment(nil, state), do: state

  defp apply_assignment(%Binary.ScalableConsumerAssignment{} = assignment, state) do
    if stale?(state.layout_epoch, assignment.layout_epoch) do
      Logger.debug("Ignoring stale assignment epoch #{assignment.layout_epoch} for #{state.topic}")
      state
    else
      Logger.debug(
        "Assignment for #{state.topic}: epoch #{assignment.layout_epoch}, #{length(assignment.segments)} segment(s)"
      )

      reconcile(state, assignment)
      %{state | layout_epoch: assignment.layout_epoch}
    end
  end

  defp stale?(nil, _incoming), do: false
  defp stale?(current, incoming), do: incoming < current

  # Reconcile the running segment children to match the assignment: add children
  # for newly assigned segments, remove children for segments no longer assigned.
  defp reconcile(state, assignment) do
    desired = Map.new(assignment.segments, fn segment -> {segment.segment_id, segment.segment_topic} end)
    existing = existing_segment_ids(state.supervisor)

    desired
    |> Enum.reject(fn {segment_id, _topic} -> MapSet.member?(existing, segment_id) end)
    |> Enum.each(fn {segment_id, segment_topic} -> add_segment(state, segment_id, segment_topic) end)

    existing
    |> Enum.reject(&Map.has_key?(desired, &1))
    |> Enum.each(&remove_segment(state.supervisor, &1, state.topic))
  end

  defp add_segment(state, segment_id, segment_topic) do
    case Supervisor.start_child(state.supervisor, state.build_child_spec.(segment_id, segment_topic)) do
      {:ok, _pid} ->
        Logger.info("Scalable stream: added segment #{segment_id} (#{segment_topic}) for #{state.topic}")

      {:ok, _pid, _info} ->
        :ok

      {:error, {:already_started, _pid}} ->
        :ok

      {:error, reason} ->
        Logger.error("Scalable stream: failed to add segment #{segment_id} for #{state.topic}: #{inspect(reason)}")
    end
  end

  defp remove_segment(supervisor, segment_id, topic) do
    Logger.info("Scalable stream: removing segment #{segment_id} for #{topic}")

    with :ok <- Supervisor.terminate_child(supervisor, segment_id),
         :ok <- Supervisor.delete_child(supervisor, segment_id) do
      :ok
    else
      {:error, reason} ->
        Logger.warning("Scalable stream: failed to remove segment #{segment_id} for #{topic}: #{inspect(reason)}")
    end
  end

  defp existing_segment_ids(supervisor) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.filter(fn {id, _pid, type, _modules} -> type == :supervisor and is_integer(id) end)
    |> MapSet.new(fn {id, _pid, _type, _modules} -> id end)
  end
end
