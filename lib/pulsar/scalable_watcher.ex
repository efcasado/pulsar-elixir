defmodule Pulsar.ScalableWatcher do
  @moduledoc """
  Watches a scalable topic (PIP-460/466/468) and reconciles one consumer group
  per segment into a parent supervisor as the topology changes.

  This is the single coordinator behind both scalable consumer models. The
  difference between them lives entirely in a pluggable **source**
  (`Pulsar.ScalableWatcher.Source`):

    * `Pulsar.ScalableWatcher.Topology` (queue) — opens a DAG watch session and
      consumes *every* segment.
    * `Pulsar.ScalableWatcher.Assignment` (stream) — registers with the
      controller and consumes the *assigned subset*.

  A source turns the broker's pushed messages into a desired segment set
  (`%{segment_id => topic}` + an epoch); everything else — broker connection
  monitoring, the `await_ready/2` handshake, stale-epoch filtering, and the
  add/remove reconcile against the supervisor — is handled here and is identical
  for both models.

  Runs as a worker child of the consumer's supervisor and adds/removes segment
  children on it via the `build_child_spec` fun, so it stays agnostic to whether
  the children are consumers or (eventually) producers.
  """

  use GenServer

  require Logger

  defmodule Source do
    @moduledoc """
    Behaviour for a scalable watcher's segment source.

    Implementations encapsulate the broker protocol (watch session vs. controller
    registration) and how a pushed message maps to the desired segment set.
    """

    @type state :: term()
    @type desired :: %{integer() => String.t()}

    @doc "Builds the source state from the watcher options."
    @callback init(opts :: keyword()) :: state

    @doc """
    Establishes the broker channel for `owner` (resolve broker, register, send
    the opening command). Returns the broker pid to monitor, the updated source
    state, and an optional initial desired set (`nil` when it arrives later as a
    pushed message).
    """
    @callback open(state, owner :: pid()) ::
                {:ok, broker_pid :: pid(), state, nil | {epoch :: integer(), desired}}
                | {:error, term()}

    @doc """
    Maps a pushed `{:broker_message, _}` payload to `{:update, epoch, desired,
    state}`, or `:ignore` for messages this source doesn't care about.
    """
    @callback handle_update(message :: term(), state) :: {:update, integer(), desired, state} | :ignore
  end

  defstruct [
    :source,
    :source_state,
    :topic,
    :supervisor,
    :build_child_spec,
    :broker_pid,
    :broker_monitor,
    :epoch,
    ready: false,
    waiters: []
  ]

  @doc """
  Starts a watcher.

  Options: `:source` (a `Source` module), `:topic`, `:supervisor`,
  `:build_child_spec` (required); plus any options the source's `init/1` needs.
  """
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc """
  Blocks until the first desired segment set has been applied (reconciled), so
  the caller observes the segment consumers once it returns.
  """
  def await_ready(watcher, timeout \\ 5_000), do: GenServer.call(watcher, :await_ready, timeout)

  @impl true
  def init(opts) do
    source = Keyword.fetch!(opts, :source)

    state = %__MODULE__{
      source: source,
      source_state: source.init(opts),
      topic: Keyword.fetch!(opts, :topic),
      supervisor: Keyword.fetch!(opts, :supervisor),
      build_child_spec: Keyword.fetch!(opts, :build_child_spec)
    }

    Logger.info("Starting scalable watcher (#{inspect(source)}) for #{state.topic}")
    {:ok, state, {:continue, :open}}
  end

  @impl true
  def handle_continue(:open, state) do
    case state.source.open(state.source_state, self()) do
      {:ok, broker_pid, source_state, initial} ->
        broker_monitor = Process.monitor(broker_pid)

        state = %{
          state
          | broker_pid: broker_pid,
            broker_monitor: broker_monitor,
            source_state: source_state
        }

        {:noreply, apply_initial(initial, state)}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  @impl true
  def handle_call(:await_ready, _from, %{ready: true} = state), do: {:reply, :ok, state}
  def handle_call(:await_ready, from, state), do: {:noreply, %{state | waiters: [from | state.waiters]}}

  @impl true
  def handle_info({:broker_message, message}, state) do
    case state.source.handle_update(message, state.source_state) do
      {:update, epoch, desired, source_state} ->
        {:noreply, apply_desired(%{state | source_state: source_state}, epoch, desired)}

      :ignore ->
        {:noreply, state}
    end
  end

  # The broker connection died. Stop so the supervisor restarts us and we
  # re-establish the source against a fresh connection.
  def handle_info({:DOWN, monitor_ref, :process, _pid, reason}, %{broker_monitor: monitor_ref} = state) do
    Logger.warning("Scalable watcher lost broker for #{state.topic}: #{inspect(reason)}")
    {:stop, :broker_disconnected, state}
  end

  def handle_info(message, state) do
    Logger.debug("ScalableWatcher ignoring unexpected message: #{inspect(message)}")
    {:noreply, state}
  end

  ## Internal

  defp apply_initial(nil, state), do: state
  defp apply_initial({epoch, desired}, state), do: apply_desired(state, epoch, desired)

  defp apply_desired(state, epoch, desired) do
    if stale?(state.epoch, epoch) do
      Logger.debug("Ignoring stale scalable epoch #{epoch} for #{state.topic}")
      state
    else
      Logger.debug("Scalable update for #{state.topic}: epoch #{epoch}, #{map_size(desired)} segment(s)")

      # Reconcile before replying so a caller blocked in await_ready/2 observes
      # the segment children once it returns.
      reconcile(state, desired)
      mark_ready(%{state | epoch: epoch})
    end
  end

  defp stale?(nil, _incoming), do: false
  defp stale?(current, incoming), do: incoming < current

  defp mark_ready(state) do
    Enum.each(state.waiters, &GenServer.reply(&1, :ok))
    %{state | ready: true, waiters: []}
  end

  # Add a child for each desired segment we don't have; remove a child for each
  # running segment no longer desired.
  defp reconcile(state, desired) do
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
        Logger.info("Scalable watcher: added segment #{segment_id} (#{segment_topic}) for #{state.topic}")

      {:ok, _pid, _info} ->
        :ok

      {:error, {:already_started, _pid}} ->
        :ok

      {:error, reason} ->
        Logger.error("Scalable watcher: failed to add segment #{segment_id} for #{state.topic}: #{inspect(reason)}")
    end
  end

  defp remove_segment(supervisor, segment_id, topic) do
    Logger.info("Scalable watcher: removing segment #{segment_id} for #{topic}")

    with :ok <- Supervisor.terminate_child(supervisor, segment_id),
         :ok <- Supervisor.delete_child(supervisor, segment_id) do
      :ok
    else
      {:error, reason} ->
        Logger.warning("Scalable watcher: failed to remove segment #{segment_id} for #{topic}: #{inspect(reason)}")
    end
  end

  defp existing_segment_ids(supervisor) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.filter(fn {id, _pid, type, _modules} -> type == :supervisor and is_integer(id) end)
    |> MapSet.new(fn {id, _pid, _type, _modules} -> id end)
  end
end
