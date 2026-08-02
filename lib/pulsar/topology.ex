defmodule Pulsar.Topology do
  @moduledoc false

  # Stable root for one logical consumer or producer; Discovery reconciles its internal groups.

  use Supervisor

  alias Pulsar.Topic
  alias Pulsar.Topology.Discovery
  alias Pulsar.Topology.Group

  require Logger

  # A broker losing its connection exits every worker registered with it at once, so a group of
  # N workers sees N restarts in the same instant. OTP's default of 3 in 5 seconds is spent by
  # any group of four or more on a single disconnect, and since a resource is only brought back
  # from an abnormal exit, spending it means the resource is gone for good. Sized to absorb
  # several full reconnects of a large group rather than to catch a fast crash loop, which the
  # workers' own bounded retry handles.
  @max_restarts 100
  @max_seconds 60

  @doc false
  @spec restart_intensity() :: keyword()
  def restart_intensity, do: [max_restarts: @max_restarts, max_seconds: @max_seconds]

  @spec start_link(module(), atom(), atom(), keyword()) :: Supervisor.on_start()
  def start_link(worker, registry, count_key, opts) do
    start_link(worker, registry, count_key, opts, [])
  end

  # The fifth argument is an internal seam for exercising asynchronous discovery without a
  # broker. Consumer and Producer deliberately expose only start_link/1.
  @doc false
  @spec start_link(module(), atom(), atom(), keyword(), keyword()) :: Supervisor.on_start()
  def start_link(worker, registry, count_key, opts, controller_opts) do
    name = Keyword.fetch!(opts, :name)
    config = %{worker: worker, count_key: count_key, opts: opts}

    Supervisor.start_link(__MODULE__, {config, controller_opts}, name: {:via, Registry, {registry, name}})
  end

  @typedoc false
  @type status :: :initializing | {:ready, :non_partitioned | {:partitioned, pos_integer()}}

  @doc false
  @spec status(pid()) :: status()
  def status(root) do
    case controller(root) do
      {:ok, controller} -> call_controller(controller, :status, :initializing)
      _not_running -> :initializing
    end
  end

  @doc false
  @spec initialized?(pid()) :: boolean()
  def initialized?(root) do
    case kind(root) do
      :topology -> match?({:ready, _topology}, status(root))
      _group_or_worker -> true
    end
  end

  defp controller(root) do
    root
    |> Supervisor.which_children()
    |> Enum.find_value({:error, :not_found}, fn
      {Discovery, pid, :worker, [Discovery]} when is_pid(pid) -> {:ok, pid}
      _child -> false
    end)
  catch
    :exit, _reason -> {:error, :not_found}
  end

  defp call_controller(controller, request, fallback) do
    GenServer.call(controller, request)
  catch
    :exit, _reason -> fallback
  end

  # Discovery is also an OTP :worker child, so traversal explicitly allows only resource workers.
  @worker_modules [Pulsar.Consumer.Worker, Pulsar.Producer.Worker]

  @doc """
  Returns the worker processes under `root`, across every partition it has.

  Only workers that are currently running are returned.
  """
  @spec workers(pid()) :: [pid()]
  def workers(root) do
    root
    |> Supervisor.which_children()
    |> Enum.flat_map(fn
      {_id, pid, :worker, [module]} when module in @worker_modules ->
        if is_pid(pid), do: [pid], else: []

      {_id, pid, :supervisor, _modules} when is_pid(pid) ->
        workers(pid)

      _child ->
        []
    end)
  catch
    :exit, _reason -> []
  end

  @doc """
  Returns `{index, pid}` for each group under `root`.

  A non-partitioned topology answers its one internal group at index zero. A `Pulsar.Topology.Group`
  passed directly is treated the same way and answers itself, so callers routing over either
  shape need no special case.

  Partition indexes come from the integer child ids rather than a group's position in the list.

  A partition between lives is reported as `:restarting` or `:undefined` instead of a pid,
  which is a distinct answer from having no such partition at all.
  """
  @spec groups(pid()) :: [{non_neg_integer(), pid() | :restarting | :undefined}]
  def groups(root) do
    case kind(root) do
      :group -> [{0, root}]
      :topology -> topology_groups(root)
    end
  end

  defp topology_groups(root) do
    root
    |> Supervisor.which_children()
    |> Enum.flat_map(fn
      {{:topic, :non_partitioned}, pid, :supervisor, _modules} -> [{0, pid}]
      {{:partition, index}, pid, :supervisor, _modules} -> [{index, pid}]
      _child -> []
    end)
  end

  @doc """
  Which level of a topology `pid` is: the stable topology root, one of its groups, or a worker.

  Uses `:proc_lib.initial_call/1` rather than traversal, so supervisors are never sent calls
  intended for workers.
  """
  @spec kind(pid()) :: :topology | :group | :worker
  def kind(pid) do
    case :proc_lib.initial_call(pid) do
      {:supervisor, __MODULE__, _args} -> :topology
      {:supervisor, Group, _args} -> :group
      _worker -> :worker
    end
  end

  @doc """
  Removes `root` from the supervisor that owns it.

  Used without a known owner for resources started directly with `start_link/1` and as the
  fallback when the expected client supervisor does not own `root`.
  """
  @spec remove(pid()) :: :ok
  def remove(root) do
    case terminate_child(owning_supervisor(root), root) do
      :ok -> :ok
      {:error, :not_found} -> stop_directly(root)
    end
  end

  @doc false
  @spec remove(pid(), GenServer.server()) :: :ok
  def remove(root, supervisor) do
    case terminate_child(supervisor, root) do
      :ok -> :ok
      {:error, :not_found} -> remove(root)
    end
  end

  # Only a supervisor is asked to terminate a child. Started with `start_link/1` from an
  # ordinary process, the first ancestor is whoever called it — and asking a GenServer to
  # `terminate_child` crashes it on an unmatched call while this reports success.
  defp owning_supervisor(pid) do
    with {:dictionary, dictionary} <- Process.info(pid, :dictionary),
         [ancestor | _rest] <- Keyword.get(dictionary, :"$ancestors", []),
         supervisor when not is_nil(supervisor) <- whereis(ancestor),
         true <- supervisor?(supervisor) do
      supervisor
    else
      _not_a_supervisor -> nil
    end
  end

  defp whereis(name) when is_atom(name), do: Process.whereis(name)
  defp whereis(pid) when is_pid(pid), do: pid

  defp supervisor?(pid) do
    match?({:supervisor, _module, _args}, :proc_lib.initial_call(pid))
  end

  defp terminate_child(nil, _pid), do: {:error, :not_found}

  defp terminate_child(supervisor, pid) do
    DynamicSupervisor.terminate_child(supervisor, pid)
  catch
    :exit, _reason -> {:error, :not_found}
  end

  # A resource that is already gone reads as stopped, which is also what a caller holding a
  # pid replaced by a restart sees.
  defp stop_directly(pid) do
    Supervisor.stop(pid)
  catch
    :exit, _reason -> :ok
  end

  @impl true
  def init({config, controller_opts}) do
    %{worker: worker, opts: opts} = config
    topic = Keyword.fetch!(opts, :topic)

    Logger.info("Starting #{inspect(worker)} topology for topic #{topic}")

    discovery = Discovery.child_spec({self(), config, controller_opts})

    Supervisor.init([discovery], [strategy: :one_for_one] ++ restart_intensity())
  end

  @doc false
  @spec reconcile(pid(), non_neg_integer(), map()) :: {:ok, non_neg_integer()} | {:error, term()}
  def reconcile(root, desired, config) when is_integer(desired) and desired >= 0 do
    children = Supervisor.which_children(root)

    case restart_stopped_groups(root, children) do
      :ok -> reconcile_children(root, desired, config)
      {:error, _reason} = error -> error
    end
  catch
    :exit, reason -> {:error, reason}
  end

  defp reconcile_children(root, desired, config) do
    children = Supervisor.which_children(root)

    topic? = Enum.any?(children, &match?({{:topic, :non_partitioned}, _, :supervisor, _}, &1))

    partitions =
      Enum.flat_map(children, fn
        {{:partition, index}, _pid, :supervisor, _modules} -> [index]
        _child -> []
      end)

    reconcile_shape(topology_shape(topic?, partitions), root, desired, config)
  end

  defp restart_stopped_groups(root, children) do
    Enum.reduce_while(children, :ok, fn
      {{:topic, :non_partitioned} = id, :undefined, :supervisor, _modules}, :ok ->
        restart_stopped_group(root, id)

      {{:partition, _index} = id, :undefined, :supervisor, _modules}, :ok ->
        restart_stopped_group(root, id)

      _child, :ok ->
        {:cont, :ok}
    end)
  end

  defp restart_stopped_group(root, id) do
    case restart_group(root, id) do
      :ok -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, {:group_restart_failed, id, reason}}}
    end
  end

  defp topology_shape(true, partitions) do
    if partitions == [], do: :non_partitioned, else: :inconsistent
  end

  defp topology_shape(false, partitions) do
    if partitions == [], do: :empty, else: {:partitioned, partitions}
  end

  defp reconcile_shape(:non_partitioned, _root, 0, _config), do: {:ok, 0}

  defp reconcile_shape(:non_partitioned, _root, desired, _config) do
    {:error, {:incompatible_topology, :non_partitioned, desired}}
  end

  defp reconcile_shape(:inconsistent, _root, _desired, _config) do
    {:error, :inconsistent_topology}
  end

  defp reconcile_shape({:partitioned, partitions}, _root, 0, _config) do
    {:ok, partition_width(partitions)}
  end

  defp reconcile_shape({:partitioned, partitions}, root, desired, config) do
    with :ok <- add_missing_partitions(root, desired, partitions, config) do
      {:ok, max(desired, partition_width(partitions))}
    end
  end

  defp reconcile_shape(:empty, root, 0, config) do
    with :ok <- start_group(root, topic_child_spec(config)), do: {:ok, 0}
  end

  defp reconcile_shape(:empty, root, desired, config) do
    with :ok <- add_missing_partitions(root, desired, [], config), do: {:ok, desired}
  end

  defp partition_width(partitions), do: Enum.max(partitions) + 1

  defp add_missing_partitions(root, desired, existing, config) do
    0..(desired - 1)
    |> Enum.reject(&(&1 in existing))
    |> Enum.reduce_while(:ok, fn index, :ok ->
      case start_group(root, partition_child_spec(index, config)) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:partition_start_failed, index, reason}}}
      end
    end)
  end

  defp start_group(root, child_spec) do
    case Supervisor.start_child(root, child_spec) do
      {:ok, _pid} -> :ok
      {:ok, _pid, _info} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, :already_present} -> restart_group(root, Map.fetch!(child_spec, :id))
      {:error, reason} -> {:error, reason}
    end
  end

  defp restart_group(root, id) do
    case Supervisor.restart_child(root, id) do
      {:ok, _pid} -> :ok
      {:ok, _pid, _info} -> :ok
      {:error, state} when state in [:running, :restarting] -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp topic_child_spec(%{worker: worker, count_key: count_key, opts: opts}) do
    group_child_spec({:topic, :non_partitioned}, worker, count_key, opts)
  end

  defp partition_child_spec(partition_index, %{worker: worker, count_key: count_key, opts: opts}) do
    partition_topic = Topic.partition(Keyword.fetch!(opts, :topic), partition_index)

    partition_opts =
      opts
      |> Keyword.put(:topic, partition_topic)
      |> Keyword.put(:name, Topic.partition(Keyword.fetch!(opts, :name), partition_index))

    group_child_spec({:partition, partition_index}, worker, count_key, partition_opts)
  end

  defp group_child_spec(id, worker, count_key, opts) do
    %{
      id: id,
      start: {Group, :start_link, [worker, count_key, opts]},
      restart: :transient,
      type: :supervisor
    }
  end
end
