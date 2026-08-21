defmodule Pulsar.Topology do
  @moduledoc false

  # Stable root for one logical consumer or producer; Discovery reconciles its internal groups.

  @behaviour Supervisor

  alias Pulsar.Backoff
  alias Pulsar.Client
  alias Pulsar.Consumer.Worker, as: ConsumerWorker
  alias Pulsar.Hash
  alias Pulsar.Producer.Worker, as: ProducerWorker
  alias Pulsar.Topic
  alias Pulsar.Topology.Discovery
  alias Pulsar.Topology.Group

  require Logger

  @await_options_schema [
    client: [type: {:or, [:atom, :pid]}, default: :default],
    timeout: [type: :timeout, default: 5_000]
  ]

  @doc false
  def child_spec(module, id, opts) do
    %{
      id: {module, id},
      start: {module, :start_link, [opts]},
      restart: :permanent,
      type: :supervisor
    }
  end

  @spec start_link(module(), atom() | nil, :consumers | :producers, keyword()) ::
          Supervisor.on_start()
  def start_link(worker, registry, kind, opts) when kind in [:consumers, :producers] do
    start_link(worker, registry, kind, opts, [])
  end

  # The fifth argument is an internal seam for exercising asynchronous discovery without a
  # broker. Consumer and Producer deliberately keep it out of the API they document.
  @doc false
  @spec start_link(module(), atom() | nil, :consumers | :producers, keyword(), keyword()) ::
          Supervisor.on_start()
  def start_link(worker, registry, kind, opts, controller_opts) when kind in [:consumers, :producers] do
    name = Keyword.fetch!(opts, :name)
    config = %{worker: worker, kind: kind, worker_count: worker_count(kind, opts), opts: opts}

    Supervisor.start_link(__MODULE__, {config, controller_opts}, start_options(registry, name))
  end

  defp start_options(nil, _name), do: []
  defp start_options(registry, name), do: [name: {:via, Registry, {registry, name}}]

  @typedoc false
  @type status :: :initializing | {:ready, :non_partitioned | {:partitioned, pos_integer()}}

  @doc false
  @spec await_ready(pid(), timeout()) :: :ok | {:error, :not_found | :timeout}
  def await_ready(root, timeout) when is_pid(root), do: await_resource(root, timeout, &topology_readiness/2)

  @doc false
  @spec await_ready(pid() | String.t() | atom(), :consumers | :producers, keyword()) ::
          :ok | {:error, :not_found | :timeout}
  def await_ready(resource, kind, opts) when kind in [:consumers, :producers] do
    opts = await_options!(opts)
    timeout = Keyword.fetch!(opts, :timeout)
    readiness = fn resolve, deadline -> resource_readiness(resolve, kind, deadline) end

    case resource do
      root when is_pid(root) ->
        await_resource(root, timeout, readiness)

      name when is_binary(name) or is_atom(name) ->
        resolve = fn -> Client.lookup(kind, name, Keyword.fetch!(opts, :client)) end
        await(resolve, timeout, &(&1 in [:not_found, :not_ready]), readiness)
    end
  end

  defp await_resource(root, timeout, readiness) do
    if topology_root?(root) do
      await(fn -> {:ok, root} end, timeout, &(&1 == :not_ready), readiness)
    else
      {:error, :not_found}
    end
  end

  defp await_options!(opts), do: NimbleOptions.validate!(opts, @await_options_schema)

  defp await(resolve, timeout, retryable?, readiness) do
    deadline = deadline(timeout)

    case Backoff.run(fn -> readiness.(resolve, deadline) end, retryable?, timeout) do
      {:error, :not_ready} -> {:error, :timeout}
      result -> result
    end
  end

  defp topology_readiness(resolve, deadline) do
    case resolve.() do
      {:ok, root} -> root_readiness(root, remaining(deadline))
      {:error, _reason} = error -> error
    end
  end

  defp resource_readiness(resolve, kind, deadline) do
    case resolve.() do
      {:ok, root} ->
        case root_readiness(root, remaining(deadline)) do
          :ok -> workers_readiness(root, kind, deadline)
          {:error, _reason} = error -> error
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp workers_readiness(root, kind, deadline) do
    groups = groups(root)
    worker_module = worker_module(kind)

    if groups != [] and Enum.all?(groups, &group_ready?(&1, worker_module, deadline)) do
      :ok
    else
      {:error, :not_ready}
    end
  end

  defp group_ready?({_index, group}, worker_module, deadline) when is_pid(group) do
    children = supervisor_children(group)

    children != [] and
      Enum.all?(children, fn
        {_id, worker, :worker, [^worker_module]} when is_pid(worker) ->
          worker_ready?(worker_module, worker, deadline)

        _not_ready ->
          false
      end)
  end

  defp group_ready?({_index, _not_running}, _worker_module, _deadline), do: false

  defp worker_ready?(worker_module, worker, deadline) do
    worker_module.ready?(worker, remaining(deadline))
  catch
    :exit, {_reason, {GenServer, :call, _call}} -> false
  end

  defp worker_module(:consumers), do: ConsumerWorker
  defp worker_module(:producers), do: ProducerWorker

  defp root_readiness(root, timeout) do
    if topology_root?(root) do
      case status(root, timeout) do
        :initializing -> {:error, :not_ready}
        {:ready, _shape} -> :ok
        {:error, :timeout} = error -> error
      end
    else
      {:error, :not_found}
    end
  end

  defp topology_root?(root), do: Process.alive?(root) and kind(root) == :topology

  defp deadline(:infinity), do: :infinity
  defp deadline(timeout), do: System.monotonic_time(:millisecond) + timeout

  defp remaining(:infinity), do: :infinity
  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

  defp status(root, timeout) do
    case controller(root) do
      {:ok, controller} -> GenServer.call(controller, :status, timeout)
      _not_running -> :initializing
    end
  catch
    :exit, {:timeout, {GenServer, :call, _call}} ->
      {:error, :timeout}

    :exit, {reason, {GenServer, :call, _call}} when reason in [:noproc, :normal, :shutdown] ->
      :initializing

    :exit, {{:shutdown, _reason}, {GenServer, :call, _call}} ->
      :initializing
  end

  defp controller(root) do
    root
    |> supervisor_children()
    |> Enum.find_value({:error, :not_found}, fn
      {{Discovery, _kind, _topic, _scheme}, pid, :worker, [Discovery]} when is_pid(pid) -> {:ok, pid}
      _child -> false
    end)
  end

  @doc false
  @spec topic(pid()) :: String.t() | {:error, :not_found}
  def topic(root) do
    root
    |> supervisor_children()
    |> Enum.find_value({:error, :not_found}, fn
      {{Discovery, _kind, topic, _scheme}, _pid, :worker, [Discovery]} -> topic
      _child -> false
    end)
  end

  @doc false
  @spec resource?(pid(), :consumers | :producers) :: boolean()
  def resource?(root, expected_kind) when expected_kind in [:consumers, :producers] do
    kind(root) == :topology and resource_kind(root) == expected_kind
  end

  defp resource_kind(root) do
    root
    |> supervisor_children()
    |> Enum.find_value(:unknown, fn
      {{Discovery, kind, _topic, _scheme}, _pid, :worker, [Discovery]} -> kind
      _child -> false
    end)
  end

  # Discovery is also an OTP :worker child, so traversal explicitly allows only resource workers.
  @worker_modules [ConsumerWorker, ProducerWorker]

  @doc """
  Returns the worker processes under `root`, across every partition it has.

  Only workers that are currently running are returned.
  """
  @spec workers(pid()) :: [pid()]
  def workers(root) do
    root
    |> supervisor_children()
    |> Enum.flat_map(fn
      {_id, pid, :worker, [module]} when module in @worker_modules ->
        if is_pid(pid), do: [pid], else: []

      # A nested topology root owns its own workers, which are not this resource's. A consumer's
      # dead letter producer is one, and its producer workers must not read as consumer workers.
      {_id, pid, :supervisor, _modules} when is_pid(pid) ->
        if kind(pid) == :topology, do: [], else: workers(pid)

      _child ->
        []
    end)
  end

  @doc """
  Returns `{index, pid}` for each group under `root`.

  A non-partitioned topology answers its one internal group at index zero. A `Pulsar.Topology.Group`
  passed directly is treated the same way and answers itself, so callers routing over either
  shape need no special case. Workers and stale pids have no groups and answer an empty list.

  Partition indexes come from the integer child ids rather than a group's position in the list.

  A partition between lives is reported as `:restarting` or `:undefined` instead of a pid,
  which is a distinct answer from having no such partition at all.
  """
  @spec groups(pid()) :: [{non_neg_integer(), pid() | :restarting | :undefined}]
  def groups(root) do
    case kind(root) do
      :group -> [{0, root}]
      :topology -> topology_groups(root)
      :worker -> []
    end
  end

  # Groups and the configured hashing scheme from one traversal, for a producer that needs both
  # to route a keyed message and would otherwise pay a second call per send.
  #
  # Unlike groups/1 this does not dispatch on kind/1: it assumes a topology root, and answers a
  # group or a worker as though it had no groups. Use groups/1 for anything else.
  @doc false
  @spec routing(pid()) :: {[{non_neg_integer(), pid() | :restarting | :undefined}], Hash.scheme() | nil}
  def routing(root) do
    children = supervisor_children(root)

    {groups_from(children), hashing_scheme_from(children)}
  end

  defp topology_groups(root), do: root |> supervisor_children() |> groups_from()

  defp groups_from(children) do
    Enum.flat_map(children, fn
      {{:topic, :non_partitioned}, pid, :supervisor, _modules} -> [{0, pid}]
      {{:partition, index}, pid, :supervisor, _modules} -> [{index, pid}]
      _child -> []
    end)
  end

  defp hashing_scheme_from(children) do
    Enum.find_value(children, fn
      {{Discovery, _kind, _topic, scheme}, _pid, :worker, [Discovery]} -> scheme
      _child -> false
    end)
  end

  defp supervisor_children(supervisor) do
    Supervisor.which_children(supervisor)
  catch
    :exit, {reason, {GenServer, :call, _call}} when reason in [:noproc, :normal, :shutdown] ->
      []

    :exit, {{:shutdown, _reason}, {GenServer, :call, _call}} ->
      []
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
  Retires `worker`, and whatever above it is left with nothing to do.

  Retiring is how a worker that is finished, or that hit something retrying cannot fix, leaves
  the tree. It is terminated by its parent rather than exiting, and no restart type undoes that,
  so an exit is left to mean one thing only: something went wrong. The slot stays `:undefined`,
  which is what tells discovery the partition is accounted for and must not be built again.

  `Supervisor.terminate_child/2` returns only once the child is down, so the tree can be read
  straight afterwards to decide whether to carry on up: a group whose workers have all gone is
  retired from the root, and a root whose groups have all gone is removed from its client.
  """
  @spec retire(pid(), term()) :: :ok
  def retire(worker, reason \\ :retired) when is_pid(worker) do
    # Discovery does it: a child asking its own supervisor to terminate it is shut down mid-call,
    # taking the rest of the cascade with it.
    with group when is_pid(group) <- owning_supervisor(worker),
         root when is_pid(root) <- owning_supervisor(group),
         {:ok, controller} <- controller(root) do
      Logger.info("Retiring worker for #{topic(root)}: #{inspect(reason)}")
      Discovery.retire(controller, worker)
    else
      _unavailable -> :ok
    end
  end

  @doc false
  @spec perform_retirement(pid(), pid()) :: :ok
  def perform_retirement(root, worker) do
    with group when is_pid(group) <- owning_supervisor(worker),
         true <- terminated?(group, worker) and empty?(group) do
      retire_group(root, group)
    else
      _nothing_to_cascade -> :ok
    end
  end

  # Removing the root takes Discovery, and so this call, with it. The removal still completes.
  defp retire_group(root, group) do
    if terminated?(root, group) and no_groups_left?(root), do: remove(root), else: :ok
  end

  defp no_groups_left?(root), do: Enum.all?(groups(root), fn {_index, pid} -> not is_pid(pid) end)

  defp terminated?(supervisor, child) do
    case child_id(supervisor, child) do
      {:ok, id} -> terminate_by_id(supervisor, id) == :ok
      :error -> false
    end
  end

  defp child_id(supervisor, pid) do
    supervisor
    |> supervisor_children()
    |> Enum.find_value(:error, fn
      {id, ^pid, _type, _modules} -> {:ok, id}
      _child -> false
    end)
  end

  defp empty?(supervisor) do
    supervisor
    |> supervisor_children()
    |> Enum.all?(fn {_id, pid, _type, _modules} -> not is_pid(pid) end)
  end

  defp terminate_by_id(supervisor, id) do
    Supervisor.terminate_child(supervisor, id)
  catch
    :exit, _reason -> {:error, :not_found}
  end

  @doc """
  Removes `root` from the supervisor that owns it.

  Used without a known owner for resources started directly with `start_link/1` and as the
  fallback when the expected client supervisor does not own `root`.
  """
  @spec remove(pid()) :: :ok
  def remove(root) do
    supervisor = owning_supervisor(root)

    case terminate_by_pid(supervisor, root) do
      :ok -> :ok
      {:error, :not_found} -> remove_by_id(supervisor, root)
    end
  end

  # A plain supervisor finds its children by id, not by pid, and merely stopping a :permanent
  # child would have it started over.
  defp remove_by_id(nil, root), do: stop_directly(root)

  defp remove_by_id(supervisor, root) do
    with {:ok, id} <- child_id(supervisor, root),
         :ok <- terminate_by_id(supervisor, id) do
      :ok
    else
      _absent -> stop_directly(root)
    end
  end

  @doc false
  @spec remove(pid(), GenServer.server()) :: :ok
  def remove(root, supervisor) do
    case terminate_by_pid(supervisor, root) do
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

  defp terminate_by_pid(nil, _pid), do: {:error, :not_found}

  defp terminate_by_pid(supervisor, pid) do
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
    {config, companions} = attach_companions(config, self())
    %{worker: worker, opts: opts} = config
    topic = Keyword.fetch!(opts, :topic)
    client = Keyword.fetch!(opts, :client)

    Logger.debug("Starting #{inspect(worker)} topology for topic #{topic}")

    discovery =
      {self(), config, controller_opts}
      |> Discovery.child_spec()
      |> Map.put(:id, {Discovery, config.kind, topic, hashing_scheme_for_config(config)})

    # Companions start before discovery so a worker never observes the tree without them. They
    # resolve their own brokers asynchronously, so none of them delays this root coming up.
    children = companions ++ [discovery]

    Supervisor.init(children, [strategy: :one_for_one] ++ Client.restart_intensity(client, :resource))
  end

  # Popped rather than read: `:companions` configures this root and is not part of what a worker
  # is started with.
  defp attach_companions(config, root) do
    case Keyword.pop(config.opts, :companions) do
      {nil, opts} ->
        {%{config | opts: opts}, []}

      {attach, opts} ->
        {opts, specs} = attach.(opts, root)
        {%{config | opts: opts}, specs}
    end
  end

  # Carried on the child id so routing reads it from the same which_children the groups come
  # from, keeping a send to one call. The registry value would be the usual place for this, but
  # it cannot serve every caller: Producer.send/3 takes a pid without consulting the registry,
  # and a producer started with start_link_unregistered/1 has none.
  #
  # nil where a resource does not route on a key, and for a producer whose options have not been
  # through Producer.Options; Hash.partition/3 resolves it to the default.
  defp hashing_scheme_for_config(%{kind: :producers, opts: opts}) do
    Keyword.get(opts, :hashing_scheme)
  end

  defp hashing_scheme_for_config(_config), do: nil

  @doc false
  @spec reconcile(pid(), non_neg_integer(), map()) ::
          {:ok, %{partition_count: non_neg_integer(), added_groups: [non_neg_integer()]}}
          | {:error, term()}
  def reconcile(root, desired, config) when is_integer(desired) and desired >= 0 do
    children = Supervisor.which_children(root)

    case reconcile_children(root, children, desired, config) do
      {:ok, partition_count, added_groups} ->
        {:ok, %{partition_count: partition_count, added_groups: added_groups}}

      {:error, _reason} = error ->
        error
    end
  catch
    :exit, reason -> {:error, reason}
  end

  defp reconcile_children(root, children, desired, config) do
    topic? = Enum.any?(children, &match?({{:topic, :non_partitioned}, _, :supervisor, _}, &1))

    partitions =
      Enum.flat_map(children, fn
        {{:partition, index}, _pid, :supervisor, _modules} -> [index]
        _child -> []
      end)

    reconcile_shape(topology_shape(topic?, partitions), root, desired, config)
  end

  defp topology_shape(true, partitions) do
    if partitions == [], do: :non_partitioned, else: :inconsistent
  end

  defp topology_shape(false, partitions) do
    if partitions == [], do: :empty, else: {:partitioned, partitions}
  end

  defp reconcile_shape(:non_partitioned, _root, 0, _config), do: {:ok, 0, []}

  defp reconcile_shape(:non_partitioned, _root, desired, _config) do
    {:error, {:incompatible_topology, :non_partitioned, desired}}
  end

  defp reconcile_shape(:inconsistent, _root, _desired, _config) do
    {:error, :inconsistent_topology}
  end

  defp reconcile_shape({:partitioned, partitions}, _root, 0, _config) do
    {:ok, partition_width(partitions), []}
  end

  defp reconcile_shape({:partitioned, partitions}, root, desired, config) do
    with {:ok, added_groups} <- add_missing_partitions(root, desired, partitions, config) do
      {:ok, max(desired, partition_width(partitions)), added_groups}
    end
  end

  defp reconcile_shape(:empty, root, 0, config) do
    with :ok <- start_group(root, topic_child_spec(config)), do: {:ok, 0, [0]}
  end

  defp reconcile_shape(:empty, root, desired, config) do
    with {:ok, added_groups} <- add_missing_partitions(root, desired, [], config) do
      {:ok, desired, added_groups}
    end
  end

  defp partition_width(partitions), do: Enum.max(partitions) + 1

  defp add_missing_partitions(root, desired, existing, config) do
    0..(desired - 1)
    |> Enum.reject(&(&1 in existing))
    |> Enum.sort(:desc)
    |> Enum.reduce_while({:ok, []}, fn index, {:ok, added} ->
      case start_group(root, partition_child_spec(index, config)) do
        :ok -> {:cont, {:ok, [index | added]}}
        {:error, reason} -> {:halt, {:error, {:partition_start_failed, index, reason}}}
      end
    end)
  end

  defp start_group(root, child_spec) do
    case Supervisor.start_child(root, child_spec) do
      {:ok, _pid} -> :ok
      {:ok, _pid, _info} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # :topic is the topic a worker subscribes to and :base_topic the one the resource was
  # configured with; they differ only for a partition. Workers carry both so a callback can
  # tell which partition it handles without inspecting the tree it lives in.
  defp topic_child_spec(%{worker: worker, worker_count: worker_count, opts: opts}) do
    topic_opts = Keyword.merge(opts, base_topic: Keyword.fetch!(opts, :topic), partition: nil)

    group_child_spec({:topic, :non_partitioned}, worker, worker_count, topic_opts)
  end

  defp partition_child_spec(partition_index, %{worker: worker, worker_count: worker_count, opts: opts}) do
    base_topic = Keyword.fetch!(opts, :topic)

    partition_opts =
      Keyword.merge(opts,
        topic: Topic.partition(base_topic, partition_index),
        base_topic: base_topic,
        partition: partition_index,
        name: Topic.partition(Keyword.fetch!(opts, :name), partition_index)
      )

    group_child_spec({:partition, partition_index}, worker, worker_count, partition_opts)
  end

  defp worker_count(:consumers, opts), do: Keyword.fetch!(opts, :consumer_count)
  defp worker_count(:producers, _opts), do: 1

  defp group_child_spec(id, worker, worker_count, opts) do
    %{
      id: id,
      start: {Group, :start_link, [worker, worker_count, opts]},
      restart: :permanent,
      type: :supervisor
    }
  end
end
