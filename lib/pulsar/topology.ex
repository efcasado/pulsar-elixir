defmodule Pulsar.Topology do
  @moduledoc false

  # Everything asked of a resource from outside it. Changing one is Root's and Controller's.

  alias Pulsar.Backoff
  alias Pulsar.Client
  alias Pulsar.Consumer.Worker, as: ConsumerWorker
  alias Pulsar.Hash
  alias Pulsar.Producer.Worker, as: ProducerWorker
  alias Pulsar.Topology.Controller
  alias Pulsar.Topology.Group
  alias Pulsar.Topology.Root

  @await_options_schema [
    client: [type: {:or, [:atom, :pid]}, default: :default],
    timeout: [type: :timeout, default: 5_000]
  ]
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
    if root?(root) do
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

    # A group running short of workers is a normal steady state, so this asks about those there.
    live = for {_id, worker, :worker, [^worker_module]} <- children, is_pid(worker), do: worker

    live != [] and Enum.all?(live, &worker_ready?(worker_module, &1, deadline))
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
    if root?(root) do
      case status(root, timeout) do
        :initializing -> {:error, :not_ready}
        {:ready, _shape} -> :ok
        {:error, :timeout} = error -> error
      end
    else
      {:error, :not_found}
    end
  end

  defp root?(root), do: Process.alive?(root) and kind(root) == :root

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

  defp controller_child(children) do
    Enum.find_value(children, fn
      {{Controller, kind, topic, scheme}, pid, :worker, [Controller]} ->
        %{kind: kind, topic: topic, scheme: scheme, pid: pid}

      _child ->
        false
    end)
  end

  defp controller(root) do
    case controller_child(supervisor_children(root)) do
      %{pid: pid} when is_pid(pid) -> {:ok, pid}
      _not_running -> {:error, :not_found}
    end
  end

  @doc false
  @spec topic(pid()) :: String.t() | {:error, :not_found}
  def topic(root) do
    case controller_child(supervisor_children(root)) do
      %{topic: topic} -> topic
      nil -> {:error, :not_found}
    end
  end

  @doc false
  @spec resource?(pid(), :consumers | :producers) :: boolean()
  def resource?(root, expected_kind) when expected_kind in [:consumers, :producers] do
    kind(root) == :root and resource_kind(root) == expected_kind
  end

  defp resource_kind(root) do
    case controller_child(supervisor_children(root)) do
      %{kind: kind} -> kind
      nil -> :unknown
    end
  end

  # Controller is also an OTP :worker child, so traversal explicitly allows only resource workers.
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
        if kind(pid) == :root, do: [], else: workers(pid)

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
      :root -> topology_groups(root)
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
    case controller_child(children) do
      %{scheme: scheme} -> scheme
      nil -> nil
    end
  end

  @doc false
  def supervisor_children(supervisor) do
    Supervisor.which_children(supervisor)
  catch
    :exit, {reason, {GenServer, :call, _call}} when reason in [:noproc, :normal, :shutdown, :timeout] ->
      []

    :exit, {{:shutdown, _reason}, {GenServer, :call, _call}} ->
      []
  end

  @doc """
  Which level of a topology `pid` is: its stable root, one of its groups, or a worker.

  Uses `:proc_lib.initial_call/1` rather than traversal, so supervisors are never sent calls
  intended for workers.
  """
  @spec kind(pid()) :: :root | :group | :worker
  def kind(pid) do
    case :proc_lib.initial_call(pid) do
      {:supervisor, Root, _args} -> :root
      {:supervisor, Group, _args} -> :group
      _worker -> :worker
    end
  end

  @doc """
  Stops a resource, by taking it out of whatever supervises it.

  Removal rather than an exit, which no restart type undoes, so an exit is left to mean one
  thing only: something went wrong. The resource is gone by the time this returns, and its
  groups and workers with it.
  """
  @spec stop(pid()) :: :ok
  def stop(root) when is_pid(root) do
    supervisor = owning_supervisor(root)

    case terminate_by_pid(supervisor, root) do
      :ok -> :ok
      {:error, :not_found} -> stop_by_id(supervisor, root)
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

  defp terminate_by_id(supervisor, id) do
    Supervisor.terminate_child(supervisor, id)
  catch
    :exit, _reason -> {:error, :not_found}
  end

  # A plain supervisor finds its children by id, not by pid, and merely stopping a :permanent
  # child would have it started over.
  defp stop_by_id(nil, root), do: stop_directly(root)

  defp stop_by_id(supervisor, root) do
    with {:ok, id} <- child_id(supervisor, root),
         :ok <- terminate_by_id(supervisor, id) do
      :ok
    else
      _absent -> stop_directly(root)
    end
  end

  defp terminate_by_pid(nil, _pid), do: {:error, :not_found}

  defp terminate_by_pid(supervisor, pid) do
    DynamicSupervisor.terminate_child(supervisor, pid)
  catch
    :exit, _reason -> {:error, :not_found}
  end

  # A resource that is already gone reads as stopped.
  defp stop_directly(pid) do
    Supervisor.stop(pid)
  catch
    :exit, _reason -> :ok
  end

  @doc false
  def owning_supervisor(pid) do
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
end
