defmodule Pulsar.Topology do
  @moduledoc false

  # The supervision shape a consumer or producer takes for one topic: a single `Pulsar.Group`
  # when the topic is not partitioned, or one group per partition plus a poller for partitions
  # added later. `start_link/4` resolves the width and starts whichever fits, so
  # `Pulsar.Consumer` and `Pulsar.Producer` do not each carry that decision.
  #
  # The rest is what both need to introspect or tear one down, whichever shape it took — a
  # `Pulsar.Group` is the degenerate topology, so these take either root.

  use Supervisor

  alias Pulsar.Group
  alias Pulsar.ServiceDiscovery
  alias Pulsar.Topic
  alias Pulsar.Topology.Discovery

  require Logger

  @spec start_link(module(), atom(), atom(), keyword()) :: Supervisor.on_start()
  def start_link(worker, registry, count_key, opts) do
    with {:ok, partitions} <- width(opts) do
      start_link(worker, registry, count_key, opts, partitions)
    end
  end

  defp start_link(worker, registry, count_key, opts, 0) do
    Group.start_link(worker, registry, count_key, opts)
  end

  defp start_link(worker, registry, count_key, opts, partitions) do
    opts = Keyword.put(opts, :partitions, partitions)
    name = Keyword.fetch!(opts, :name)

    Supervisor.start_link(__MODULE__, {worker, registry, count_key, opts}, name: {:via, Registry, {registry, name}})
  end

  # Resolved by the caller for Pulsar.Consumer.start/4 and Pulsar.Producer.start/2, so that
  # the lookup and its retries do not run inside the client's supervisor.
  defp width(opts) do
    case Keyword.fetch(opts, :partitions) do
      {:ok, partitions} ->
        {:ok, partitions}

      :error ->
        ServiceDiscovery.partition_count_with_retry(Keyword.fetch!(opts, :topic), client: Keyword.fetch!(opts, :client))
    end
  end

  @doc """
  Returns the worker processes under `root`, across every partition it has.
  """
  @spec workers(pid(), module()) :: [pid()]
  def workers(root, worker_module) do
    root
    |> Supervisor.which_children()
    |> Enum.flat_map(fn
      {_id, pid, :worker, [^worker_module]} when is_pid(pid) -> [pid]
      {_id, pid, :supervisor, _modules} when is_pid(pid) -> workers(pid, worker_module)
      _child -> []
    end)
  end

  @doc """
  Returns how many partitions `root` covers, or `0` when the topic is not partitioned.
  """
  @spec partitions(pid()) :: non_neg_integer()
  def partitions(root) do
    root
    |> Supervisor.which_children()
    |> Enum.count(fn {_id, _pid, type, _modules} -> type == :supervisor end)
  end

  @doc """
  Which level of a topology `pid` is: the partitioned supervisor, one partition's group, or a
  worker.

  Callers need this because a supervisor cannot answer a `GenServer` call — asking one for a
  worker's answer would take it down. Taken from `:proc_lib.initial_call/1` rather than by
  walking the tree, which is what an earlier partition-routing bug turned on.
  """
  @spec kind(pid()) :: :partitioned | :group | :worker
  def kind(pid) do
    case :proc_lib.initial_call(pid) do
      {:supervisor, __MODULE__, _args} -> :partitioned
      {:supervisor, Group, _args} -> :group
      _worker -> :worker
    end
  end

  @doc """
  Removes `root` from the supervisor that owns it.

  Asks the process which supervisor that is, rather than deriving one from `:client`: a pid
  carries no clue which client it belongs to, and stopping a permanent child any other way
  just has its supervisor start it again. One supervised by the caller has no owning
  `DynamicSupervisor`, so it is stopped directly.
  """
  @spec remove(pid()) :: :ok
  def remove(root) do
    case terminate_child(owning_supervisor(root), root) do
      :ok -> :ok
      {:error, :not_found} -> stop_directly(root)
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
  def init({worker, registry, count_key, opts}) do
    topic = Keyword.fetch!(opts, :topic)
    partitions = Keyword.fetch!(opts, :partitions)

    Logger.info("Starting partitioned #{inspect(worker)} for topic #{topic} with #{partitions} partitions")

    build_child_spec = &partition_child_spec(&1, worker, registry, count_key, opts)

    discovery_children =
      Discovery.child_specs(self(),
        topic: topic,
        client: Keyword.fetch!(opts, :client),
        interval_ms: Keyword.fetch!(opts, :partition_discovery_interval_ms),
        build_child_spec: build_child_spec
      )

    partition_children = Enum.map(0..(partitions - 1), build_child_spec)

    Supervisor.init(partition_children ++ discovery_children, strategy: :one_for_one)
  end

  defp partition_child_spec(partition_index, worker, registry, count_key, opts) do
    partition_topic = Topic.partition(Keyword.fetch!(opts, :topic), partition_index)

    partition_opts =
      opts
      |> Keyword.put(:topic, partition_topic)
      |> Keyword.put(:name, Topic.partition(Keyword.fetch!(opts, :name), partition_index))

    %{
      id: partition_topic,
      start: {Group, :start_link, [worker, registry, count_key, partition_opts]},
      restart: :permanent,
      type: :supervisor
    }
  end
end
