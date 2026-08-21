defmodule Pulsar.Topology.Root do
  @moduledoc false

  # The stable root of one logical consumer or producer: the supervisor itself, and the
  # operations that change the tree below it. Reading that tree is Pulsar.Topology's job.

  @behaviour Supervisor

  alias Pulsar.Client
  alias Pulsar.Topic
  alias Pulsar.Topology
  alias Pulsar.Topology.Controller
  alias Pulsar.Topology.Group

  require Logger

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
  @doc false
  @spec stop_worker(pid(), pid()) :: :ok
  def stop_worker(root, worker) do
    with group when is_pid(group) <- Topology.owning_supervisor(worker),
         true <- terminated?(group, worker) and empty?(group) do
      stop_group(root, group)
    else
      _nothing_to_cascade -> :ok
    end
  end

  # Removing the root takes Controller, and so this call, with it. The removal still completes.
  defp stop_group(root, group) do
    if terminated?(root, group) and no_groups_left?(root), do: Topology.stop(root), else: :ok
  end

  defp no_groups_left?(root), do: Enum.all?(Topology.groups(root), fn {_index, pid} -> not is_pid(pid) end)

  defp terminated?(supervisor, child) do
    case Topology.child_id(supervisor, child) do
      {:ok, id} -> Topology.terminate_by_id(supervisor, id) == :ok
      :error -> false
    end
  end

  defp empty?(supervisor) do
    supervisor
    |> Topology.supervisor_children()
    |> Enum.all?(fn {_id, pid, _type, _modules} -> not is_pid(pid) end)
  end

  @impl true
  def init({config, controller_opts}) do
    {config, companions} = attach_companions(config, self())
    %{worker: worker, opts: opts} = config
    topic = Keyword.fetch!(opts, :topic)
    client = Keyword.fetch!(opts, :client)

    Logger.debug("Starting #{inspect(worker)} topology for topic #{topic}")

    controller =
      {self(), config, controller_opts}
      |> Controller.child_spec()
      |> Map.put(:id, {Controller, config.kind, topic, hashing_scheme_for_config(config)})

    # Companions start before the controller so a worker never observes the tree without them.
    # They resolve their own brokers asynchronously, so none of them delays this root coming up.
    children = companions ++ [controller]

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
      # A spec that is present but not running is a partition that was stopped on purpose. It is
      # accounted for, exactly as an :undefined child is, so reconciliation leaves it alone.
      {:error, :already_present} -> :ok
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
