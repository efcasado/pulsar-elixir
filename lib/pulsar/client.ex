defmodule Pulsar.Client do
  @moduledoc """
  A client represents an isolated Pulsar connection context and owns the consumers and
  producers that use it.

  ## Usage

  The client is the only thing that belongs in the host application's supervision tree.
  Consumers and producers are declared on it and run underneath it:

      children = [
        {Pulsar.Client,
         host: "pulsar://localhost:6650",
         consumers: [
           [topic: topic, subscription_name: "sub", callback_module: MyCallback]
         ]}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  Several named clients can coexist. Consumers and producers select one with `:client`,
  defaulting to `:default`.

  ## Declared and runtime resources

  For sets only known at runtime — a consumer per tenant, say — `Pulsar.Consumer.start/1`
  and `Pulsar.Producer.start/1` add to a running client:

      Pulsar.Consumer.start(
        topic: topic,
        subscription_name: "sub",
        callback_module: MyCallback,
        client: :analytics
      )

  Declared resources are recreated after their client or resource branch restarts. Runtime
  resources are not; their caller must restore them.

  Starting a client or resource establishes ownership, not readiness. Resource initialization
  continues in the background, so operations may temporarily return `{:error, :not_ready}`.

  `consumers/1` and `producers/1` list the logical resources currently running under a
  client. Partitioned resources still appear once: the returned pid is their stable root,
  not one entry per partition or worker.

  See the [architecture guide](architecture.html) for the complete ownership and recovery
  model.
  """

  use Supervisor

  alias Pulsar.Broker.Options, as: BrokerOptions
  alias Pulsar.Client.Bootstrap
  alias Pulsar.Producer.EpochStore

  @resource_modules %{consumers: Pulsar.Consumer, producers: Pulsar.Producer}

  @schema [
            name: [
              type: :atom,
              default: :default,
              doc: """
              Name the client is registered under, and the name consumers and producers
              select it by. Defaults to `:default`, which is also their default `:client`.
              """
            ],
            host: [
              type: :string,
              required: true,
              doc: "Bootstrap broker URL, e.g. `pulsar://localhost:6650`."
            ],
            consumers: [
              type: {:list, :keyword_list},
              default: [],
              doc: """
              Consumers to run under this client, each a keyword list of `Pulsar.Consumer`
              options. Their `:client` is set to this one, and they are restored after the
              client or consumer branch restarts. They initialize asynchronously after the
              client starts; declaration is not a readiness signal.
              """
            ],
            producers: [
              type: {:list, :keyword_list},
              default: [],
              doc: """
              Producers to run under this client, each a keyword list of `Pulsar.Producer`
              options, with the same asynchronous lifecycle and restoration guarantees as
              `:consumers`.

              Consumers and producers are independent and start concurrently, so a consumer
              can receive a message before a declared producer is registered. A callback that
              publishes has to handle `{:error, :producer_not_found}` and
              `{:error, :not_ready}`, since a producer may be starting or restarting.
              """
            ]
          ] ++ BrokerOptions.schema()

  ## Public API

  @doc false
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, :default),
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc """
  Starts a client with the given options.

  ## Options

  #{NimbleOptions.docs(@schema)}
  """
  def start_link(opts) do
    opts = validate_options!(opts)
    name = Keyword.fetch!(opts, :name)

    case Supervisor.start_link(__MODULE__, opts, name: name) do
      {:error, reason} -> {:error, unwrap_start_error(reason)}
      result -> result
    end
  end

  # The failure a caller cares about is nested once per branch supervisor it happened under.
  defp unwrap_start_error({:shutdown, {:failed_to_start_child, _id, reason}}), do: unwrap_start_error(reason)
  defp unwrap_start_error(reason), do: reason

  @impl true
  def init(opts) do
    client_name = Keyword.fetch!(opts, :name)
    broker_opts = build_broker_opts(opts)

    :persistent_term.put({__MODULE__, client_name, :broker_opts}, broker_opts)

    EpochStore.init(client_name)

    children = [
      {Registry, keys: :unique, name: broker_registry(client_name)},
      {DynamicSupervisor, strategy: :one_for_one, name: broker_supervisor(client_name)},
      resources_spec(opts)
    ]

    # Brokers first: everything below resolves topics through them, so losing them means
    # starting the resources over.
    Supervisor.init(children, strategy: :rest_for_one)
  end

  # Consumers and producers depend on the brokers and on their own registry, but not on each
  # other. One flat :rest_for_one chain made them dependants of whichever came first, so a
  # consumer supervisor exceeding its restart intensity took every runtime producer with it.
  defp resources_spec(opts) do
    branches = Enum.map([:consumers, :producers], &branch_spec(&1, opts))

    # This boundary counts whole branch failures, not the resource restarts below them. Keep
    # OTP's default intensity so repeatedly rebuilding a branch escalates to the client.
    %{
      id: :resources,
      start: {Supervisor, :start_link, [branches, [strategy: :one_for_one]]},
      type: :supervisor
    }
  end

  # Within a branch the order is a dependency: resources register their names in the registry
  # as they start, so a registry that came back empty would leave them alive and unreachable.
  defp branch_spec(kind, opts) do
    client = Keyword.fetch!(opts, :name)
    registry = registry(kind, client)
    supervisor = resource_supervisor(kind, client)

    children = [
      {Registry, keys: :unique, name: registry},
      {DynamicSupervisor, [strategy: :one_for_one, name: supervisor] ++ Pulsar.Topology.restart_intensity()},
      {Bootstrap, {kind, opts}}
    ]

    # Resource roots use the wider topology budget in their DynamicSupervisor. The branch
    # supervisor described below keeps OTP's default so repeated registry, Bootstrap, or
    # exhausted DynamicSupervisor failures rebuild the branch instead of cycling indefinitely.
    %{
      id: kind,
      start: {Supervisor, :start_link, [children, [strategy: :rest_for_one]]},
      type: :supervisor
    }
  end

  @doc false
  @spec lookup(atom(), term()) :: {:ok, pid()} | {:error, :not_found}
  def lookup(registry, key) do
    case Registry.lookup(registry, key) do
      [{pid, _value}] -> {:ok, pid}
      [] -> {:error, :not_found}
    end
  rescue
    # A client that is not running has no registry to ask, which reads the same as having
    # nothing registered rather than raising at whoever asked.
    ArgumentError -> {:error, :not_found}
  end

  @doc false
  @spec start_resource(atom(), {module(), term()}) ::
          DynamicSupervisor.on_start_child() | {:error, :client_not_found}
  def start_resource(supervisor, child_spec) do
    DynamicSupervisor.start_child(supervisor, child_spec)
  catch
    :exit, {reason, {GenServer, :call, _call}} when reason in [:noproc, :normal, :shutdown] ->
      {:error, :client_not_found}

    :exit, {{:shutdown, _reason}, {GenServer, :call, _call}} ->
      {:error, :client_not_found}
  end

  @doc """
  Returns the consumer resources currently running under a client.

  Each pid is the stable topology root returned by `Pulsar.Consumer.start/1`, regardless
  of how many partitions or workers that consumer has. Returns an empty list while the
  client or its consumer branch is unavailable. The order is unspecified.
  """
  @spec consumers(atom()) :: [pid()]
  def consumers(client_name \\ :default) do
    :consumers |> resource_supervisor(client_name) |> resource_roots()
  end

  @doc """
  Returns the producer resources currently running under a client.

  Each pid is the stable topology root returned by `Pulsar.Producer.start/1`, regardless
  of how many partitions or workers that producer has. Returns an empty list while the
  client or its producer branch is unavailable. The order is unspecified.
  """
  @spec producers(atom()) :: [pid()]
  def producers(client_name \\ :default) do
    :producers |> resource_supervisor(client_name) |> resource_roots()
  end

  ## Process Name Helpers

  @doc false
  @spec name(atom() | pid()) :: {:ok, atom()} | {:error, :not_found}
  def name(name) when is_atom(name), do: {:ok, name}

  def name(pid) when is_pid(pid) do
    case Process.info(pid, :registered_name) do
      {:registered_name, name} when is_atom(name) -> {:ok, name}
      _not_registered -> {:error, :not_found}
    end
  end

  @doc false
  def broker_registry(client_name) do
    Module.concat([__MODULE__, client_name, BrokerRegistry])
  end

  @doc false
  @spec resource_module(:consumers | :producers) :: module()
  def resource_module(kind), do: Map.fetch!(@resource_modules, kind)

  @doc false
  @spec registry(:consumers | :producers, atom()) :: atom()
  def registry(kind, client_name), do: process_name(kind, client_name, "Registry")

  @doc false
  def broker_supervisor(client_name) do
    Module.concat([__MODULE__, client_name, BrokerSupervisor])
  end

  @doc false
  @spec resource_supervisor(:consumers | :producers, atom()) :: atom()
  def resource_supervisor(kind, client_name), do: process_name(kind, client_name, "Supervisor")

  defp process_name(kind, client_name, suffix) do
    resource = kind |> resource_module() |> Module.split() |> List.last()
    Module.concat([__MODULE__, client_name, resource <> suffix])
  end

  @doc false
  def get_broker_opts(client_name) do
    :persistent_term.get({__MODULE__, client_name, :broker_opts}, [])
  end

  @doc """
  Returns a random broker process from the specified client's broker supervisor.

  Defaults to the `:default` client if no client is specified.

  This is useful for operations that need any broker from a client (e.g., service discovery).
  """
  @spec random_broker(atom()) :: pid() | nil
  def random_broker(client_name \\ :default) do
    case children_of(broker_supervisor(client_name)) do
      [] ->
        nil

      children ->
        {_id, pid, _, _} = Enum.random(children)
        pid
    end
  end

  # A client that was never started has no supervisor to ask, which reads the same as a
  # client with no brokers rather than exiting whoever asked.
  defp children_of(supervisor) do
    Supervisor.which_children(supervisor)
  catch
    :exit, _reason -> []
  end

  defp resource_roots(supervisor) do
    for {_id, pid, :supervisor, _modules} <- children_of(supervisor), is_pid(pid), do: pid
  end

  @doc """
  Starts a broker connection.

  If a broker for the given URL already exists, returns the existing broker.
  Otherwise, starts a new broker connection with the provided options.

  Returns `{:ok, broker_pid}` if successful, `{:error, reason}` otherwise.
  """
  @spec start_broker(String.t(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start_broker(broker_url, opts \\ []) do
    client = Keyword.get(opts, :client, :default)
    broker_registry = broker_registry(client)
    broker_supervisor = broker_supervisor(client)

    case lookup_broker(broker_url, client: client) do
      {:ok, broker_pid} ->
        {:ok, broker_pid}

      {:error, :not_found} ->
        global_opts = get_broker_opts(client)
        merged_opts = Keyword.merge(global_opts, Keyword.delete(opts, :client))
        registry_opts = [{:name, {:via, Registry, {broker_registry, broker_url}}} | merged_opts]

        child_spec = %{
          id: broker_url,
          start: {Pulsar.Broker, :start_link, [broker_url, registry_opts]},
          restart: :permanent
        }

        case DynamicSupervisor.start_child(broker_supervisor, child_spec) do
          {:ok, broker_pid} ->
            {:ok, broker_pid}

          {:error, {:already_started, broker_pid}} ->
            {:ok, broker_pid}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  @doc """
  Looks up an existing broker connection by broker URL.

  Returns `{:ok, broker_pid}` if found, `{:error, :not_found}` otherwise.
  """
  @spec lookup_broker(String.t(), keyword()) :: {:ok, pid()} | {:error, :not_found}
  def lookup_broker(broker_url, opts \\ []) do
    client = Keyword.get(opts, :client, :default)
    broker_registry = broker_registry(client)

    lookup(broker_registry, broker_url)
  end

  @doc """
  Stops a broker connection by broker URL.
  """
  @spec stop_broker(String.t(), keyword()) :: :ok | {:error, :not_found}
  def stop_broker(broker_url, opts \\ []) do
    case lookup_broker(broker_url, opts) do
      {:ok, broker_pid} ->
        Pulsar.Broker.stop(broker_pid)
        :ok

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  @doc """
  Stops a client, and with it every consumer, producer and broker connection it owns.

  For a client you started yourself, from a script or IEx. A client in a supervision tree is
  restarted by its supervisor whatever its exit reason, so this only cycles it; stop those by
  removing them from the tree.

  ## Options

  - `:timeout` - Maximum time to wait for shutdown (default: 5000ms)

  ## Examples

      Pulsar.Client.stop(:my_client)

  """
  @spec stop(atom(), keyword()) :: :ok
  def stop(client_name, opts \\ []) when is_atom(client_name) do
    timeout = Keyword.get(opts, :timeout, 5000)

    try do
      Supervisor.stop(client_name, :normal, timeout)
    catch
      :exit, _reason -> :ok
    end

    :persistent_term.erase({__MODULE__, client_name, :broker_opts})
    :ok
  end

  ## Private Functions

  defp validate_options!(opts) do
    opts
    |> NimbleOptions.validate!(@schema)
    |> validate_resources!()
  end

  defp validate_resources!(opts) do
    client = Keyword.fetch!(opts, :name)

    opts
    |> Keyword.update!(:consumers, &validate_each!(&1, Pulsar.Consumer, Pulsar.Consumer.Options, client))
    |> Keyword.update!(:producers, &validate_each!(&1, Pulsar.Producer, Pulsar.Producer.Options, client))
  end

  defp validate_each!(entries, module, options, client) do
    entries
    |> Enum.map(&options.validate!(Keyword.put(&1, :client, client)))
    |> reject_duplicate_names!(module, client)
  end

  # Two declarations resolving to one registry name would leave the second silently
  # discarded, since starting it reports the first as already started.
  defp reject_duplicate_names!(entries, module, client) do
    duplicates =
      entries
      |> Enum.frequencies_by(&module.id/1)
      |> Enum.filter(fn {_name, count} -> count > 1 end)
      |> Enum.map(fn {name, _count} -> name end)

    if duplicates != [] do
      raise ArgumentError,
            "Pulsar client #{inspect(client)} declares more than one #{inspect(module)} named " <>
              "#{Enum.map_join(duplicates, ", ", &inspect/1)}. Give each declaration a distinct :name."
    end

    entries
  end

  defp build_broker_opts(opts) do
    Keyword.take(opts, BrokerOptions.keys())
  end
end
