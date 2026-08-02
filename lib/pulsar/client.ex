defmodule Pulsar.Client do
  @moduledoc """
  A client represents an isolated Pulsar connection context.

  Each client maintains:
  - Separate broker connections
  - Independent consumer/producer registries
  - Isolated broker configuration

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

  Declaring them here rather than beside the client means the tree matches the dependency:
  a consumer resolves brokers through registries the client owns, and cannot outlive a
  client restart holding a registration the replacement registries know nothing about.

  Several clients can coexist, each with its own connections and registries:

      children = [
        {Pulsar.Client, name: :analytics, host: "pulsar://analytics:6650"},
        {Pulsar.Client, name: :events, host: "pulsar://events:6650"}
      ]

  ## Declared and runtime resources

  Anything declared on a client belongs to it. For sets only known at runtime — a consumer
  per tenant, say — `Pulsar.Consumer.start/1` and `Pulsar.Producer.start/1` add to a running
  client, picking it with `:client`:

      Pulsar.Consumer.start(
        topic: topic,
        subscription_name: "sub",
        callback_module: MyCallback,
        client: :analytics
      )

  The two differ in one respect. Declared resources are recreated whenever the client
  restarts; resources added with `start/1` are not, because the `DynamicSupervisor` holding
  them has no static child list to bring back.
  """

  use Supervisor

  alias Pulsar.Broker.Options, as: BrokerOptions
  alias Pulsar.Client.Bootstrap

  require Logger

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
              options. Their `:client` is set to this one. They are started again whenever
              the client restarts, unlike consumers added later with `Pulsar.Consumer.start/1`.

              They start just after the client rather than during its startup, so a client that
              is up may not have them yet.

              Nor is a consumer that exists one that is consuming. Its stable supervisor is
              registered first; topic discovery, worker startup, subscription, and callback
              initialization then happen in the background. Neither declaring a consumer nor
              starting one is a readiness signal; the first
              `c:Pulsar.Consumer.Callback.handle_message/2` is.

              An unreachable broker does not stop the client: topology discovery is retried
              with backoff until the broker becomes reachable.
              """
            ],
            producers: [
              type: {:list, :keyword_list},
              default: [],
              doc: """
              Producers to run under this client, each a keyword list of `Pulsar.Producer`
              options, on the same terms as `:consumers`.

              Consumers and producers are independent and start concurrently, so a consumer
              can receive a message before a declared producer is registered. A callback that
              publishes has to handle `{:error, :producer_not_found}` in any case, since a
              producer may also be restarting.
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

    # Store broker opts in client state (passed to children via registry metadata)
    :persistent_term.put({__MODULE__, client_name, :broker_opts}, broker_opts)

    Pulsar.ProducerEpochStore.init(client_name)

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
    client_name = Keyword.fetch!(opts, :name)

    branches = [
      branch_spec(:consumers, consumer_registry(client_name), consumer_supervisor(client_name), opts),
      branch_spec(:producers, producer_registry(client_name), producer_supervisor(client_name), opts)
    ]

    %{
      id: :resources,
      start: {Supervisor, :start_link, [branches, [strategy: :one_for_one]]},
      type: :supervisor
    }
  end

  # Within a branch the order is a dependency: resources register their names in the registry
  # as they start, so a registry that came back empty would leave them alive and unreachable.
  defp branch_spec(kind, registry, supervisor, opts) do
    children = [
      {Registry, keys: :unique, name: registry},
      {DynamicSupervisor, [strategy: :one_for_one, name: supervisor] ++ Pulsar.Topology.restart_intensity()},
      {Bootstrap, {kind, opts}}
    ]

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
    :exit, {:noproc, _call} -> {:error, :client_not_found}
  end

  @doc """
  Returns the consumer resources currently running under a client.

  Each pid is the stable topology root returned by `Pulsar.Consumer.start/1`, regardless
  of how many partitions or workers that consumer has. Returns an empty list while the
  client or its consumer branch is unavailable. The order is unspecified.
  """
  @spec consumers(atom()) :: [pid()]
  def consumers(client_name \\ :default) do
    client_name |> consumer_supervisor() |> resource_roots()
  end

  @doc """
  Returns the producer resources currently running under a client.

  Each pid is the stable topology root returned by `Pulsar.Producer.start/1`, regardless
  of how many partitions or workers that producer has. Returns an empty list while the
  client or its producer branch is unavailable. The order is unspecified.
  """
  @spec producers(atom()) :: [pid()]
  def producers(client_name \\ :default) do
    client_name |> producer_supervisor() |> resource_roots()
  end

  ## Registry and Supervisor Name Helpers

  @doc false
  def broker_registry(client_name) do
    Module.concat([__MODULE__, client_name, BrokerRegistry])
  end

  @doc false
  def consumer_registry(client_name) do
    Module.concat([__MODULE__, client_name, ConsumerRegistry])
  end

  @doc false
  def producer_registry(client_name) do
    Module.concat([__MODULE__, client_name, ProducerRegistry])
  end

  @doc false
  def broker_supervisor(client_name) do
    Module.concat([__MODULE__, client_name, BrokerSupervisor])
  end

  @doc false
  def consumer_supervisor(client_name) do
    Module.concat([__MODULE__, client_name, ConsumerSupervisor])
  end

  @doc false
  def producer_supervisor(client_name) do
    Module.concat([__MODULE__, client_name, ProducerSupervisor])
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

  # Unknown options are only warned about for now, and will be rejected in the next
  # major version.
  defp validate_options!(opts) do
    {known, unknown} = Keyword.split(opts, Keyword.keys(@schema))

    if unknown != [] do
      Logger.warning("Pulsar.Client ignoring unknown options: #{inspect(Keyword.keys(unknown))}")
    end

    known
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
              "#{Enum.map_join(duplicates, ", ", &inspect/1)}. Names default to the topic, so two " <>
              "entries on one topic need distinct :name values."
    end

    entries
  end

  defp build_broker_opts(opts) do
    Keyword.take(opts, BrokerOptions.keys())
  end
end
