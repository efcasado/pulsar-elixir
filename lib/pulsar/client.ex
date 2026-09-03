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

  Both forms initialize asynchronously. `Pulsar.Consumer.await_ready/2` and
  `Pulsar.Producer.await_ready/2` provide a bounded topology-and-worker readiness barrier when
  one is needed.

  `consumers/1` and `producers/1` list the logical resources currently running under a
  client. Partitioned resources still appear once: the returned pid is their stable root,
  not one entry per partition or worker.

  See the [architecture guide](architecture.html) for the complete ownership and recovery
  model.
  """

  use Supervisor

  alias Pulsar.Broker.Options, as: BrokerOptions
  alias Pulsar.Broker.Pool, as: BrokerPool
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
              type: {:custom, __MODULE__, :validate_broker_url, []},
              required: true,
              doc: "Bootstrap broker URL, e.g. `pulsar://localhost:6650`."
            ],
            connections_per_broker: [
              type: :pos_integer,
              default: 1,
              doc: """
              Connections this client opens to each broker. Consumer and producer workers
              are assigned slots round-robin and retain their slot across worker and group
              restarts. Each slot adds one process and TCP connection per discovered broker.
              Defaults to one.
              """
            ],
            consumers: [
              type: {:list, :keyword_list},
              default: [],
              doc: """
              Consumers declared under this client, each a keyword list of `Pulsar.Consumer`
              options. Their `:client` is set to this one. See the module documentation for
              the lifecycle of declared resources.
              """
            ],
            worker_restart_intensity: [
              type: :keyword_list,
              keys: [
                max_restarts: [type: :non_neg_integer, default: 3],
                max_seconds: [type: :pos_integer, default: 5]
              ],
              default: [max_restarts: 3, max_seconds: 5],
              doc: """
              How often a consumer or producer worker under this client may be restarted before
              its partition gives up, as `[max_restarts: integer, max_seconds: integer]`. OTP's
              own intensity by default.

              A group multiplies `:max_restarts` by its worker count, so a broker dropping every
              worker registered with it at once counts as one round rather than as many. See
              `docs/architecture.md` for what that trades and how the two numbers relate to
              `Pulsar.Backoff`.
              """
            ],
            resource_restart_intensity: [
              type: :keyword_list,
              keys: [
                max_restarts: [type: :non_neg_integer, default: 3],
                max_seconds: [type: :pos_integer, default: 5]
              ],
              default: [max_restarts: 3, max_seconds: 5],
              doc: """
              How many times a partition may give up before the consumer or producer it belongs
              to does, and how many resources may do that before the client does. The failure
              then reaches whatever supervises the client.

              Much smaller than `:worker_restart_intensity`, and deliberately so: a partition
              only gives up when it genuinely cannot run, and tying this to the larger budget
              would make escalation depend on how quickly the failure comes back.
              """
            ],
            producers: [
              type: {:list, :keyword_list},
              default: [],
              doc: """
              Producers declared under this client, each a keyword list of `Pulsar.Producer`
              options. Their `:client` is set to this one.

              Consumers and producers initialize independently, so callbacks that publish
              during startup must handle `{:error, :not_found}` and
              `{:error, :not_ready}`.
              """
            ]
          ] ++ BrokerOptions.schema()

  @default_client_terms %{
    broker_opts: [],
    connections_per_broker: 1,
    restart_intensity: [
      worker: @schema[:worker_restart_intensity][:default],
      resource: @schema[:resource_restart_intensity][:default]
    ]
  }

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

    :persistent_term.put(client_key(client_name), %{
      broker_opts: build_broker_opts(opts),
      connection_slot_counter: :atomics.new(1, []),
      connections_per_broker: Keyword.fetch!(opts, :connections_per_broker),
      restart_intensity: [
        worker: Keyword.fetch!(opts, :worker_restart_intensity),
        resource: Keyword.fetch!(opts, :resource_restart_intensity)
      ]
    })

    EpochStore.init(client_name)

    children = [
      {Registry, keys: :unique, name: broker_registry(client_name)},
      brokers_spec(opts),
      resources_spec(opts)
    ]

    # Brokers first: everything below resolves topics through them, so losing them means
    # starting the resources over.
    Supervisor.init(children, strategy: :rest_for_one)
  end

  # The configured pool starts with this supervisor; pools learned through lookup are added to
  # the same owner. Stable URL-based child ids make pool removal unambiguous.
  defp brokers_spec(opts) do
    client = Keyword.fetch!(opts, :name)
    url = Keyword.fetch!(opts, :host)

    %{
      id: :brokers,
      start:
        {Supervisor, :start_link,
         [[broker_pool_spec(url, client)], [strategy: :one_for_one, name: broker_supervisor(client)]]},
      type: :supervisor
    }
  end

  defp broker_pool_spec(url, client, connection_opts \\ []) do
    name = {:via, Registry, {broker_registry(client), url}}

    opts =
      client
      |> get_broker_opts()
      |> Keyword.merge(connection_opts)
      |> Keyword.put(:connections_per_broker, connections_per_broker(client))
      |> Keyword.put(:name, name)

    BrokerPool.child_spec({url, opts})
  end

  # Separate branches isolate consumer and producer failures. OTP's default intensity at this
  # boundary remains the final escalation path above the wider resource-level budget.
  defp resources_spec(opts) do
    branches = Enum.map([:consumers, :producers], &branch_spec(&1, opts))

    %{
      id: :resources,
      start: {Supervisor, :start_link, [branches, [strategy: :one_for_one]]},
      type: :supervisor
    }
  end

  # The registry precedes its resources so :rest_for_one rebuilds the branch if it is replaced.
  defp branch_spec(kind, opts) do
    client = Keyword.fetch!(opts, :name)
    registry = registry(kind, client)
    supervisor = resource_supervisor(kind, client)

    children = [
      {Registry, keys: :unique, name: registry},
      {DynamicSupervisor, [strategy: :one_for_one, name: supervisor] ++ restart_intensity(client, :resource)},
      {Bootstrap, {kind, opts}}
    ]

    %{
      id: kind,
      start: {Supervisor, :start_link, [children, [strategy: :rest_for_one]]},
      type: :supervisor
    }
  end

  @doc false
  @spec lookup(:consumers | :producers, term(), atom() | pid()) :: {:ok, pid()} | {:error, :not_found}
  def lookup(kind, key, client \\ :default) when kind in [:consumers, :producers] do
    with {:ok, client_name} <- client_name(client) do
      lookup_registry(registry(kind, client_name), key)
    end
  end

  defp lookup_registry(registry, key) do
    case Registry.lookup(registry, key) do
      [{pid, _value}] -> {:ok, pid}
      [] -> {:error, :not_found}
    end
  rescue
    # Registry.lookup/2 raises when the client's registry is not running.
    ArgumentError -> {:error, :not_found}
  end

  @doc false
  @spec restart_intensity(atom(), :worker | :resource) :: keyword()
  def restart_intensity(client_name, level) when level in [:worker, :resource] do
    client_name |> client_term(:restart_intensity) |> Keyword.fetch!(level)
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

  defp client_name(name) when is_atom(name), do: {:ok, name}

  defp client_name(pid) when is_pid(pid) do
    case Process.info(pid, :registered_name) do
      {:registered_name, name} when is_atom(name) -> {:ok, name}
      _not_registered -> {:error, :not_found}
    end
  end

  @doc false
  @spec validate_broker_url(term()) :: {:ok, String.t()} | {:error, String.t()}
  def validate_broker_url(url) when is_binary(url) do
    case URI.new(url) do
      {:ok, %URI{scheme: scheme, host: host}}
      when scheme in ["pulsar", "pulsar+ssl"] and is_binary(host) and host != "" ->
        {:ok, url}

      _invalid ->
        {:error, "must be a valid pulsar:// or pulsar+ssl:// broker URL"}
    end
  end

  def validate_broker_url(_url), do: {:error, "must be a valid pulsar:// or pulsar+ssl:// broker URL"}

  @doc false
  def broker_registry(client_name) do
    Module.concat([__MODULE__, client_name, BrokerRegistry])
  end

  @doc false
  @spec resource_module(:consumers | :producers) :: module()
  def resource_module(kind), do: Map.fetch!(@resource_modules, kind)

  @doc false
  @spec registry(:consumers | :producers, atom()) :: atom()
  def registry(:consumers, client_name), do: Module.concat([__MODULE__, client_name, "ConsumerRegistry"])
  def registry(:producers, client_name), do: Module.concat([__MODULE__, client_name, "ProducerRegistry"])

  @doc false
  def broker_supervisor(client_name) do
    Module.concat([__MODULE__, client_name, BrokerSupervisor])
  end

  @doc false
  @spec resource_supervisor(:consumers | :producers, atom()) :: atom()
  def resource_supervisor(:consumers, client_name), do: Module.concat([__MODULE__, client_name, "ConsumerSupervisor"])

  def resource_supervisor(:producers, client_name), do: Module.concat([__MODULE__, client_name, "ProducerSupervisor"])

  @doc false
  def get_broker_opts(client_name), do: client_term(client_name, :broker_opts)

  @doc false
  @spec connections_per_broker(atom()) :: pos_integer()
  def connections_per_broker(client_name), do: client_term(client_name, :connections_per_broker)

  @doc false
  @spec allocate_connection_slots(atom(), pos_integer()) :: [non_neg_integer()]
  def allocate_connection_slots(client_name, count) when is_integer(count) and count > 0 do
    case client_terms(client_name) do
      %{connection_slot_counter: counter, connections_per_broker: pool_size} ->
        last = :atomics.add_get(counter, 1, count)
        Enum.map((last - count)..(last - 1), &rem(&1, pool_size))

      _no_client ->
        List.duplicate(0, count)
    end
  end

  defp client_term(client_name, key) do
    client_name |> client_terms() |> Map.fetch!(key)
  end

  defp client_terms(client_name), do: :persistent_term.get(client_key(client_name), @default_client_terms)

  @doc """
  Returns a connection from a random broker pool registered to the specified client.

  Defaults to the `:default` client if no client is specified.

  This is useful for operations that need any broker from a client (e.g., service discovery).
  """
  @spec random_broker(atom()) :: pid() | nil
  def random_broker(client_name \\ :default) do
    client_name
    |> registered_broker_pools()
    |> Enum.shuffle()
    |> Enum.find_value(fn pool ->
      case BrokerPool.checkout(pool, :random) do
        {:ok, broker} -> broker
        {:error, _reason} -> nil
      end
    end)
  end

  defp registered_broker_pools(client_name) do
    Registry.select(broker_registry(client_name), [{{:"$1", :"$2", :"$3"}, [], [:"$2"]}])
  rescue
    ArgumentError -> []
  end

  # An unavailable supervisor contributes no children during startup or restart.
  defp children_of(supervisor) do
    Supervisor.which_children(supervisor)
  catch
    :exit, {reason, {GenServer, :call, _call}} when reason in [:noproc, :normal, :shutdown] ->
      []

    :exit, {{:shutdown, _reason}, {GenServer, :call, _call}} ->
      []
  end

  defp resource_roots(supervisor) do
    for {_id, pid, :supervisor, _modules} <- children_of(supervisor), is_pid(pid), do: pid
  end

  @doc """
  Selects a broker connection.

  If a pool for the given URL already exists, returns one of its connections.
  Otherwise, starts a pool with the client-configured number of connections and returns one.

  The internal `:connection_slot` option selects a numbered pool member. By default, any live
  connection process is returned, including one that is currently reconnecting.

  Returns `{:ok, broker_pid}` if successful, `{:error, reason}` otherwise.
  """
  @spec start_broker(String.t(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start_broker(broker_url, opts \\ []) do
    client = Keyword.get(opts, :client, :default)
    connection_slot = Keyword.get(opts, :connection_slot, :random)
    broker_supervisor = broker_supervisor(client)

    case lookup_broker_pool(broker_url, client) do
      {:ok, pool_pid} ->
        BrokerPool.checkout(pool_pid, connection_slot)

      {:error, :not_found} ->
        connection_opts = Keyword.drop(opts, [:client, :connection_slot, :connections_per_broker])
        child_spec = broker_pool_spec(broker_url, client, connection_opts)

        case Supervisor.start_child(broker_supervisor, child_spec) do
          {:ok, pool_pid} ->
            BrokerPool.checkout(pool_pid, connection_slot)

          {:error, {:already_started, pool_pid}} ->
            BrokerPool.checkout(pool_pid, connection_slot)

          {:error, :already_present} ->
            {:error, :disconnected}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  @doc """
  Looks up an existing broker pool by URL and selects one of its connections.

  Returns `{:ok, broker_pid}` if found, or an error when the pool or requested connection is
  unavailable.
  """
  @spec lookup_broker(String.t(), keyword()) ::
          {:ok, pid()} | {:error, :not_found | :disconnected}
  def lookup_broker(broker_url, opts \\ []) do
    client = Keyword.get(opts, :client, :default)

    with {:ok, pool} <- lookup_broker_pool(broker_url, client) do
      BrokerPool.checkout(pool, Keyword.get(opts, :connection_slot, :random))
    end
  end

  defp lookup_broker_pool(broker_url, client) do
    client |> broker_registry() |> lookup_registry(broker_url)
  end

  @doc """
  Stops and removes the entire pool for a broker URL, including all of its connections.

  A later lookup may discover and start a new pool for the URL.
  """
  @spec stop_broker(String.t(), keyword()) :: :ok | {:error, :not_found}
  def stop_broker(broker_url, opts \\ []) do
    client = Keyword.get(opts, :client, :default)
    supervisor = broker_supervisor(client)
    child_id = {:broker_pool, broker_url}

    with :ok <- Supervisor.terminate_child(supervisor, child_id) do
      Supervisor.delete_child(supervisor, child_id)
    end
  catch
    :exit, {_reason, {GenServer, :call, _call}} ->
      {:error, :not_found}
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

    # Before the tree comes down, not after: a supervised client restarts the moment it stops,
    # and an erase outliving the stop would take the successor's configuration with it.
    :persistent_term.erase(client_key(client_name))

    try do
      Supervisor.stop(client_name, :normal, timeout)
    catch
      :exit, _reason -> :ok
    end

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

  defp client_key(client_name), do: {__MODULE__, client_name}
end
