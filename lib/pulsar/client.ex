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

              They start just after the client rather than during its startup, so that resolving
              a topic against an unreachable broker cannot block your application's boot: a
              client that is up may not have them yet. `Pulsar.Consumer.start/1` is synchronous
              if you need one before the next line runs.

              One that fails to start does not stop the client: it is logged and retried with
              backoff, so a consumer whose broker is unreachable at boot starts once the broker
              is reachable.
              """
            ],
            producers: [
              type: {:list, :keyword_list},
              default: [],
              doc: """
              Producers to run under this client, each a keyword list of `Pulsar.Producer`
              options, on the same terms as `:consumers`. Started before the consumers, so a
              callback that publishes has its producer available.
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
      {:error, {:shutdown, {:failed_to_start_child, Bootstrap, reason}}} -> {:error, reason}
      result -> result
    end
  end

  @impl true
  def init(opts) do
    client_name = Keyword.fetch!(opts, :name)
    broker_opts = build_broker_opts(opts)

    # Store broker opts in client state (passed to children via registry metadata)
    :persistent_term.put({__MODULE__, client_name, :broker_opts}, broker_opts)

    Pulsar.ProducerEpochStore.init(client_name)

    children = [
      {Registry, keys: :unique, name: broker_registry(client_name)},
      {Registry, keys: :unique, name: consumer_registry(client_name)},
      {Registry, keys: :unique, name: producer_registry(client_name)},
      {DynamicSupervisor, strategy: :one_for_one, name: broker_supervisor(client_name)},
      {DynamicSupervisor, strategy: :one_for_one, name: consumer_supervisor(client_name)},
      {DynamicSupervisor, strategy: :one_for_one, name: producer_supervisor(client_name)},
      {Bootstrap, opts}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
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

    case Registry.lookup(broker_registry, broker_url) do
      [{pid, _value}] -> {:ok, pid}
      [] -> {:error, :not_found}
    end
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
  Stops a client and all its resources gracefully.

  This stops all producers, consumers, brokers, and the client supervisor.

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
    |> Keyword.update!(:consumers, &validate_each!(&1, Pulsar.Consumer.Options, client))
    |> Keyword.update!(:producers, &validate_each!(&1, Pulsar.Producer.Options, client))
  end

  defp validate_each!(entries, options, client) do
    Enum.map(entries, &options.validate!(Keyword.put(&1, :client, client)))
  end

  defp build_broker_opts(opts) do
    Keyword.take(opts, BrokerOptions.keys())
  end
end
