defmodule Pulsar do
  @moduledoc """
  Pulsar client for Elixir.

  This module provides a high-level API for interacting with the Elixir client for Apache Pulsar.

  ## Architecture

  The library supports multi-client architecture, allowing you to connect to multiple Pulsar
  clusters simultaneously. Each client maintains its own set of registries and supervisors for
  brokers, consumers, and producers.

  ### Supervision Tree

      Pulsar.Supervisor
      └── Client (:default)
          ├── Registries
          │   ├── BrokerRegistry
          │   ├── ConsumerRegistry
          │   └── ProducerRegistry
          │
          ├── ProducerEpochStore (ETS)
          │
          ├── BrokerSupervisor
          │   ├── Broker 1
          │   │   ├── monitors: C1, C2, DLQ-P1, P1
          │   └── Broker 2
          │       ├── monitors: C3, C4
          │
          ├── ConsumerSupervisor
          │   ├── ConsumerGroup: my-topic
          │   │   └── C1 (with DLQ policy)
          │   │       └── DLQ-P1 (linked process)
          │   │
          │   └── PartitionedConsumer: my-partitioned-topic
          │       ├── ConsumerGroup partition-0
          │       │   └── C2
          │       ├── ConsumerGroup partition-1
          │       │   └── C3
          │       └── ConsumerGroup partition-2
          │           └── C4
          │
          └── ProducerSupervisor
              ├── ProducerGroup: my-topic
              │   └── P1
              │
              └── PartitionedProducer: my-partitioned-topic
                  ├── ProducerGroup partition-0
                  │   └── P2
                  ├── ProducerGroup partition-1
                  │   └── P3
                  └── ProducerGroup partition-2
                      └── P4

  ### Consumer/Producer Architecture

  Both consumers and producers are managed through supervised processes:

  **Consumers:**
  - **Regular topics**: Consumer groups with configurable process count
  - **Partitioned topics**: PartitionedConsumer supervisor managing consumer groups per partition
  - The `start_consumer/4` function returns a single PID that can be registered with a name,
    regardless of partitioning or process count

  **Producers:**
  - **Regular topics**: Producer groups with configurable process count
  - **Partitioned topics**: PartitionedProducer supervisor managing producer groups per partition
  - The `start_producer/2` function returns a single PID that can be registered with a name,
    regardless of partitioning or process count

  ## Examples

      # Start a broker connection (optional - automatically started by clients)
      {:ok, broker_pid} = Pulsar.start_broker("pulsar://other-broker:6650")

      # Start a producer
      {:ok, producer_pid} = Pulsar.start_producer(
        "persistent://public/default/my-topic",
        name: :my_producer
      )

      # Send a message
      {:ok, message_id} = Pulsar.send(:my_producer, "Hello, Pulsar!")

      # Start a producer for a partitioned topic (single PID returned)
      {:ok, producer_pid} = Pulsar.start_producer(
        "persistent://public/default/my-partitioned-topic",
        name: :my_partitioned_producer
      )

      # Messages are automatically routed to partitions
      {:ok, message_id} = Pulsar.send(:my_partitioned_producer, "Partitioned message!")

      # Start a consumer for a regular topic
      {:ok, consumer_pid} = Pulsar.start_consumer(
        "persistent://public/default/my-topic",
        "my-subscription",
        MyApp.MessageHandler,
        subscription_type: :Exclusive
      )

      # Start a consumer for a partitioned topic (single PID returned)
      {:ok, consumer_pid} = Pulsar.start_consumer(
        "persistent://public/default/my-partitioned-topic",
        "my-subscription",
        MyApp.MessageHandler,
        subscription_type: :Shared,
        consumer_count: 2  # 2 consumers per partition
      )

      # Register consumer/producer with custom name
      {:ok, consumer_pid} = Pulsar.start_consumer(
        "persistent://public/default/my-topic",
        "my-subscription",
        MyApp.MessageHandler,
        name: MyApp.MyConsumer
      )

      # Stop consumer/producer
      Pulsar.stop_consumer(consumer_pid)
      Pulsar.stop_producer(producer_pid)

      # Or stop by name
      Pulsar.stop_consumer(:my_consumer)
      Pulsar.stop_producer(:my_producer)

      # Lookup by name
      {:ok, consumer_pid} = Pulsar.lookup_consumer("my-topic-my-subscription")
      {:ok, producer_pid} = Pulsar.lookup_producer(:my_producer)
  """
  use Application

  alias Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData

  require Logger

  @default_client :default
  @app_supervisor Pulsar.Supervisor

  @doc """
  Start the Pulsar application with custom configuration.

  ## Examples

      # Start with custom configuration (single client)
      {:ok, pid} = Pulsar.start(
        host: "pulsar://localhost:6650",
        consumers: [
          {:my_consumer, [
            topic: "my-topic",
            subscription_name: "my-subscription",
            callback_module: MyConsumerCallback,
            subscription_type: :Shared,
            startup_delay_ms: 500,
            startup_jitter_ms: 1000
          ]}
        ]
      )

      # Start with multiple clients
      {:ok, pid} = Pulsar.start(
        clients: [
          default: [host: "pulsar://localhost:6650"],
          cluster_2: [host: "pulsar://other:6650"]
        ],
        consumers: [
          {:consumer1, [client: :default, topic: "topic1", ...]},
          {:consumer2, [client: :cluster_2, topic: "topic2", ...]}
        ]
      )

      # Later, stop it
      :ok = Pulsar.stop(pid)
  """
  def start(config) do
    start(:normal, config)
  end

  def start_link(config), do: start(config)

  @impl true
  def start(_type, opts) do
    consumers =
      Keyword.get(opts, :consumers, Application.get_env(:pulsar, :consumers, []))

    producers =
      Keyword.get(opts, :producers, Application.get_env(:pulsar, :producers, []))

    # Get client configurations - support both :clients (multi-client) and :host (single client)
    clients_config =
      case Keyword.get(opts, :clients, Application.get_env(:pulsar, :clients)) do
        nil ->
          # Fallback to legacy single-client mode with :host
          bootstrap_host = Keyword.get(opts, :host, Application.get_env(:pulsar, :host))
          if bootstrap_host, do: [{@default_client, [host: bootstrap_host] ++ opts}], else: []

        clients when is_list(clients) ->
          clients
      end

    # Start clients (brokers are now started within each client)
    children =
      Enum.map(clients_config, fn {client_name, client_opts} ->
        {Pulsar.Client, Keyword.put(client_opts, :name, client_name)}
      end)

    sup_opts = [strategy: :one_for_one, name: @app_supervisor]
    {:ok, pid} = Supervisor.start_link(children, sup_opts)

    # Start consumers
    Enum.each(consumers, fn {consumer_name, consumer_opts} ->
      topic = Keyword.fetch!(consumer_opts, :topic)
      subscription_name = Keyword.fetch!(consumer_opts, :subscription_name)
      callback_module = Keyword.fetch!(consumer_opts, :callback_module)
      client = Keyword.get(consumer_opts, :client, @default_client)

      opts =
        consumer_opts
        |> Keyword.delete(:topic)
        |> Keyword.delete(:subscription_name)
        |> Keyword.delete(:callback_module)
        |> Keyword.put_new(:name, Atom.to_string(consumer_name))
        |> Keyword.put(:client, client)

      Pulsar.start_consumer(topic, subscription_name, callback_module, opts)
    end)

    # Start producers
    Enum.each(producers, fn {producer_name, producer_opts} ->
      topic = Keyword.fetch!(producer_opts, :topic)
      client = Keyword.get(producer_opts, :client, @default_client)

      opts =
        producer_opts
        |> Keyword.delete(:topic)
        |> Keyword.put(:name, producer_name)
        |> Keyword.put(:client, client)

      Pulsar.start_producer(topic, opts)
    end)

    {:ok, pid}
  end

  @impl true
  def stop(pid) when is_pid(pid) do
    Supervisor.stop(pid, :normal)
  end

  @doc """
  Starts a broker connection.

  Delegates to `Pulsar.Client.start_broker/2`.
  """
  @spec start_broker(String.t(), keyword()) :: {:ok, pid()} | {:error, term()}
  defdelegate start_broker(broker_url, opts \\ []), to: Pulsar.Client

  @doc """
  Looks up an existing broker connection by broker URL.

  Delegates to `Pulsar.Client.lookup_broker/2`.
  """
  @spec lookup_broker(String.t(), keyword()) :: {:ok, pid()} | {:error, :not_found}
  defdelegate lookup_broker(broker_url, opts \\ []), to: Pulsar.Client

  @doc """
  Stops a broker connection by broker URL.

  Delegates to `Pulsar.Client.stop_broker/2`.
  """
  @spec stop_broker(String.t(), keyword()) :: :ok | {:error, :not_found}
  defdelegate stop_broker(broker_url, opts \\ []), to: Pulsar.Client

  @doc """
  Starts a consumer for a topic (regular or partitioned).

  This is the primary way to consume messages from Pulsar topics. For regular topics,
  a single consumer group is created. For partitioned topics, a PartitionedConsumer
  supervisor is created that manages individual consumer groups for each partition.

  ## Parameters

  - `topic` - The topic to subscribe to (regular or partitioned)
  - `subscription_name` - Name of the subscription
  - `callback_module` - Module that implements `Pulsar.Consumer.Callback` behaviour
  - `opts` - Optional parameters:
    - `:subscription_type` - Type of subscription (e.g., :Exclusive, :Shared, :Key_Shared, default: :Shared)
    - `:name` - Custom name for the consumer (default: "topic-subscription_name")
    - `:consumer_count` - Number of consumer processes per topic/partition (default: 1)
    - `:init_args` - Arguments passed to callback module's init/1 function
    - `:flow_initial` - Initial flow permits (default: 100)
    - `:flow_threshold` - Flow permits threshold for refill (default: 50)
    - `:flow_refill` - Flow permits refill amount (default: 50)
    - `:initial_position` - Initial position for subscription (`:latest` or `:earliest`, defaults to `:latest`)
    - Other options passed to ConsumerGroup supervisor

  ## Return Values

  - For regular topics: PID of the consumer group supervisor
  - For partitioned topics: PID of the PartitionedConsumer supervisor that manages all partition groups

  ## Partitioned Topics

  When you subscribe to a partitioned topic, the function automatically:
  1. Queries the broker for partition metadata
  2. Creates a PartitionedConsumer supervisor
  3. The PartitionedConsumer creates separate consumer groups for each partition
  4. Returns a single PID for the PartitionedConsumer supervisor

  ## Consumer Naming and Registry

  All consumers are automatically registered in a registry and can be looked up by name:
  - **Default naming**: `"<topic>-<subscription_name>"`
  - **Custom naming**: Provided via the `:name` option
  - **Partitioned topics**: Individual partition groups are named `"<base_name>-partition-<index>"`

  This allows you to manage consumers by name without keeping track of PIDs.

  ## Examples

      # Regular topic - returns single PID
      iex> {:ok, consumer_pid} = Pulsar.start_consumer(
      ...>   "persistent://public/default/my-topic",
      ...>   "my-subscription",
      ...>   MyApp.MessageHandler,
      ...>   subscription_type: :Exclusive
      ...> )
      {:ok, #PID<0.456.0>}

      # Partitioned topic
      iex> {:ok, consumer_pid} = Pulsar.start_consumer(
      ...>   "persistent://public/default/my-partitioned-topic",
      ...>   "my-subscription",
      ...>   MyApp.MessageHandler,
      ...>   subscription_type: :Shared
      ...> )
      {:ok, #PID<0.456.0>}

      # Register with custom name
      iex> {:ok, consumer_pid} = Pulsar.start_consumer(
      ...>   "persistent://public/default/my-partitioned-topic",
      ...>   "my-subscription",
      ...>   MyApp.MessageHandler,
      ...>   subscription_type: :Key_Shared,
      ...>   consumer_count: 2,
      ...>   name: MyApp.MyConsumer
      ...> )
      {:ok, #PID<0.456.0>}
  """

  @spec start_consumer(String.t(), String.t(), module(), keyword()) :: {:ok, pid} | {:error, term}
  def start_consumer(topic, subscription_name, callback_module, opts \\ []) do
    client = Keyword.get(opts, :client, @default_client)
    consumer_supervisor = Pulsar.Client.consumer_supervisor(client)
    subscription_type = Keyword.get(opts, :subscription_type, :Shared)
    name = Keyword.get(opts, :name, topic <> "-" <> subscription_name)

    case check_partitioned_topic(topic, client) do
      {:ok, 0} ->
        # Regular topic - create single consumer group
        child_spec = %{
          id: name,
          start: {
            Pulsar.ConsumerGroup,
            :start_link,
            [name, topic, subscription_name, subscription_type, callback_module, opts]
          },
          restart: :permanent,
          type: :supervisor
        }

        DynamicSupervisor.start_child(consumer_supervisor, child_spec)

      {:ok, partitions} when partitions > 0 ->
        # Partitioned topic - create PartitionedConsumer supervisor
        child_spec = %{
          id: name,
          start: {
            Pulsar.PartitionedConsumer,
            :start_link,
            [name, topic, partitions, subscription_name, subscription_type, callback_module, opts]
          },
          restart: :permanent,
          type: :supervisor
        }

        DynamicSupervisor.start_child(consumer_supervisor, child_spec)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Stops a consumer (regular or partitioned).

  Accepts either:
  - A consumer PID (returned by `start_consumer/1`)
  - A consumer ID string (for programmatic access)

  For partitioned topics, this stops the PartitionedConsumer supervisor,
  which automatically stops all partition consumer groups.

  Returns `:ok` if successful, `{:error, :not_found}` if the consumer doesn't exist.

  ## Examples

      # Stop regular consumer
      iex> {:ok, consumer_pid} = Pulsar.start_consumer(...)
      iex> Pulsar.stop_consumer(consumer_pid)
      :ok

      # Stop partitioned consumer (stops all partitions)
      iex> {:ok, consumer_pid} = Pulsar.start_consumer(...)  # partitioned topic
      iex> Pulsar.stop_consumer(consumer_pid)
      :ok

      # Stop by consumer ID string
      iex> Pulsar.stop_consumer("my-topic-my-subscription")
      :ok
  """
  @spec stop_consumer(pid() | String.t(), keyword()) :: :ok | {:error, :not_found}
  def stop_consumer(group_pid, opts \\ [])

  def stop_consumer(group_pid, _opts) when is_pid(group_pid) do
    Supervisor.stop(group_pid)
  end

  def stop_consumer(group_id, opts) when is_binary(group_id) do
    client = Keyword.get(opts, :client, @default_client)
    consumer_registry = Pulsar.Client.consumer_registry(client)

    case Registry.lookup(consumer_registry, group_id) do
      [{group_pid, _value}] ->
        stop_consumer(group_pid)

      [] ->
        {:error, :not_found}
    end
  end

  @doc """
  Looks up a consumer by name.

  Returns `{:ok, consumer_pid}` if found, `{:error, :not_found}` otherwise.

  ## Examples

      iex> {:ok, consumer_pid} = Pulsar.start_consumer("my-topic", "my-subscription", MyHandler, name: "my-consumer")
      iex> Pulsar.lookup_consumer("my-consumer")
      {:ok, consumer_pid}

      iex> Pulsar.lookup_consumer("nonexistent")
      {:error, :not_found}
  """
  @spec lookup_consumer(String.t(), keyword()) :: {:ok, pid()} | {:error, :not_found}
  def lookup_consumer(name, opts \\ []) do
    client = Keyword.get(opts, :client, @default_client)
    consumer_registry = Pulsar.Client.consumer_registry(client)

    case Registry.lookup(consumer_registry, name) do
      [{consumer_pid, _value}] -> {:ok, consumer_pid}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Gets all consumer processes managed by a consumer manager.

  Works with both ConsumerGroup and PartitionedConsumer supervisors.
  Returns a flat list of consumer process PIDs.
  """
  @spec get_consumers(pid() | String.t(), keyword()) :: [pid()] | {:error, :not_found}
  def get_consumers(group_pid, opts \\ [])

  def get_consumers(group_pid, _opts) when is_pid(group_pid) do
    case Supervisor.which_children(group_pid) do
      [] ->
        []

      [{_id, _child_pid, :worker, [Pulsar.Consumer]} | _] ->
        # This is a ConsumerGroup - children are Consumer processes
        Pulsar.ConsumerGroup.get_consumers(group_pid)

      [{_id, _child_pid, :supervisor, _modules} | _] ->
        # This is a PartitionedConsumer - children are ConsumerGroup supervisors
        Pulsar.PartitionedConsumer.get_consumers(group_pid)

      _ ->
        {:error, :unknown_supervisor_type}
    end
  end

  def get_consumers(group_id, opts) when is_binary(group_id) do
    client = Keyword.get(opts, :client, @default_client)
    consumer_registry = Pulsar.Client.consumer_registry(client)

    case Registry.lookup(consumer_registry, group_id) do
      [{group_pid, _value}] ->
        get_consumers(group_pid)

      [] ->
        {:error, :not_found}
    end
  end

  @doc """
  Starts a producer for the given topic.

  The broker will assign a unique producer name automatically.
  Returns a single ProducerGroup supervisor PID that manages one or more producer processes.

  ## Parameters

  - `topic` - The topic to publish to (required)
  - `opts` - Optional parameters:
    - `:name` - Custom name for the producer group (default: "<topic>-producer")
    - `:producer_count` - Number of producer processes in the group (default: 1)
    - `:access_mode` - Producer access mode (default: `:Shared`). Available modes:
      - `:Shared` - Multiple producers can publish on the topic
      - `:Exclusive` - Only one producer can publish. Other producers get errors immediately.
      - `:WaitForExclusive` - Wait for exclusive access if another producer is connected
      - `:ExclusiveWithFencing` - Immediately remove any existing producer
    - Other options passed to individual producer processes

  ## Producer Naming and Registry

  All producers are automatically registered in a registry and can be looked up by name:
  - **Default naming**: `"<topic>-producer"`
  - **Custom naming**: Provided via the `:name` option

  This allows you to manage producers by name without keeping track of PIDs.

  ## Producer Access Modes

  Access modes control how many producers can publish to a topic simultaneously:

  - **`:Shared`** (default) - Multiple producers can publish to the same topic
  - **`:Exclusive`** - Only one producer can be connected. If another tries to connect, it fails immediately
  - **`:WaitForExclusive`** - Waits for exclusive access instead of failing
  - **`:ExclusiveWithFencing`** - Takes over by immediately disconnecting the existing producer

  ## Examples

      # Start producer with default settings (1 producer, shared mode)
      iex> {:ok, producer_pid} = Pulsar.start_producer(
      ...>   "persistent://public/default/my-topic"
      ...> )
      {:ok, #PID<0.789.0>}

      # Start producer with exclusive access
      iex> {:ok, producer_pid} = Pulsar.start_producer(
      ...>   "persistent://public/default/my-topic",
      ...>   access_mode: :Exclusive
      ...> )
      {:ok, #PID<0.789.0>}

      # Register with custom name
      iex> {:ok, producer_pid} = Pulsar.start_producer(
      ...>   "persistent://public/default/my-topic",
      ...>   name: "my-producer",
      ...> )
      {:ok, #PID<0.789.0>}
  """
  @spec start_producer(String.t(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start_producer(topic, opts \\ []) do
    client = Keyword.get(opts, :client, @default_client)
    producer_supervisor = Pulsar.Client.producer_supervisor(client)
    {name, producer_opts} = Keyword.pop(opts, :name, "#{topic}-producer")

    case check_partitioned_topic(topic, client) do
      {:ok, 0} ->
        child_spec = %{
          id: name,
          start: {
            Pulsar.ProducerGroup,
            :start_link,
            [name, topic, producer_opts]
          },
          restart: :transient,
          type: :supervisor
        }

        DynamicSupervisor.start_child(producer_supervisor, child_spec)

      {:ok, partitions} when partitions > 0 ->
        child_spec = %{
          id: name,
          start: {
            Pulsar.PartitionedProducer,
            :start_link,
            [name, topic, partitions, producer_opts]
          },
          restart: :transient,
          type: :supervisor
        }

        DynamicSupervisor.start_child(producer_supervisor, child_spec)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Stops a producer group.

  Accepts either:
  - A producer group PID (returned by `start_producer/2`)
  - A producer group ID string (for programmatic access)

  This stops the ProducerGroup supervisor, which automatically stops all producer processes.

  Returns `:ok` if successful, `{:error, :not_found}` if the producer doesn't exist.

  ## Examples

      # Stop producer group
      iex> {:ok, producer_pid} = Pulsar.start_producer(...)
      iex> Pulsar.stop_producer(producer_pid)
      :ok

      # Stop by producer group ID string
      iex> Pulsar.stop_producer("my-topic-producer")
      :ok
  """
  @spec stop_producer(pid() | String.t(), keyword()) :: :ok | {:error, :not_found}
  def stop_producer(group_pid, opts \\ [])

  def stop_producer(group_pid, _opts) when is_pid(group_pid) do
    Supervisor.stop(group_pid)
  end

  def stop_producer(group_pid, opts) when is_binary(group_pid) do
    client = Keyword.get(opts, :client, @default_client)
    producer_registry = Pulsar.Client.producer_registry(client)

    case Registry.lookup(producer_registry, group_pid) do
      [{producer_pid, _value}] ->
        stop_producer(producer_pid)

      [] ->
        {:error, :not_found}
    end
  end

  @doc """
  Looks up a producer group by name.

  Returns `{:ok, producer_pid}` if found, `{:error, :not_found}` otherwise.

  ## Examples

      iex> {:ok, producer_pid} = Pulsar.start_producer("my-topic", name: "my-producer")
      iex> Pulsar.lookup_producer("my-producer")
      {:ok, producer_pid}

      iex> Pulsar.lookup_producer("nonexistent")
      {:error, :not_found}
  """
  @spec lookup_producer(String.t(), keyword()) :: {:ok, pid()} | {:error, :not_found}
  def lookup_producer(name, opts \\ []) do
    client = Keyword.get(opts, :client, @default_client)
    producer_registry = Pulsar.Client.producer_registry(client)

    case Registry.lookup(producer_registry, name) do
      [{producer_pid, _value}] -> {:ok, producer_pid}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Gets all producer processes managed by a producer group.

  Returns a list of producer process PIDs.

  ## Examples

      iex> {:ok, group_pid} = Pulsar.start_producer("my-topic", producer_count: 3)
      iex> Pulsar.get_producers(group_pid)
      [#PID<0.123.0>, #PID<0.124.0>, #PID<0.125.0>]

      # By name
      iex> Pulsar.get_producers("my-topic-producer")
      [#PID<0.123.0>, #PID<0.124.0>, #PID<0.125.0>]
  """
  @spec get_producers(pid() | String.t(), keyword()) :: [pid()] | {:error, :not_found}
  def get_producers(group_pid, opts \\ [])

  def get_producers(group_pid, _opts) when is_pid(group_pid) do
    # Check which type of supervisor this is based on child type
    case Supervisor.which_children(group_pid) do
      [] ->
        []

      [{_id, _, :supervisor, _} | _] ->
        # PartitionedProducer (children are ProducerGroups - supervisors)
        Pulsar.PartitionedProducer.get_producers(group_pid)

      [{_id, _, :worker, _} | _] ->
        # ProducerGroup (children are Producer workers)
        Pulsar.ProducerGroup.get_producers(group_pid)
    end
  end

  def get_producers(group_id, opts) when is_binary(group_id) do
    client = Keyword.get(opts, :client, @default_client)
    producer_registry = Pulsar.Client.producer_registry(client)

    case Registry.lookup(producer_registry, group_id) do
      [{group_pid, _value}] ->
        get_producers(group_pid)

      [] ->
        {:error, :not_found}
    end
  end

  @doc """
  Sends a flow command to request more messages from a consumer.

  This is a convenience wrapper around `Pulsar.Consumer.send_flow/2`.
  Use this when you've disabled automatic flow control by setting `:flow_initial` to 0.

  ## Parameters

  - `consumer` - The consumer process PID or name
  - `permits` - Number of message permits to request

  ## Examples

      # Start consumer with manual flow control
      {:ok, consumer} = Pulsar.start_consumer(
        topic,
        subscription,
        MyCallback,
        flow_initial: 0
      )

      # Request 10 messages
      Pulsar.send_flow(consumer, 10)
  """
  @spec send_flow(pid() | String.t(), non_neg_integer()) :: :ok | {:error, term()}
  def send_flow(consumer, permits) when is_pid(consumer) do
    Pulsar.Consumer.send_flow(consumer, permits)
  end

  def send_flow(consumer_name, permits, opts \\ []) when is_binary(consumer_name) do
    case lookup_consumer(consumer_name, opts) do
      {:ok, consumer_pid} ->
        Pulsar.Consumer.send_flow(consumer_pid, permits)

      {:error, :not_found} ->
        {:error, :consumer_not_found}
    end
  end

  @doc """
  Manually acknowledges one or more messages.

  This is a convenience wrapper around `Pulsar.Consumer.ack/2`.
  Use this when your callback returns `{:noreply, state}` to manually control acknowledgment.
  Supports batching multiple message IDs in a single ACK command for better performance.

  ## Parameters

  - `consumer` - The consumer process PID or name
  - `message_ids` - A single message ID or a list of message IDs to acknowledge

  ## Examples

      # Acknowledge a single message
      Pulsar.ack(consumer_pid, message_id)

      # Acknowledge multiple messages in batch (more efficient)
      Pulsar.ack(consumer_pid, [message_id1, message_id2, message_id3])

      # Can also be used with message structures for Broadway
      def ack(consumer, messages) do
        message_ids = Enum.map(messages, & &1.message_id)
        Pulsar.ack(consumer, message_ids)
      end
  """
  @spec ack(pid() | String.t(), MessageIdData.t() | [MessageIdData.t()], keyword()) :: :ok | {:error, term()}
  def ack(consumer, message_ids, opts \\ [])

  def ack(consumer, message_ids, _opts) when is_pid(consumer) do
    Pulsar.Consumer.ack(consumer, message_ids)
  end

  def ack(consumer_name, message_ids, opts) when is_binary(consumer_name) do
    case lookup_consumer(consumer_name, opts) do
      {:ok, consumer_pid} ->
        Pulsar.Consumer.ack(consumer_pid, message_ids)

      {:error, :not_found} ->
        {:error, :consumer_not_found}
    end
  end

  @doc """
  Manually negatively acknowledges one or more messages.

  This is a convenience wrapper around `Pulsar.Consumer.nack/2`.
  Use this when your callback returns `{:noreply, state}` to manually control acknowledgment.
  Supports batching multiple message IDs in a single NACK for better performance.

  The messages will be tracked for redelivery if `:redelivery_interval` is configured.
  When the messages are redelivered and the redelivery count exceeds `:max_redelivery`,
  they will automatically be sent to the dead letter queue (if `:dead_letter_policy` is configured),
  regardless of whether you use manual or automatic acknowledgment.

  ## Parameters

  - `consumer` - The consumer process PID or name
  - `message_ids` - A single message ID or a list of message IDs to negatively acknowledge

  ## Examples

      # NACK a single message
      Pulsar.nack(consumer_pid, message_id)

      # NACK multiple messages in batch (more efficient)
      Pulsar.nack(consumer_pid, [message_id1, message_id2, message_id3])

      # Can also be used with message structures for Broadway
      def nack(consumer, messages) do
        message_ids = Enum.map(messages, & &1.message_id)
        Pulsar.nack(consumer, message_ids)
      end
  """
  @spec nack(pid() | String.t(), MessageIdData.t() | [MessageIdData.t()], keyword()) :: :ok | {:error, term()}
  def nack(consumer, message_ids, opts \\ [])

  def nack(consumer, message_ids, _opts) when is_pid(consumer) do
    Pulsar.Consumer.nack(consumer, message_ids)
  end

  def nack(consumer_name, message_ids, opts) when is_binary(consumer_name) do
    case lookup_consumer(consumer_name, opts) do
      {:ok, consumer_pid} ->
        Pulsar.Consumer.nack(consumer_pid, message_ids)

      {:error, :not_found} ->
        {:error, :consumer_not_found}
    end
  end

  @doc """
  Sends a message synchronously using a producer group.

  The producer group must be started first using `start_producer/2`.

  ## Parameters

  - `producer_group_name` - Name of the producer group (from start_producer's :name option, or default "topic-producer")
  - `message` - Binary message payload
  - `opts` - Optional parameters:
    - `:timeout` - Timeout in milliseconds (default: 5000)
    - `:partition_key` - Partition routing key (string)
    - `:ordering_key` - Key for ordering in Key_Shared subscriptions (binary)
    - `:properties` - Custom message metadata as a map (e.g., `%{"trace_id" => "abc"}`)
    - `:event_time` - Application event timestamp (DateTime or milliseconds since epoch)
    - `:deliver_at_time` - Absolute delayed delivery time (DateTime or milliseconds since epoch)
    - `:deliver_after` - Relative delayed delivery in milliseconds from now

  ## Return Values

  Returns `{:ok, message_id_data}` on success or `{:error, reason}` on failure.

  ## Examples

      # Basic send
      Pulsar.send(producer, "payload")

      # With message key for partitioning
      Pulsar.send(producer, "payload", key: "user-123")

      # With custom properties
      Pulsar.send(producer, "payload", properties: %{"trace_id" => "abc"})

      # With delayed delivery (60 seconds from now)
      Pulsar.send(producer, "payload", deliver_after: 60_000)

  """
  @spec send(String.t() | pid(), binary(), keyword()) ::
          {:ok, MessageIdData.t()} | {:error, term()}
  def send(producer_group_pid_or_name, message, opts \\ [])

  def send(producer_group_pid, message, opts) when is_pid(producer_group_pid) do
    send_to_producer(producer_group_pid, message, opts)
  end

  def send(producer_group_name, message, opts) when is_binary(message) do
    client = Keyword.get(opts, :client, @default_client)

    case lookup_producer(producer_group_name, client: client) do
      {:ok, group_pid} ->
        send_to_producer(group_pid, message, opts)

      {:error, :not_found} ->
        {:error, :producer_not_found}
    end
  end

  defp send_to_producer(producer_pid, message, opts) do
    case Supervisor.which_children(producer_pid) do
      [{_id, _, :supervisor, _} | _] -> Pulsar.PartitionedProducer.send_message(producer_pid, message, opts)
      _ -> Pulsar.ProducerGroup.send_message(producer_pid, message, opts)
    end
  end

  @spec check_partitioned_topic(String.t(), atom()) :: {:ok, integer()} | {:error, term()}
  defp check_partitioned_topic(_topic, _client, attempts \\ 10, delay_ms \\ 500)

  defp check_partitioned_topic(_topic, _client, 0, _delay_ms) do
    {:error, :partition_check_failed}
  end

  defp check_partitioned_topic(topic, client, attempts, delay_ms) do
    case do_check_partitioned_topic(topic, client) do
      {:ok, response} ->
        {:ok, response}

      _error ->
        Process.sleep(delay_ms)
        check_partitioned_topic(topic, client, attempts - 1, delay_ms)
    end
  end

  @spec do_check_partitioned_topic(String.t(), atom()) ::
          {:ok, integer()} | {:error, term()}
  defp do_check_partitioned_topic(topic, client) do
    broker = Pulsar.Client.random_broker(client)

    case Pulsar.Broker.partitioned_topic_metadata(broker, topic) do
      {:ok, %{response: :Success, partitions: partitions}} ->
        {:ok, partitions}

      {:ok, %{response: :Failed, error: error}} ->
        {:error, {:partition_metadata_check_failed, error}}

      {:error, reason} ->
        Logger.warning("Error checking partitioned topic metadata for #{topic}: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
