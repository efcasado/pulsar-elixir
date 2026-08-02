# Architecture

Pulsar's process tree follows the dependencies between a client, its broker connections,
and the consumers and producers that use them. The result is one ownership boundary per
Pulsar cluster and one stable process for each logical resource, even when that resource
spans several partitions and workers.

This guide explains those boundaries, what starts asynchronously, and what applications
can expect when part of the tree restarts.

## Why Does Ownership Matter?

A consumer or producer cannot work without the connection context provided by a
`Pulsar.Client`. It therefore belongs below that client, rather than beside it in the host
application's supervision tree:

```elixir
children = [
  {Pulsar.Client,
   name: :events,
   host: "pulsar://localhost:6650",
   producers: [
     [topic: "persistent://public/default/audit", name: :audit]
   ],
   consumers: [
     [topic: "persistent://public/default/orders",
      subscription_name: "order-service",
      callback_module: MyApp.OrderHandler]
   ]}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

The client owns the registries used to resolve names and the broker processes used for
topic lookup. Keeping dependent resources below it prevents them from surviving a client
restart with stale registrations or connections.

Several clients can run at once. Each one has its own broker connections, registries,
consumers, producers, and configuration, so a resource belonging to `:events` does not
share runtime state with one belonging to `:analytics`.

## Public Boundaries

Applications work through four public modules:

| Module | Responsibility |
| --- | --- |
| `Pulsar.Client` | Owns one connection context and the resources using it |
| `Pulsar.Consumer` | Starts, stops, and controls logical consumers |
| `Pulsar.Producer` | Starts, stops, and publishes through logical producers |
| `Pulsar.Reader` | Provides stream-based reading through a temporary consumer |

Logical-resource operations use a registered name or the stable pid returned when a resource
starts, as documented by each function. The facades do not expose registry lookup, partition
counts, groups, or worker enumeration. Those are topology details and can change while the
logical resource remains the same.

Acknowledgement is the exception. A message contains the broker-side id of the worker that
received it, so `Pulsar.Consumer.ack/2` and `Pulsar.Consumer.nack/2` require that worker pid.
A callback can capture `self()` and pass it, together with the message id, to asynchronous
work.

## The Client Tree

A client starts the following ownership tree:

```text
MyApp.Supervisor
└── Pulsar.Client
    ├── BrokerRegistry
    ├── BrokerSupervisor
    │   └── broker connection(s)
    └── resources
        ├── consumers
        │   ├── ConsumerRegistry
        │   ├── ConsumerSupervisor
        │   │   └── stable consumer root(s)
        │   │       ├── topology discovery
        │   │       └── partition group(s)
        │   │           └── consumer worker(s)
        │   └── Bootstrap
        └── producers
            ├── ProducerRegistry
            ├── ProducerSupervisor
            │   └── stable producer root(s)
            │       ├── topology discovery
            │       └── partition group(s)
            │           └── producer worker(s)
            └── Bootstrap
```

The broker registry maps service URLs to connection processes. The consumer and producer
registries map application-facing names to stable topology roots; internal partition groups
are not registered as public resources.

Consumer and producer branches are siblings. A failure that rebuilds the consumer branch
does not take runtime producers down with it, and the reverse is also true. If the broker
infrastructure itself must be rebuilt, the resource subtree is later in the dependency
chain and is rebuilt as well. An individual broker connection loss normally affects the
workers using that connection rather than restarting every resource branch.

Each branch also has a <code>Pulsar.Client.Bootstrap</code> process. It starts resources declared on
the client after the client tree is available and recreates those declarations when the
branch starts again.

## Logical Resources and Stable Roots

Starting a consumer or producer creates one <code>Pulsar.Topology</code> root for its logical topic.
That is the pid returned to the caller, registered under its public name, and returned by
`Pulsar.Client.consumers/1` or `Pulsar.Client.producers/1`.

A non-partitioned topic has one internal <code>Pulsar.Topology.Group</code>. A partitioned topic has
one group per partition:

```text
stable topology root (:orders)
├── topology discovery
├── group for partition 0
│   ├── worker 1
│   └── worker 2
├── group for partition 1
│   ├── worker 1
│   └── worker 2
└── group for partition 2
    ├── worker 1
    └── worker 2
```

The number of workers in each group comes from `:consumer_count` or `:producer_count`.
Adding partitions changes the children below the root, but not the root itself. This is why
names, stop operations, client listings, and publishing target the logical resource instead
of a particular worker.

Producer publishing resolves the logical root, selects a partition group, and sends through
one of that group's workers. Consumer workers receive broker messages and invoke the
configured `Pulsar.Consumer.Callback` in the worker process.

## Startup Is Asynchronous

Starting a client or resource establishes ownership; it is not a readiness check. Resource
startup proceeds in stages:

1. The client starts its registries and supervisors.
2. A consumer or producer registers its stable topology root.
3. <code>Pulsar.Topology.Discovery</code> asks <code>Pulsar.Topology.Resolver</code> for partition metadata.
4. The topology creates the required groups and workers.
5. Workers resolve the topic broker and register or subscribe.

The public `start` call returns after step 2. This keeps broker availability and metadata
lookups out of the host application's startup path, but it means operations can observe the
resource between registration and readiness.

For example, publishing by name can return `{:error, :producer_not_found}` before a declared
producer has been registered, and `{:error, :not_ready}` while its topology is initializing.
Applications that publish during startup or from a consumer callback should handle both.

Initial metadata failures are retried with backoff by the discovery process. Resolver also
finds the topic owner when workers connect. Once a partitioned topology is ready, discovery
periodically checks for newly added partitions and adds the missing groups without replacing
the existing ones. Pulsar topics do not shrink, so a lower transient metadata result does not
remove groups.

`Pulsar.Reader` builds on this lifecycle. Each enumeration creates a temporary non-durable
consumer below the selected client, waits internally for the expected workers to become
ready, and then exposes their messages as a stream. Halting the stream or failing startup
removes that temporary consumer; the client remains running.

## Declared and Runtime Resources

Resources can enter a client in two ways:

| Kind | How it starts | After a branch or client restart |
| --- | --- | --- |
| Declared | In the client's `:consumers` or `:producers` options | Recreated by Bootstrap |
| Runtime | Through `Pulsar.Consumer.start/1` or `Pulsar.Producer.start/1` | The caller must restore it |

Declared resources are appropriate when the set is known when the host supervision tree is
built. Runtime resources are useful for dynamic sets, such as one consumer per tenant, but
their owner must recreate them after the client or their resource branch restarts.

Stopping either kind through `Pulsar.Consumer.stop/2` or `Pulsar.Producer.stop/2` asks its
owning supervisor to remove it. The same API also handles a resource started directly, so
callers do not need to know which supervisor owns the root.

## Failure and Recovery

Recovery happens at the narrowest useful boundary:

- An unexpected worker failure is restarted inside its group.
- A broker connection loss restarts the workers that depended on it while the client remains
  available.
- A terminal broker rejection, such as an incompatible schema, is not retried forever. A
  group with no viable workers shuts down; if no significant groups remain, the logical root
  shuts down as well.
- A consumer branch failure is isolated from the producer branch, and vice versa.
- A client or branch restart recreates declared resources; runtime resources remain the
  responsibility of their caller.

Registry and broker lookups also account for these restart windows. Public operations return
their documented error tuples when a client, branch, registry, broker, or worker is missing
instead of making the caller exit because an internal process is temporarily unavailable.

## Internal Modules

The shared topology implementation removes separate group and partitioned supervisors for
consumers and producers:

- <code>Pulsar.Topology</code> owns one logical resource;
- <code>Pulsar.Topology.Discovery</code> initializes and reconciles its partitions;
- <code>Pulsar.Topology.Resolver</code> resolves topic owners and partition metadata;
- <code>Pulsar.Topology.Group</code> owns the workers for one topic or partition;
- <code>Pulsar.Consumer.Worker</code> and <code>Pulsar.Producer.Worker</code> implement broker-facing behavior;
- <code>Pulsar.Backoff</code> provides the common retry policy.

These modules are implementation details rather than additional application-facing APIs.
Keeping traversal inside this layer lets the public Consumer and Producer facades remain
stable if the supervision shape changes.

## Architectural Guarantees

The design is built around a small set of guarantees:

1. A consumer or producer cannot outlive the client context it depends on.
2. Each logical consumer or producer has one registered stable root.
3. Partitions, groups, and worker pids stay behind the public facade.
4. Starting establishes ownership, not readiness.
5. Consumer and producer failures are isolated from each other.
6. Declared resources are restored automatically; owners restore runtime resources.

These guarantees are the durable contract. The exact processes below each stable root may
evolve without requiring applications to change how they start, address, or stop resources.
