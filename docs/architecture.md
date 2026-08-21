# Architecture

Pulsar's process tree follows the dependencies between a client, its broker connections,
and the consumers and producers that use them. The result is one ownership boundary per
Pulsar cluster and one stable process for each logical resource, even when that resource
spans several partitions and workers.

This guide explains those boundaries, what starts asynchronously, and what applications
can expect when part of the tree restarts.

<!-- Internal modules use <code> tags and are excluded from ExDoc autolinking in mix.exs. -->

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
    ├── brokers
    │   ├── BrokerSupervisor
    │   │   └── broker connection(s) learned through lookup
    │   └── initial broker connection
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
            │           └── producer worker (one per group)
            └── Bootstrap
```

The client-configured broker is a static child of the broker branch. Connections learned
through topic lookup are children of its dynamic broker supervisor. Both kinds register in
the broker registry, which maps service URLs to connection processes. The consumer and
producer registries map application-facing names to stable topology roots; internal
partition groups are not registered as public resources.

Consumer and producer branches are siblings. A failure that rebuilds the consumer branch
does not take runtime producers down with it, and the reverse is also true. If the broker
infrastructure itself must be rebuilt, the resource subtree is later in the dependency
chain and is rebuilt as well. An individual broker connection loss normally affects the
workers using that connection rather than restarting every resource branch.

Each branch also has a <code>Pulsar.Client.Bootstrap</code> process. It registers declared resource
roots before branch startup completes and recreates those declarations when the branch starts
again. Topic discovery and worker initialization remain asynchronous.

## Logical Resources and Stable Roots

Starting a consumer or producer creates one <code>Pulsar.Topology</code> root for its logical topic.
That is the pid returned to the caller, registered under its public name, and returned by
`Pulsar.Client.consumers/1` or `Pulsar.Client.producers/1`.

A non-partitioned topic has one internal <code>Pulsar.Topology.Group</code>. A partitioned topic has
one group per partition. What lives inside each group depends on the resource:

```text
producer topology root (:orders-producer)
├── topology discovery
├── group for partition 0
│   └── producer worker
└── group for partition 1
    └── producer worker

consumer topology root (:orders-billing, consumer_count: 2)
├── topology discovery
├── group for partition 0
│   ├── consumer worker 1
│   └── consumer worker 2
└── group for partition 1
    ├── consumer worker 1
    └── consumer worker 2
```

Producer groups contain one worker, preserving one ordered send lane and one sequence-id and
batching domain per partition. Consumer groups may contain several workers, configured with
`:consumer_count`. Adding partitions changes the children below the root, but not the root
itself. This is why names, stop operations, client listings, and publishing target the logical
resource instead of a particular worker.

The stable root represents that logical resource across worker restarts and broker
reconnections. It remains registered and appears in client listings while operations report
that no workers are available, so the pid applications use to address the resource does not
change while its workers churn.

Every level of the tree is `restart: :permanent`, so an exit means one thing only: something
went wrong, and it is passed upward. A worker that is finished, or that hit something retrying
cannot fix, therefore does not exit. It asks to be retired, and `Pulsar.Topology.Discovery`
terminates it through its parent, which no restart type undoes. Its slot stays `:undefined`,
which is also what tells discovery the partition is accounted for and must not be built again.

Retiring cascades. A group whose workers have all gone is retired from the root, and a root
whose groups have all gone is removed from its client, taking discovery and any companions
with it. One partition of a terminated topic finishing leaves its siblings draining at their
own pace; the last one to finish takes the whole resource away and leaves nothing behind.

Producer publishing resolves the logical root, selects a partition group, and sends through
that group's worker. Consumer workers receive broker messages and invoke the configured
`Pulsar.Consumer.Callback` in the worker process.

## Partitioned and Non-Partitioned Resources

Both shapes are the same resource to a caller. There is one stable root either way, and it is what
names, listings, `stop`, `await_ready` and publishing address. Each unit of work is one group, groups
hold the workers, and retiring cascades identically. <code>Pulsar.Topology.groups/1</code> answers
`{index, pid}` for both — a non-partitioned topology reports its single group at index zero, and a
group passed directly answers itself — so code routing over either shape needs no special case.

The differences all sit below that line.

**Identity.** A non-partitioned group has the child id `{:topic, :non_partitioned}`; a partitioned one
has `{:partition, index}`. Those ids are the tree's memory: reconciliation derives the set of existing
partitions from them with the pid wildcarded, which is how a retired partition reads as accounted for
rather than missing.

**What a worker is told.** A partition worker is started with `:topic` set to its own partition and
`:base_topic` to the topic the resource was configured with, alongside its `:partition` index. A
non-partitioned worker has the two topics equal and `:partition` set to `nil`. A callback can tell
which partition it handles without inspecting the tree it lives in.

**How many workers.** A producer group always holds one worker, preserving a single ordered send lane
and one sequence-id and batching domain per partition. A consumer group holds `:consumer_count` of
them, so a partitioned consumer runs partitions × `:consumer_count` workers in total.

**Discovery.** Metadata answering zero partitions means non-partitioned, and discovery then stops
polling — there is nothing for the topology to grow into. A partitioned topology keeps checking on
`:partition_discovery_interval_ms`. Growth is one-way: Pulsar topics do not shrink, so a lower
transient result never removes groups.

**Mismatches are refused rather than reconciled.** A non-partitioned topology later told it has
partitions answers `{:error, {:incompatible_topology, :non_partitioned, count}}`, and a tree holding
both a non-partitioned group and partition groups answers `{:error, :inconsistent_topology}`. Neither
is repaired in place, because either would mean the topic changed identity underneath a running
resource.

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

For example, publishing by name can return `{:error, :not_found}` before a declared
producer has been registered, and `{:error, :not_ready}` while its topology is initializing.
Applications that publish during startup or from a consumer callback should handle both.
`Pulsar.Consumer.await_ready/2` and `Pulsar.Producer.await_ready/2` provide a bounded wait for
initial topology construction and worker initialization when an application needs a startup
barrier. This readiness is a snapshot: broker availability and worker restarts can still affect
the following operation.

Initial metadata failures are retried with backoff by the discovery process. Resolver also
finds the topic owner when workers connect. Once a partitioned topology is ready, discovery
periodically checks for newly added partitions and adds the missing groups without replacing
the existing ones. Pulsar topics do not shrink, so a lower transient metadata result does not
remove groups.

A retired group is never rebuilt. Its workers finished, or stopped for a reason retrying
cannot change, and its `:undefined` slot is what discovery reads to know the partition is
accounted for. Setting `:partition_discovery_interval_ms` to `false` disables later metadata
checks; initial discovery still runs.

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
- A terminal broker rejection, such as an incompatible schema or an `:exclusive` subscription
  already held, retires that worker rather than restarting it into the same answer. Its group
  is retired once its last worker has gone, and the resource once its last group has. Callers
  then see `{:error, :not_found}` rather than a registered resource with nothing in it.
- A resource that cannot run at all is not left quietly missing; the failure reaches whatever
  supervises the client. See [Error Propagation](#error-propagation).
- A consumer branch failure is isolated from the producer branch, and vice versa.
- A client or branch restart recreates declared resources; runtime resources remain the
  responsibility of their caller.

Registry and broker lookups also account for these restart windows. Public operations return
their documented error tuples when a client, branch, registry, broker, or worker is missing
instead of making the caller exit because an internal process is temporarily unavailable.

## Error Propagation

One rule decides the shape of this: **an exit is a failure**. Anything finished leaves by being
removed from its supervisor, which no restart type undoes and which spends no budget. Every exit that
reaches a supervisor is therefore something going wrong, and it is allowed to travel.

It travels one level at a time, and each level has a budget:

| Level | Absorbs | Budget |
| --- | --- | --- |
| group | worker exits | `:worker_restart_intensity` |
| root | groups that gave up | `:resource_restart_intensity` |
| client branch | resources that gave up | `:resource_restart_intensity` |
| client | branches that gave up | OTP's default |

A worker that crashes is restarted in place. A partition that cannot run exhausts the worker budget
and its group exits. A resource whose partitions keep giving up exhausts the resource budget and its
root exits. A client whose resources keep giving up runs out in turn, and the failure arrives at
whatever supervises the client.

Two things keep that from firing on ordinary trouble.

A broker being away cannot spend the worker budget. <code>Pulsar.Backoff</code> holds a starting
worker for its retry budget before giving up, so a start against an unreachable broker costs seconds
rather than microseconds and an outage produces far fewer restarts than the window allows. A group
that exits has hit something retrying cannot fix.

The two budgets are deliberately far apart. `:worker_restart_intensity` is sized for correlated
failure — a broker dropping every worker registered with it at once. `:resource_restart_intensity` is
small, because it counts events that already mean something is broken. Sharing one number between the
levels would tie escalation to how quickly a failure returns: with the larger budget at both levels,
100 group failures at a 10ms round trip is 112 seconds against a 60 second window, so the level above
never exhausts and the resource loops instead of coming down. Both are configured on `Pulsar.Client`.

Retirement contributes to none of this. A retired worker, group or resource was terminated by its
parent rather than exiting, so it costs no restart and escalates nothing.

## Implementation Notes for Contributors

<code>Pulsar.Topology</code> is the stable root of a logical resource, and
<code>Pulsar.Topology.Group</code> owns the workers for one topic or partition. The stateful
<code>Pulsar.Topology.Discovery</code> process owns discovery status, retry backoff, and polling,
and is the only process that changes the shape of a resource's tree — both reconciliation and
retirement are carried out there, so the two cannot interleave. <code>Pulsar.Topology</code>
supplies those operations and Discovery performs them; the stateless
<code>Pulsar.Topology.Resolver</code> performs broker metadata and owner lookups.
These modules remain behind the Consumer and Producer facades.

After a metadata lookup, Discovery reconciles the root and remembers the resulting partition
count. A pass adds missing partition groups from highest index to lowest. Existing groups are
not replaced, retired groups are left alone, and a lower metadata result never removes
partitions.

Starting higher indexes first lets producer routing treat growth as one transition. Routing
uses the contiguous partition range beginning at zero, so a partial 4-to-6 expansion continues
to use modulus 4 until both new groups exist, then switches directly to modulus 6. Restarting
or stopped groups retain their slots and return an availability error instead of temporarily
moving the key to another partition.

A public name resolves through the client registry to the stable root. The facades classify a
pid from its OTP initial call, read partition identity from group child ids, and traverse only
live consumer or producer workers. Processes may still disappear between those steps, so the
facades translate expected shutdown races into their documented error results.

Discovery and reconciliation logs report topology changes and failures. Their telemetry spans
use `[:pulsar, :topology, :discovery, ...]` and
`[:pulsar, :topology, :reconciliation, ...]`; resolver spans use
`[:pulsar, :topology, :resolver, ...]`. Metadata polling follows
`:partition_discovery_interval_ms`.

## Design Invariants

1. A consumer or producer cannot outlive the client context it depends on.
2. Each logical consumer or producer has one registered stable root, which persists while its
   workers restart and is removed once every one of its groups has been retired.
3. Partitions, groups, and worker pids stay behind the public facade.
4. Starting establishes ownership, not readiness.
5. Consumer and producer failures are isolated from each other.
6. Declared resources are restored automatically; owners restore runtime resources.
7. An exit means a failure and is passed upward. Anything that is finished leaves by being
   removed from its supervisor, never by exiting.
