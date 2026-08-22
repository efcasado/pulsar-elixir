# Agent guidance

This file contains guidance for coding agents working on `pulsar-elixir`.

For public API usage, start with `README.md` and the module documentation. Before changing clients,
consumers, producers, readers, brokers, topology, or supervision, read `docs/architecture.md`. It is
the source of truth for process ownership, topology lifecycle, recovery, and failure propagation.

## Process model and invariants

`pulsar-elixir` is an OTP application and Apache Pulsar client. Process ownership, restart behavior,
failure propagation, and lifecycle semantics are part of the library's public behavior even when
the modules implementing them are internal.

Preserve these invariants from `docs/architecture.md`:

1. A consumer or producer cannot outlive the client context it depends on.
2. Each logical consumer or producer has one registered stable root, even while it has no live
   workers.
3. Partitions, groups, and worker pids remain behind the public facade.
4. Starting establishes ownership, not readiness.
5. Consumer and producer failures are isolated from each other.
6. Declared resources are restored automatically; callers restore runtime resources.
7. An abnormal worker exit means failure and propagates upward. A deliberate consumer callback
   completion exits normally from a transient worker and remains stopped.

Call out any proposed violation explicitly instead of silently changing the model.

Prefer idiomatic OTP and simple supervision structures over custom lifecycle machinery. Do not add
catches, monitors, retries, or process indirection merely to suppress crashes; identify which
process owns the failure first.

Retry transient failures locally. Let terminal failures exhaust and propagate through the
supervision boundaries documented in `docs/architecture.md`. Do not implicitly change that blast
radius.

Consumer workers are transient. Producer workers and every supervision boundary above workers are
permanent. Do not change restart types, introduce `:significant` children, or enable
`:auto_shutdown` without reconciling the change with stable empty topology roots, graceful callback
completion, and failure propagation.

Before changing child specifications, shutdown reasons, links, monitors, trapped exits, retry
classification, or restart intensity, determine:

- which process owns the child;
- which boundary should recover or escalate;
- whether the failure is transient, terminal, or deliberate completion;
- whether the logical resource must remain addressable;
- whether sibling resources should be affected; and
- whether declared resources must be recreated after recovery.

An exit of `:normal`, `:shutdown`, or `{:shutdown, reason}` is successful completion for a transient
child. Preserve linked exit reasons when trapping exits so OTP can retain that distinction.

## Concurrency and public boundaries

Assume a supervised process can terminate between lookup and use. Code at public facades that
interacts with registries, supervisors, broker connections, topology groups, or workers should turn
expected lifecycle races into the documented public result rather than unexpectedly exiting the
caller. Do not assume a pid is still alive merely because a registry or supervisor just returned it.

Prefer changing or extending `Pulsar.Client`, `Pulsar.Consumer`, `Pulsar.Producer`, and
`Pulsar.Reader` instead of exposing internal topology modules.

When changing public behavior:

- preserve documented return shapes where possible;
- update typespecs and module or function documentation;
- update relevant guides and examples;
- consider compatibility and upgrade implications; and
- add a changelog entry when appropriate for the release process.

Avoid mixing broad refactors into behavioral fixes unless the refactor materially clarifies the
ownership or lifecycle change.

## Implementation conventions

Follow the formatter and existing code style. Prefer explicit ownership, pattern matching, public
error tuples, and existing project abstractions. Do not add an abstraction solely to remove a small
amount of duplication.

Avoid adding runtime dependencies unless they provide clear value that would be difficult or
inappropriate to implement with Elixir/OTP or an existing dependency. Dependency weight and
downstream compatibility matter for a client library.

`lib/pulsar/protocol/binary.ex` is generated from `pulsar.proto`. Do not edit it manually. Use the
protocol-generation tasks in `.mise.toml`.

## Development setup

Tool versions are managed with `mise`:

```bash
mise install
mix deps.get
```

## Verification

At minimum, format the code and run the tests relevant to the change. Tests must compile without
warnings.

Run the static checks used by CI with:

```bash
mix format --check-formatted
MIX_ENV=test mix compile --warnings-as-errors
MIX_ENV=test mix credo --strict
MIX_ENV=test mix dialyzer
```

Run unit tests with:

```bash
mix test --exclude integration --warnings-as-errors
```

Integration tests require Docker and this entry in `/etc/hosts`:

```text
127.0.0.1 broker1 broker2
```

The integration harness starts and stops the Docker Compose services automatically. Run all
integration tests with:

```bash
mix test --only integration --max-cases 2 --warnings-as-errors
```

While iterating, run the relevant integration directory, for example:

```bash
mix test test/integration/topology --max-cases 2 --warnings-as-errors
mix test test/integration/consumer --max-cases 2 --warnings-as-errors
mix test test/integration/producer --max-cases 2 --warnings-as-errors
```

Add or update tests for behavioral changes. Prefer externally observable behavior over internal
tree assertions unless the tree itself is the behavior under test. For OTP and supervision changes,
consider startup and shutdown, unexpected child termination, restart exhaustion, transient versus
terminal errors, lookup races, branch or client restarts, stable logical identity, and isolation
between consumer and producer branches.

Avoid tests whose correctness depends mainly on arbitrary sleeps. Prefer messages, monitors,
telemetry, readiness functions, or other observable synchronization.

## Repository map

- `docs/architecture.md` documents process ownership and lifecycle semantics.
- `docs/` contains public feature and upgrade guides.
- `test/unit/` contains isolated tests; `test/integration/` exercises real Pulsar services.
- `test/support/` contains test-only helpers.
- `examples/` contains example applications and usage.
- `bench/` contains benchmarks.

Keep this file focused on contributor and agent instructions. If ownership or lifecycle semantics
change, update `docs/architecture.md` and retain only the corresponding high-level invariant here.
