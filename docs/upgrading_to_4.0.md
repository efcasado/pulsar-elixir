# Upgrading to 4.0

This guide is a code-migration checklist for applications upgrading from 3.x. For details about
new features and internal design, follow the links in [Where to look next](#where-to-look-next).

First, update the dependency:

```elixir
{:pulsar, "~> 4.0", hex: :pulsar_elixir}
```

Leaving `:consumer_count` in configuration raises a startup error. Two changes need an explicit
review even after the application starts:

- Replacing a consumer with several named resources changes how you stop, wait for, and grant
  flow to those consumers.
- Restart budgets now cover the whole resource. Review them for the number of partitions and
  your recovery policy.

## 1. Remove `:consumer_count`

4.0 starts one consumer worker for a non-partitioned topic, or one per partition. Remove
`:consumer_count` from consumer configuration and startup calls, including `consumer_count: 1`.
If you omitted the option in 3.x, no consumer configuration change is needed.

For a count greater than one, replace the single consumer with separately named resources on
the same topic and subscription. Update the client's `:consumers` option:

```elixir
# 3.x
consumers = [
  [topic: topic,
   subscription_name: "order-service",
   callback_module: MyApp.OrderHandler,
   subscription_type: :shared,
   name: :orders,
   consumer_count: 2]
]

# 4.0
consumers = [
  [topic: topic,
   subscription_name: "order-service",
   callback_module: MyApp.OrderHandler,
   subscription_type: :shared,
   name: :orders],
  [topic: topic,
   subscription_name: "order-service",
   callback_module: MyApp.OrderHandler,
   subscription_type: :shared,
   name: :orders_2]
]
```

Give every resource a unique name within its client. The same pattern works with `:key_shared`
and with `:failover` for standby consumers. An `:exclusive` subscription admits only one consumer
per partition across all resources.

## 2. Update consumer lifecycle operations

Each named consumer has its own lifecycle. Stopping `:orders` no longer stops the additional
consumer. Update operations that should apply to both:

```elixir
for name <- [:orders, :orders_2] do
  :ok = Pulsar.Consumer.stop(name)
end
```

Likewise, call `Pulsar.Consumer.await_ready/2` for each replacement consumer when both must be
ready. If you grant permits manually, call `Pulsar.Consumer.send_flow/3` for each consumer that
should receive them. Pass the same `:client` option used at startup when using a named client.

`Pulsar.Client.consumers/1` now lists each replacement resource separately. Callback state
remains independent per worker.

## 3. Review restart budgets

`:worker_restart_intensity` now applies to the whole consumer or producer root: its partition
workers, topology controller, and companions share the budget. `:resource_restart_intensity`
applies to resource restarts in the client branch; the intermediate partition supervisors are gone.

Both options remain configurable on `Pulsar.Client`. The worker budget now defaults to ten
restarts in five seconds; the resource budget remains three restarts in five seconds.

The worker budget does not automatically scale with partition count. For three partitions, the
default allows three complete worker restart waves and one additional restart. An eleventh restart
in five seconds exhausts it. Tune the count and window for the number of workers expected to fail
together, including controller and companion restarts, and your desired recovery policy.

Declared resources are recreated after branch or client recovery. Applications remain responsible
for recreating runtime resources after their branch or client is rebuilt.

## Final checklist

- Remove every `consumer_count` option, even when its value is `1`.
- Replace higher counts with uniquely named consumers on the same subscription.
- Update lifecycle and manual-flow operations to address the replacement resources.
- Review the shared restart budget for both consumers and producers.

## Where to look next

- `Pulsar.Client` documents restart budgets and declared resources.
- `Pulsar.Consumer` documents startup, readiness, flow control, and stopping.
- `Pulsar.Consumer.Callback` documents scaling consumers and callback lifecycle events.
- The [architecture guide](architecture.html) covers ownership, startup, and recovery.
- Applications upgrading from 2.x should also follow the [3.0 upgrade guide](upgrading_to_3-0.html).
