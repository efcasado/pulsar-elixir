# Upgrading to 4.0

This guide covers upgrading from 3.x to 4.x. The main change is a simpler topology: each logical
consumer or producer has one worker for a non-partitioned topic, or one per partition, directly
under its resource root. Applications upgrading from 2.x should first follow the
[3.0 upgrade guide](upgrading_to_3-0.html).

Update the dependency for the 4.x release:

```elixir
{:pulsar, "~> 4.0", hex: :pulsar_elixir}
```

## 1. Remove `consumer_count`

The option is no longer accepted, including `consumer_count: 1`. Remove it from consumer
configuration and startup calls. If you already used one worker per partition, that completes
the consumer configuration change.

For a count greater than one, replace the single consumer with separately named resources on
the same topic and subscription. For example, this 3.x client declaration:

```elixir
consumers: [
  [topic: topic,
   subscription_name: "order-service",
   callback_module: MyApp.OrderHandler,
   subscription_type: :shared,
   name: :orders,
   consumer_count: 2]
]
```

becomes:

```elixir
consumers: [
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

Each resource now has its own lifecycle. Update code that stops, waits for, or grants flow to
the old consumer so it addresses every replacement resource when appropriate. Client consumer
listings now include each of those resources. Callback state remains independent per worker.

## 2. Review restart budgets

`worker_restart_intensity` now applies to the whole consumer or producer root: its partition
workers, topology controller, and companions share the budget. `resource_restart_intensity`
applies to resource restarts in the client branch; the intermediate partition supervisors are gone.

Both options remain configurable on `Pulsar.Client`, defaulting to three restarts in five seconds.
The worker budget does not automatically scale with partition count. For example, four abnormal
partition-worker exits in five seconds exhaust the default root budget. Tune the count and window
for the number of workers expected to fail together and your desired recovery policy.

Declared resources are recreated after branch or client recovery. Applications remain responsible
for recreating runtime resources after their branch or client is rebuilt. See the
[architecture guide](architecture.html#error-propagation) for recovery details.

## Final checklist

- Remove every `consumer_count` option, even when its value is `1`.
- Replace higher counts with uniquely named consumers on the same subscription.
- Update lifecycle and manual-flow operations to address the replacement resources.
- Review the shared restart budget for both consumers and producers.
