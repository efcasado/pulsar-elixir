# Schemas

## What are Schemas?

[Schemas](https://pulsar.apache.org/docs/next/schema-overview/) in Apache Pulsar enable type-safe messaging by defining the structure and type of message data. When producers or consumers register with a schema, the broker validates compatibility and tracks schema versions.

> #### Note {: .info}
>
> This library currently supports schema registration and compatibility validation. However, payload encoding and decoding must be handled manually by the application. Automatic serialization/deserialization may be added in future versions.

## Supported Schema Types

**Primitive types** (no definition required):
- `:String`, `:Bool`, `:Int8`, `:Int16`, `:Int32`, `:Int64`, `:Float`, `:Double`, `:None`

**Structured types** (definition required):
- `:Json`, `:Avro`, `:Protobuf`, `:ProtobufNative`, `:KeyValue`

**Temporal types**:
- `:Date`, `:Time`, `:Timestamp`, `:Instant`, `:LocalDate`, `:LocalTime`, `:LocalDateTime`

## Basic Usage

### Producers

```elixir
{:ok, producer} = Pulsar.Producer.start(
  "topic",
  schema: [
    type: :Json,              # required
    definition: schema_def,   # required for structured types
    name: "my-schema",        # optional
    properties: %{}           # optional
  ]
)

# You must encode payloads manually
user = %{id: 1, name: "Alice"}
Pulsar.Producer.send(producer, Jason.encode!(user))
```

### Consumers

```elixir
{:ok, consumer} = Pulsar.Consumer.start(
  "topic",
  "subscription",
  MyCallback,
  schema: [type: :Json, definition: schema_def] # must match the topic's schema
)

# In your callback, decode payloads manually
def handle_message(%Pulsar.Message{payload: payload}, state) do
  user = Jason.decode!(payload)
  {:ok, state}
end
```

### Supervision tree

```elixir
children = [
  {Pulsar.Client,
   host: "pulsar://localhost:6650",
   producers: [
     [topic: "users",
      name: :user_producer,
      schema: [type: :Json, definition: user_schema_def]]
   ],
   consumers: [
     [topic: "users",
      subscription_name: "my-sub",
      callback_module: MyCallback,
      schema: [type: :Json, definition: user_schema_def]]
   ]}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

**Schema options:**
- **`:type`** - (required) The schema type
- **`:definition`** - Schema definition (required for non-primitive types). For `:Json` and `:Avro`, can be a struct or map (automatically JSON-encoded) or a binary string.
- **`:name`** - Optional schema name
- **`:properties`** - Optional metadata as a map

### Examples

**String schema:**
```elixir
{:ok, producer} = Pulsar.Producer.start("logs", schema: [type: :String])
Pulsar.Producer.send(producer, "Application started")
```

**JSON schema** (uses Avro record format):
```elixir
schema_def = %{
  type: "record",
  name: "User",
  fields: [
    %{name: "id", type: "int"},
    %{name: "name", type: "string"}
  ]
}

{:ok, producer} = Pulsar.Producer.start(
  "users",
  schema: [type: :Json, definition: schema_def]
)

Pulsar.Producer.send(producer, Jason.encode!(%{id: 1, name: "Alice"}))
```

## Compatibility & Evolution

The broker enforces compatibility when registering schemas:

```elixir
# Producer with String schema
{:ok, p1} = Pulsar.Producer.start("topic", schema: [type: :String])

# Different schema type - REJECTED by broker
{:ok, p2} = Pulsar.Producer.start("topic", schema: [type: :Int32])
# Process terminates with {:IncompatibleSchema, ...}
```

**Compatible changes:**
- Adding optional fields with defaults
- Evolving JSON schemas with backward-compatible fields

**Incompatible changes:**
- Changing schema type (String → Int32)
- Removing required fields

The broker tracks schema versions, allowing gradual migration as schemas evolve.
