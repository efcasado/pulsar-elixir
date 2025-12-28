# Schemas

## What are Schemas?

[Schemas](https://pulsar.apache.org/docs/next/schema-overview/) in Apache Pulsar enable type-safe messaging by defining the structure and type of message data. When a producer registers with a schema, the broker validates it and tracks compatibility as formats evolve.

## Supported Schema Types

**Primitive types** (no definition required):
- `:String`, `:Bool`, `:Int8`, `:Int16`, `:Int32`, `:Int64`, `:Float`, `:Double`, `:None`

**Structured types** (definition required):
- `:Json`, `:Avro`, `:Protobuf`, `:ProtobufNative`, `:KeyValue`

**Temporal types**:
- `:Date`, `:Time`, `:Timestamp`, `:Instant`, `:LocalDate`, `:LocalTime`, `:LocalDateTime`

## Basic Usage

### How to Use Schemas in pulsar-elixir

Schemas are configured as options when starting a producer:

```elixir
{:ok, producer} = Pulsar.start_producer(
  "topic",
  schema: [
    type: :Json,              # required
    definition: schema_def,   # required for structured types
    name: "my-schema",        # optional
    properties: %{}           # optional
  ]
)
```

**Configuration options:**
- **`:type`** - (required) The schema type
- **`:definition`** - Schema definition (required for non-primitive types)
- **`:name`** - Optional schema name for identification
- **`:properties`** - Optional metadata as a map

### String Schema

```elixir
{:ok, producer} = Pulsar.start_producer(
  "logs-topic",
  schema: [type: :String]
)

Pulsar.send(producer, "Application started")
```

### JSON Schema

JSON schemas use Avro record format for the definition:

```elixir
schema_def = Jason.encode!(%{
  type: "record",
  name: "User",
  fields: [
    %{name: "id", type: "int"},
    %{name: "name", type: "string"},
    %{name: "email", type: ["null", "string"], default: nil}
  ]
})

{:ok, producer} = Pulsar.start_producer(
  "users-topic",
  schema: [
    type: :Json,
    definition: schema_def,
    name: "user-schema"
  ]
)

user = %{id: 1, name: "Alice", email: "alice@example.com"}
Pulsar.send(producer, Jason.encode!(user))
```

## Schema Compatibility

The broker enforces compatibility when registering schemas on existing topics.

**Compatible changes:**
- Adding optional fields with defaults

**Incompatible changes:**
- Changing schema type (String → Int32)
- Removing required fields

```elixir
# First producer
{:ok, p1} = Pulsar.start_producer("topic", schema: [type: :String])

# Second producer with different type - FAILS
{:ok, p2} = Pulsar.start_producer("topic", schema: [type: :Int32])
# Producer terminates with :IncompatibleSchema error
```

## Schema Evolution

As your application evolves, you may need to modify schemas. The broker tracks each schema version and ensures new versions remain compatible with old ones. This allows older consumers to continue working while newer producers send enhanced data.

```elixir
# Initial schema: just name
schema_v1 = Jason.encode!(%{
  type: "record",
  name: "User",
  fields: [%{name: "name", type: "string"}]
})

{:ok, producer} = Pulsar.start_producer("users", schema: [type: :Json, definition: schema_v1])

# Later: add optional age field
schema_v2 = Jason.encode!(%{
  type: "record",
  name: "User",
  fields: [
    %{name: "name", type: "string"},
    %{name: "age", type: ["null", "int"], default: nil}
  ]
})

{:ok, producer} = Pulsar.start_producer("users", schema: [type: :Json, definition: schema_v2])
```
