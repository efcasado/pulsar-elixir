defmodule Pulsar.Schema do
  @moduledoc """
  Schema definition for Pulsar messages.

  Pulsar schemas enable type-safe messaging by defining the structure of message data.
  When a producer connects with a schema, the broker registers it and returns a
  schema version that is included in every message.

  ## Supported Types

  - Primitive: `:None`, `:String`, `:Bool`, `:Int8`, `:Int16`, `:Int32`, `:Int64`, `:Float`, `:Double`
  - Structured: `:Json`, `:Avro`, `:Protobuf`, `:ProtobufNative`, `:KeyValue`
  - Temporal: `:Date`, `:Time`, `:Timestamp`, `:Instant`, `:LocalDate`, `:LocalTime`, `:LocalDateTime`

  See `new/1` for usage examples.
  """

  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary

  @valid_types ~w(
    None String Json Protobuf Avro Bool Int8 Int16 Int32 Int64
    Float Double Date Time Timestamp KeyValue Instant LocalDate
    LocalTime LocalDateTime ProtobufNative AutoConsume
  )a

  @primitive_types ~w(None String Bool Int8 Int16 Int32 Int64 Float Double)a

  @type schema_type :: unquote(Enum.reduce(@valid_types, &{:|, [], [&1, &2]}))

  @type t :: %__MODULE__{
          type: schema_type(),
          definition: binary(),
          name: String.t() | nil,
          properties: map() | nil
        }

  defstruct [:type, :definition, :name, :properties]

  @doc """
  Creates a new schema.

  ## Options

  - `:type` - (required) The schema type (e.g., `:Json`, `:String`, `:Avro`)
  - `:definition` - The schema definition (required for non-primitive types)
  - `:name` - Optional schema name
  - `:properties` - Optional metadata properties as a map

  ## Returns

  - `{:ok, schema}` on success
  - `{:error, reason}` if validation fails

  ## Examples

      # JSON schema (Pulsar uses Avro-style record format)
      {:ok, schema} = Pulsar.Schema.new(
        type: :Json,
        definition: ~s({"type": "record", "name": "User", "fields": [{"name": "id", "type": "int"}]})
      )

      # String schema (primitive, no definition needed)
      {:ok, schema} = Pulsar.Schema.new(type: :String)

      # With properties
      {:ok, schema} = Pulsar.Schema.new(
        type: :Avro,
        definition: avro_schema,
        name: "my-schema",
        properties: %{"version" => "1.0"}
      )

  """
  @spec new(keyword()) :: {:ok, t()} | {:error, term()}
  def new(opts) when is_list(opts) do
    with {:ok, type} <- validate_type(opts[:type]),
         {:ok, definition} <- validate_definition(type, opts[:definition]),
         {:ok, properties} <- validate_properties(opts[:properties]) do
      {:ok, %__MODULE__{type: type, definition: definition, name: opts[:name], properties: properties}}
    end
  end

  @doc """
  Creates a new schema, raising on error.

  Same as `new/1` but raises `ArgumentError` if validation fails.
  """
  @spec new!(keyword()) :: t()
  def new!(opts) when is_list(opts) do
    case new(opts) do
      {:ok, schema} -> schema
      {:error, reason} -> raise ArgumentError, "invalid schema: #{inspect(reason)}"
    end
  end

  @doc """
  Converts a schema to the binary protocol format.

  Returns `nil` if the input is `nil`.
  """
  @spec to_binary(t() | nil) :: Binary.Schema.t() | nil
  def to_binary(nil), do: nil

  def to_binary(%__MODULE__{} = schema) do
    %Binary.Schema{
      name: schema.name || "",
      schema_data: schema.definition || <<>>,
      type: schema.type,
      properties: Pulsar.Protocol.to_key_value_list(schema.properties)
    }
  end

  # Private functions

  defp validate_type(nil), do: {:error, :type_required}
  defp validate_type(type) when type in @valid_types, do: {:ok, type}
  defp validate_type(type), do: {:error, {:invalid_type, type}}

  defp validate_definition(type, def) when type in @primitive_types, do: {:ok, def || <<>>}
  defp validate_definition(_type, nil), do: {:error, :definition_required}
  defp validate_definition(_type, def) when is_binary(def), do: {:ok, def}
  defp validate_definition(_type, def), do: {:error, {:invalid_definition, def}}

  defp validate_properties(nil), do: {:ok, nil}
  defp validate_properties(props) when is_map(props), do: {:ok, props}
  defp validate_properties(props), do: {:error, {:invalid_properties, props}}
end
