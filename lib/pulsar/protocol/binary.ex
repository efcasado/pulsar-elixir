defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CompressionType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:NONE, 0)
  field(:LZ4, 1)
  field(:ZLIB, 2)
  field(:ZSTD, 3)
  field(:SNAPPY, 4)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.ProducerAccessMode do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:Shared, 0)
  field(:Exclusive, 1)
  field(:WaitForExclusive, 2)
  field(:ExclusiveWithFencing, 3)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.ServerError do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:UnknownError, 0)
  field(:MetadataError, 1)
  field(:PersistenceError, 2)
  field(:AuthenticationError, 3)
  field(:AuthorizationError, 4)
  field(:ConsumerBusy, 5)
  field(:ServiceNotReady, 6)
  field(:ProducerBlockedQuotaExceededError, 7)
  field(:ProducerBlockedQuotaExceededException, 8)
  field(:ChecksumError, 9)
  field(:UnsupportedVersionError, 10)
  field(:TopicNotFound, 11)
  field(:SubscriptionNotFound, 12)
  field(:ConsumerNotFound, 13)
  field(:TooManyRequests, 14)
  field(:TopicTerminatedError, 15)
  field(:ProducerBusy, 16)
  field(:InvalidTopicName, 17)
  field(:IncompatibleSchema, 18)
  field(:ConsumerAssignError, 19)
  field(:TransactionCoordinatorNotFound, 20)
  field(:InvalidTxnStatus, 21)
  field(:NotAllowedError, 22)
  field(:TransactionConflict, 23)
  field(:TransactionNotFound, 24)
  field(:ProducerFenced, 25)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.AuthMethod do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:AuthMethodNone, 0)
  field(:AuthMethodYcaV1, 1)
  field(:AuthMethodAthens, 2)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.ProtocolVersion do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:v0, 0)
  field(:v1, 1)
  field(:v2, 2)
  field(:v3, 3)
  field(:v4, 4)
  field(:v5, 5)
  field(:v6, 6)
  field(:v7, 7)
  field(:v8, 8)
  field(:v9, 9)
  field(:v10, 10)
  field(:v11, 11)
  field(:v12, 12)
  field(:v13, 13)
  field(:v14, 14)
  field(:v15, 15)
  field(:v16, 16)
  field(:v17, 17)
  field(:v18, 18)
  field(:v19, 19)
  field(:v20, 20)
  field(:v21, 21)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.KeySharedMode do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:AUTO_SPLIT, 0)
  field(:STICKY, 1)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.TxnAction do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:COMMIT, 0)
  field(:ABORT, 1)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.Schema.Type do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:None, 0)
  field(:String, 1)
  field(:Json, 2)
  field(:Protobuf, 3)
  field(:Avro, 4)
  field(:Bool, 5)
  field(:Int8, 6)
  field(:Int16, 7)
  field(:Int32, 8)
  field(:Int64, 9)
  field(:Float, 10)
  field(:Double, 11)
  field(:Date, 12)
  field(:Time, 13)
  field(:Timestamp, 14)
  field(:KeyValue, 15)
  field(:Instant, 16)
  field(:LocalDate, 17)
  field(:LocalTime, 18)
  field(:LocalDateTime, 19)
  field(:ProtobufNative, 20)
  field(:AutoConsume, 21)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandSubscribe.SubType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:Exclusive, 0)
  field(:Shared, 1)
  field(:Failover, 2)
  field(:Key_Shared, 3)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandSubscribe.InitialPosition do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:Latest, 0)
  field(:Earliest, 1)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandPartitionedTopicMetadataResponse.LookupType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:Success, 0)
  field(:Failed, 1)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandLookupTopicResponse.LookupType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:Redirect, 0)
  field(:Connect, 1)
  field(:Failed, 2)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandAck.AckType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:Individual, 0)
  field(:Cumulative, 1)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandAck.ValidationError do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:UncompressedSizeCorruption, 0)
  field(:DecompressionError, 1)
  field(:ChecksumMismatch, 2)
  field(:BatchDeSerializeError, 3)
  field(:DecryptionError, 4)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandTopicMigrated.ResourceType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:Producer, 0)
  field(:Consumer, 1)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetTopicsOfNamespace.Mode do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:PERSISTENT, 0)
  field(:NON_PERSISTENT, 1)
  field(:ALL, 2)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.BaseCommand.Type do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:CONNECT, 2)
  field(:CONNECTED, 3)
  field(:SUBSCRIBE, 4)
  field(:PRODUCER, 5)
  field(:SEND, 6)
  field(:SEND_RECEIPT, 7)
  field(:SEND_ERROR, 8)
  field(:MESSAGE, 9)
  field(:ACK, 10)
  field(:FLOW, 11)
  field(:UNSUBSCRIBE, 12)
  field(:SUCCESS, 13)
  field(:ERROR, 14)
  field(:CLOSE_PRODUCER, 15)
  field(:CLOSE_CONSUMER, 16)
  field(:PRODUCER_SUCCESS, 17)
  field(:PING, 18)
  field(:PONG, 19)
  field(:REDELIVER_UNACKNOWLEDGED_MESSAGES, 20)
  field(:PARTITIONED_METADATA, 21)
  field(:PARTITIONED_METADATA_RESPONSE, 22)
  field(:LOOKUP, 23)
  field(:LOOKUP_RESPONSE, 24)
  field(:CONSUMER_STATS, 25)
  field(:CONSUMER_STATS_RESPONSE, 26)
  field(:REACHED_END_OF_TOPIC, 27)
  field(:SEEK, 28)
  field(:GET_LAST_MESSAGE_ID, 29)
  field(:GET_LAST_MESSAGE_ID_RESPONSE, 30)
  field(:ACTIVE_CONSUMER_CHANGE, 31)
  field(:GET_TOPICS_OF_NAMESPACE, 32)
  field(:GET_TOPICS_OF_NAMESPACE_RESPONSE, 33)
  field(:GET_SCHEMA, 34)
  field(:GET_SCHEMA_RESPONSE, 35)
  field(:AUTH_CHALLENGE, 36)
  field(:AUTH_RESPONSE, 37)
  field(:ACK_RESPONSE, 38)
  field(:GET_OR_CREATE_SCHEMA, 39)
  field(:GET_OR_CREATE_SCHEMA_RESPONSE, 40)
  field(:NEW_TXN, 50)
  field(:NEW_TXN_RESPONSE, 51)
  field(:ADD_PARTITION_TO_TXN, 52)
  field(:ADD_PARTITION_TO_TXN_RESPONSE, 53)
  field(:ADD_SUBSCRIPTION_TO_TXN, 54)
  field(:ADD_SUBSCRIPTION_TO_TXN_RESPONSE, 55)
  field(:END_TXN, 56)
  field(:END_TXN_RESPONSE, 57)
  field(:END_TXN_ON_PARTITION, 58)
  field(:END_TXN_ON_PARTITION_RESPONSE, 59)
  field(:END_TXN_ON_SUBSCRIPTION, 60)
  field(:END_TXN_ON_SUBSCRIPTION_RESPONSE, 61)
  field(:TC_CLIENT_CONNECT_REQUEST, 62)
  field(:TC_CLIENT_CONNECT_RESPONSE, 63)
  field(:WATCH_TOPIC_LIST, 64)
  field(:WATCH_TOPIC_LIST_SUCCESS, 65)
  field(:WATCH_TOPIC_UPDATE, 66)
  field(:WATCH_TOPIC_LIST_CLOSE, 67)
  field(:TOPIC_MIGRATED, 68)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.Schema do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:name, 1, required: true, type: :string)
  field(:schema_data, 3, required: true, type: :bytes)

  field(:type, 4,
    required: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.Schema.Type,
    enum: true
  )

  field(:properties, 5, repeated: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.KeyValue)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:ledgerId, 1, required: true, type: :uint64)
  field(:entryId, 2, required: true, type: :uint64)
  field(:partition, 3, optional: true, type: :int32, default: -1)
  field(:batch_index, 4, optional: true, type: :int32, default: -1)
  field(:ack_set, 5, repeated: true, type: :int64)
  field(:batch_size, 6, optional: true, type: :int32)

  field(:first_chunk_message_id, 7,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
  )
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.KeyValue do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:key, 1, required: true, type: :string)
  field(:value, 2, required: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.KeyLongValue do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:key, 1, required: true, type: :string)
  field(:value, 2, required: true, type: :uint64)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.IntRange do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:start, 1, required: true, type: :int32)
  field(:end, 2, required: true, type: :int32)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.EncryptionKeys do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:key, 1, required: true, type: :string)
  field(:value, 2, required: true, type: :bytes)
  field(:metadata, 3, repeated: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.KeyValue)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.MessageMetadata do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:producer_name, 1, required: true, type: :string)
  field(:sequence_id, 2, required: true, type: :uint64)
  field(:publish_time, 3, required: true, type: :uint64)
  field(:properties, 4, repeated: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.KeyValue)
  field(:replicated_from, 5, optional: true, type: :string)
  field(:partition_key, 6, optional: true, type: :string)
  field(:replicate_to, 7, repeated: true, type: :string)

  field(:compression, 8,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CompressionType,
    default: :NONE,
    enum: true
  )

  field(:uncompressed_size, 9, optional: true, type: :uint32, default: 0)
  field(:num_messages_in_batch, 11, optional: true, type: :int32, default: 1)
  field(:event_time, 12, optional: true, type: :uint64, default: 0)

  field(:encryption_keys, 13,
    repeated: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.EncryptionKeys
  )

  field(:encryption_algo, 14, optional: true, type: :string)
  field(:encryption_param, 15, optional: true, type: :bytes)
  field(:schema_version, 16, optional: true, type: :bytes)
  field(:partition_key_b64_encoded, 17, optional: true, type: :bool, default: false)
  field(:ordering_key, 18, optional: true, type: :bytes)
  field(:deliver_at_time, 19, optional: true, type: :int64)
  field(:marker_type, 20, optional: true, type: :int32)
  field(:txnid_least_bits, 22, optional: true, type: :uint64)
  field(:txnid_most_bits, 23, optional: true, type: :uint64)
  field(:highest_sequence_id, 24, optional: true, type: :uint64, default: 0)
  field(:null_value, 25, optional: true, type: :bool, default: false)
  field(:uuid, 26, optional: true, type: :string)
  field(:num_chunks_from_msg, 27, optional: true, type: :int32)
  field(:total_chunk_msg_size, 28, optional: true, type: :int32)
  field(:chunk_id, 29, optional: true, type: :int32)
  field(:null_partition_key, 30, optional: true, type: :bool, default: false)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.SingleMessageMetadata do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:properties, 1, repeated: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.KeyValue)
  field(:partition_key, 2, optional: true, type: :string)
  field(:payload_size, 3, required: true, type: :int32)
  field(:compacted_out, 4, optional: true, type: :bool, default: false)
  field(:event_time, 5, optional: true, type: :uint64, default: 0)
  field(:partition_key_b64_encoded, 6, optional: true, type: :bool, default: false)
  field(:ordering_key, 7, optional: true, type: :bytes)
  field(:sequence_id, 8, optional: true, type: :uint64)
  field(:null_value, 9, optional: true, type: :bool, default: false)
  field(:null_partition_key, 10, optional: true, type: :bool, default: false)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.BrokerEntryMetadata do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:broker_timestamp, 1, optional: true, type: :uint64)
  field(:index, 2, optional: true, type: :uint64)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandConnect do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:client_version, 1, required: true, type: :string)

  field(:auth_method, 2,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.AuthMethod,
    enum: true
  )

  field(:auth_method_name, 5, optional: true, type: :string)
  field(:auth_data, 3, optional: true, type: :bytes)
  field(:protocol_version, 4, optional: true, type: :int32, default: 0)
  field(:proxy_to_broker_url, 6, optional: true, type: :string)
  field(:original_principal, 7, optional: true, type: :string)
  field(:original_auth_data, 8, optional: true, type: :string)
  field(:original_auth_method, 9, optional: true, type: :string)

  field(:feature_flags, 10,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.FeatureFlags
  )

  field(:proxy_version, 11, optional: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.FeatureFlags do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:supports_auth_refresh, 1, optional: true, type: :bool, default: false)
  field(:supports_broker_entry_metadata, 2, optional: true, type: :bool, default: false)
  field(:supports_partial_producer, 3, optional: true, type: :bool, default: false)
  field(:supports_topic_watchers, 4, optional: true, type: :bool, default: false)

  field(:supports_get_partitioned_metadata_without_auto_creation, 5,
    optional: true,
    type: :bool,
    default: false
  )
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandConnected do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:server_version, 1, required: true, type: :string)
  field(:protocol_version, 2, optional: true, type: :int32, default: 0)
  field(:max_message_size, 3, optional: true, type: :int32)
  field(:feature_flags, 4, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.FeatureFlags)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandAuthResponse do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:client_version, 1, optional: true, type: :string)
  field(:response, 2, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.AuthData)
  field(:protocol_version, 3, optional: true, type: :int32, default: 0)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandAuthChallenge do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:server_version, 1, optional: true, type: :string)
  field(:challenge, 2, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.AuthData)
  field(:protocol_version, 3, optional: true, type: :int32, default: 0)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.AuthData do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:auth_method_name, 1, optional: true, type: :string)
  field(:auth_data, 2, optional: true, type: :bytes)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.KeySharedMeta do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:keySharedMode, 1,
    required: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.KeySharedMode,
    enum: true
  )

  field(:hashRanges, 3, repeated: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.IntRange)
  field(:allowOutOfOrderDelivery, 4, optional: true, type: :bool, default: false)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandSubscribe do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:topic, 1, required: true, type: :string)
  field(:subscription, 2, required: true, type: :string)

  field(:subType, 3,
    required: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandSubscribe.SubType,
    enum: true
  )

  field(:consumer_id, 4, required: true, type: :uint64)
  field(:request_id, 5, required: true, type: :uint64)
  field(:consumer_name, 6, optional: true, type: :string)
  field(:priority_level, 7, optional: true, type: :int32)
  field(:durable, 8, optional: true, type: :bool, default: true)

  field(:start_message_id, 9,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
  )

  field(:metadata, 10, repeated: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.KeyValue)
  field(:read_compacted, 11, optional: true, type: :bool)
  field(:schema, 12, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.Schema)

  field(:initialPosition, 13,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandSubscribe.InitialPosition,
    default: :Latest,
    enum: true
  )

  field(:replicate_subscription_state, 14, optional: true, type: :bool)
  field(:force_topic_creation, 15, optional: true, type: :bool, default: true)
  field(:start_message_rollback_duration_sec, 16, optional: true, type: :uint64, default: 0)

  field(:keySharedMeta, 17,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.KeySharedMeta
  )

  field(:subscription_properties, 18,
    repeated: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.KeyValue
  )

  field(:consumer_epoch, 19, optional: true, type: :uint64)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandPartitionedTopicMetadata do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:topic, 1, required: true, type: :string)
  field(:request_id, 2, required: true, type: :uint64)
  field(:original_principal, 3, optional: true, type: :string)
  field(:original_auth_data, 4, optional: true, type: :string)
  field(:original_auth_method, 5, optional: true, type: :string)
  field(:metadata_auto_creation_enabled, 6, optional: true, type: :bool, default: true)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandPartitionedTopicMetadataResponse do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:partitions, 1, optional: true, type: :uint32)
  field(:request_id, 2, required: true, type: :uint64)

  field(:response, 3,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandPartitionedTopicMetadataResponse.LookupType,
    enum: true
  )

  field(:error, 4,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.ServerError,
    enum: true
  )

  field(:message, 5, optional: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandLookupTopic do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:topic, 1, required: true, type: :string)
  field(:request_id, 2, required: true, type: :uint64)
  field(:authoritative, 3, optional: true, type: :bool, default: false)
  field(:original_principal, 4, optional: true, type: :string)
  field(:original_auth_data, 5, optional: true, type: :string)
  field(:original_auth_method, 6, optional: true, type: :string)
  field(:advertised_listener_name, 7, optional: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandLookupTopicResponse do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:brokerServiceUrl, 1, optional: true, type: :string)
  field(:brokerServiceUrlTls, 2, optional: true, type: :string)

  field(:response, 3,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandLookupTopicResponse.LookupType,
    enum: true
  )

  field(:request_id, 4, required: true, type: :uint64)
  field(:authoritative, 5, optional: true, type: :bool, default: false)

  field(:error, 6,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.ServerError,
    enum: true
  )

  field(:message, 7, optional: true, type: :string)
  field(:proxy_through_service_url, 8, optional: true, type: :bool, default: false)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandProducer do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:topic, 1, required: true, type: :string)
  field(:producer_id, 2, required: true, type: :uint64)
  field(:request_id, 3, required: true, type: :uint64)
  field(:producer_name, 4, optional: true, type: :string)
  field(:encrypted, 5, optional: true, type: :bool, default: false)
  field(:metadata, 6, repeated: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.KeyValue)
  field(:schema, 7, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.Schema)
  field(:epoch, 8, optional: true, type: :uint64, default: 0)
  field(:user_provided_producer_name, 9, optional: true, type: :bool, default: true)

  field(:producer_access_mode, 10,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.ProducerAccessMode,
    default: :Shared,
    enum: true
  )

  field(:topic_epoch, 11, optional: true, type: :uint64)
  field(:txn_enabled, 12, optional: true, type: :bool, default: false)
  field(:initial_subscription_name, 13, optional: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandSend do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:producer_id, 1, required: true, type: :uint64)
  field(:sequence_id, 2, required: true, type: :uint64)
  field(:num_messages, 3, optional: true, type: :int32, default: 1)
  field(:txnid_least_bits, 4, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 5, optional: true, type: :uint64, default: 0)
  field(:highest_sequence_id, 6, optional: true, type: :uint64, default: 0)
  field(:is_chunk, 7, optional: true, type: :bool, default: false)
  field(:marker, 8, optional: true, type: :bool, default: false)
  field(:message_id, 9, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandSendReceipt do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:producer_id, 1, required: true, type: :uint64)
  field(:sequence_id, 2, required: true, type: :uint64)
  field(:message_id, 3, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData)
  field(:highest_sequence_id, 4, optional: true, type: :uint64, default: 0)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandSendError do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:producer_id, 1, required: true, type: :uint64)
  field(:sequence_id, 2, required: true, type: :uint64)

  field(:error, 3,
    required: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.ServerError,
    enum: true
  )

  field(:message, 4, required: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandMessage do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:message_id, 2, required: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData)
  field(:redelivery_count, 3, optional: true, type: :uint32, default: 0)
  field(:ack_set, 4, repeated: true, type: :int64)
  field(:consumer_epoch, 5, optional: true, type: :uint64)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandAck do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:consumer_id, 1, required: true, type: :uint64)

  field(:ack_type, 2,
    required: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandAck.AckType,
    enum: true
  )

  field(:message_id, 3, repeated: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData)

  field(:validation_error, 4,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandAck.ValidationError,
    enum: true
  )

  field(:properties, 5, repeated: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.KeyLongValue)
  field(:txnid_least_bits, 6, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 7, optional: true, type: :uint64, default: 0)
  field(:request_id, 8, optional: true, type: :uint64)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandAckResponse do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)

  field(:error, 4,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.ServerError,
    enum: true
  )

  field(:message, 5, optional: true, type: :string)
  field(:request_id, 6, optional: true, type: :uint64)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandActiveConsumerChange do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:is_active, 2, optional: true, type: :bool, default: false)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandFlow do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:messagePermits, 2, required: true, type: :uint32)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandUnsubscribe do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:request_id, 2, required: true, type: :uint64)
  field(:force, 3, optional: true, type: :bool, default: false)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandSeek do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:request_id, 2, required: true, type: :uint64)
  field(:message_id, 3, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData)
  field(:message_publish_time, 4, optional: true, type: :uint64)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandReachedEndOfTopic do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:consumer_id, 1, required: true, type: :uint64)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandTopicMigrated do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:resource_id, 1, required: true, type: :uint64)

  field(:resource_type, 2,
    required: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandTopicMigrated.ResourceType,
    enum: true
  )

  field(:brokerServiceUrl, 3, optional: true, type: :string)
  field(:brokerServiceUrlTls, 4, optional: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandCloseProducer do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:producer_id, 1, required: true, type: :uint64)
  field(:request_id, 2, required: true, type: :uint64)
  field(:assignedBrokerServiceUrl, 3, optional: true, type: :string)
  field(:assignedBrokerServiceUrlTls, 4, optional: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandCloseConsumer do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:request_id, 2, required: true, type: :uint64)
  field(:assignedBrokerServiceUrl, 3, optional: true, type: :string)
  field(:assignedBrokerServiceUrlTls, 4, optional: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandRedeliverUnacknowledgedMessages do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:message_ids, 2, repeated: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData)
  field(:consumer_epoch, 3, optional: true, type: :uint64)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandSuccess do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:schema, 2, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.Schema)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandProducerSuccess do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:producer_name, 2, required: true, type: :string)
  field(:last_sequence_id, 3, optional: true, type: :int64, default: -1)
  field(:schema_version, 4, optional: true, type: :bytes)
  field(:topic_epoch, 5, optional: true, type: :uint64)
  field(:producer_ready, 6, optional: true, type: :bool, default: true)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandError do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)

  field(:error, 2,
    required: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.ServerError,
    enum: true
  )

  field(:message, 3, required: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandPing do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandPong do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandConsumerStats do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:consumer_id, 4, required: true, type: :uint64)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandConsumerStatsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)

  field(:error_code, 2,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.ServerError,
    enum: true
  )

  field(:error_message, 3, optional: true, type: :string)
  field(:msgRateOut, 4, optional: true, type: :double)
  field(:msgThroughputOut, 5, optional: true, type: :double)
  field(:msgRateRedeliver, 6, optional: true, type: :double)
  field(:consumerName, 7, optional: true, type: :string)
  field(:availablePermits, 8, optional: true, type: :uint64)
  field(:unackedMessages, 9, optional: true, type: :uint64)
  field(:blockedConsumerOnUnackedMsgs, 10, optional: true, type: :bool)
  field(:address, 11, optional: true, type: :string)
  field(:connectedSince, 12, optional: true, type: :string)
  field(:type, 13, optional: true, type: :string)
  field(:msgRateExpired, 14, optional: true, type: :double)
  field(:msgBacklog, 15, optional: true, type: :uint64)
  field(:messageAckRate, 16, optional: true, type: :double)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetLastMessageId do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:consumer_id, 1, required: true, type: :uint64)
  field(:request_id, 2, required: true, type: :uint64)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetLastMessageIdResponse do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:last_message_id, 1,
    required: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
  )

  field(:request_id, 2, required: true, type: :uint64)

  field(:consumer_mark_delete_position, 3,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.MessageIdData
  )
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetTopicsOfNamespace do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:namespace, 2, required: true, type: :string)

  field(:mode, 3,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetTopicsOfNamespace.Mode,
    default: :PERSISTENT,
    enum: true
  )

  field(:topics_pattern, 4, optional: true, type: :string)
  field(:topics_hash, 5, optional: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetTopicsOfNamespaceResponse do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:topics, 2, repeated: true, type: :string)
  field(:filtered, 3, optional: true, type: :bool, default: false)
  field(:topics_hash, 4, optional: true, type: :string)
  field(:changed, 5, optional: true, type: :bool, default: true)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandWatchTopicList do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:watcher_id, 2, required: true, type: :uint64)
  field(:namespace, 3, required: true, type: :string)
  field(:topics_pattern, 4, required: true, type: :string)
  field(:topics_hash, 5, optional: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandWatchTopicListSuccess do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:watcher_id, 2, required: true, type: :uint64)
  field(:topic, 3, repeated: true, type: :string)
  field(:topics_hash, 4, required: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandWatchTopicUpdate do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:watcher_id, 1, required: true, type: :uint64)
  field(:new_topics, 2, repeated: true, type: :string)
  field(:deleted_topics, 3, repeated: true, type: :string)
  field(:topics_hash, 4, required: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandWatchTopicListClose do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:watcher_id, 2, required: true, type: :uint64)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetSchema do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:topic, 2, required: true, type: :string)
  field(:schema_version, 3, optional: true, type: :bytes)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetSchemaResponse do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)

  field(:error_code, 2,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.ServerError,
    enum: true
  )

  field(:error_message, 3, optional: true, type: :string)
  field(:schema, 4, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.Schema)
  field(:schema_version, 5, optional: true, type: :bytes)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetOrCreateSchema do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:topic, 2, required: true, type: :string)
  field(:schema, 3, required: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.Schema)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetOrCreateSchemaResponse do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)

  field(:error_code, 2,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.ServerError,
    enum: true
  )

  field(:error_message, 3, optional: true, type: :string)
  field(:schema_version, 4, optional: true, type: :bytes)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandTcClientConnectRequest do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:tc_id, 2, required: true, type: :uint64, default: 0)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandTcClientConnectResponse do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)

  field(:error, 2,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.ServerError,
    enum: true
  )

  field(:message, 3, optional: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandNewTxn do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:txn_ttl_seconds, 2, optional: true, type: :uint64, default: 0)
  field(:tc_id, 3, optional: true, type: :uint64, default: 0)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandNewTxnResponse do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)

  field(:error, 4,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.ServerError,
    enum: true
  )

  field(:message, 5, optional: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandAddPartitionToTxn do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)
  field(:partitions, 4, repeated: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandAddPartitionToTxnResponse do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)

  field(:error, 4,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.ServerError,
    enum: true
  )

  field(:message, 5, optional: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.Subscription do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:topic, 1, required: true, type: :string)
  field(:subscription, 2, required: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandAddSubscriptionToTxn do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)
  field(:subscription, 4, repeated: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.Subscription)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandAddSubscriptionToTxnResponse do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)

  field(:error, 4,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.ServerError,
    enum: true
  )

  field(:message, 5, optional: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandEndTxn do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)

  field(:txn_action, 4,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.TxnAction,
    enum: true
  )
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandEndTxnResponse do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)

  field(:error, 4,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.ServerError,
    enum: true
  )

  field(:message, 5, optional: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandEndTxnOnPartition do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)
  field(:topic, 4, optional: true, type: :string)

  field(:txn_action, 5,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.TxnAction,
    enum: true
  )

  field(:txnid_least_bits_of_low_watermark, 6, optional: true, type: :uint64)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandEndTxnOnPartitionResponse do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)

  field(:error, 4,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.ServerError,
    enum: true
  )

  field(:message, 5, optional: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandEndTxnOnSubscription do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)
  field(:subscription, 4, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.Subscription)

  field(:txn_action, 5,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.TxnAction,
    enum: true
  )

  field(:txnid_least_bits_of_low_watermark, 6, optional: true, type: :uint64)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.CommandEndTxnOnSubscriptionResponse do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:request_id, 1, required: true, type: :uint64)
  field(:txnid_least_bits, 2, optional: true, type: :uint64, default: 0)
  field(:txnid_most_bits, 3, optional: true, type: :uint64, default: 0)

  field(:error, 4,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.ServerError,
    enum: true
  )

  field(:message, 5, optional: true, type: :string)
end

defmodule Pulsar.Protocol.Binary.Pulsar.Proto.BaseCommand do
  @moduledoc false

  use Protobuf, syntax: :proto2, protoc_gen_elixir_version: "0.13.0"

  field(:type, 1,
    required: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.BaseCommand.Type,
    enum: true
  )

  field(:connect, 2, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandConnect)
  field(:connected, 3, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandConnected)
  field(:subscribe, 4, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandSubscribe)
  field(:producer, 5, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandProducer)
  field(:send, 6, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandSend)

  field(:send_receipt, 7,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandSendReceipt
  )

  field(:send_error, 8,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandSendError
  )

  field(:message, 9, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandMessage)
  field(:ack, 10, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandAck)
  field(:flow, 11, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandFlow)

  field(:unsubscribe, 12,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandUnsubscribe
  )

  field(:success, 13, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandSuccess)
  field(:error, 14, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandError)

  field(:close_producer, 15,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandCloseProducer
  )

  field(:close_consumer, 16,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandCloseConsumer
  )

  field(:producer_success, 17,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandProducerSuccess
  )

  field(:ping, 18, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandPing)
  field(:pong, 19, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandPong)

  field(:redeliverUnacknowledgedMessages, 20,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandRedeliverUnacknowledgedMessages
  )

  field(:partitionMetadata, 21,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandPartitionedTopicMetadata
  )

  field(:partitionMetadataResponse, 22,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandPartitionedTopicMetadataResponse
  )

  field(:lookupTopic, 23,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandLookupTopic
  )

  field(:lookupTopicResponse, 24,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandLookupTopicResponse
  )

  field(:consumerStats, 25,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandConsumerStats
  )

  field(:consumerStatsResponse, 26,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandConsumerStatsResponse
  )

  field(:reachedEndOfTopic, 27,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandReachedEndOfTopic
  )

  field(:seek, 28, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandSeek)

  field(:getLastMessageId, 29,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetLastMessageId
  )

  field(:getLastMessageIdResponse, 30,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetLastMessageIdResponse
  )

  field(:active_consumer_change, 31,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandActiveConsumerChange
  )

  field(:getTopicsOfNamespace, 32,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetTopicsOfNamespace
  )

  field(:getTopicsOfNamespaceResponse, 33,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetTopicsOfNamespaceResponse
  )

  field(:getSchema, 34,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetSchema
  )

  field(:getSchemaResponse, 35,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetSchemaResponse
  )

  field(:authChallenge, 36,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandAuthChallenge
  )

  field(:authResponse, 37,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandAuthResponse
  )

  field(:ackResponse, 38,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandAckResponse
  )

  field(:getOrCreateSchema, 39,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetOrCreateSchema
  )

  field(:getOrCreateSchemaResponse, 40,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandGetOrCreateSchemaResponse
  )

  field(:newTxn, 50, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandNewTxn)

  field(:newTxnResponse, 51,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandNewTxnResponse
  )

  field(:addPartitionToTxn, 52,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandAddPartitionToTxn
  )

  field(:addPartitionToTxnResponse, 53,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandAddPartitionToTxnResponse
  )

  field(:addSubscriptionToTxn, 54,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandAddSubscriptionToTxn
  )

  field(:addSubscriptionToTxnResponse, 55,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandAddSubscriptionToTxnResponse
  )

  field(:endTxn, 56, optional: true, type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandEndTxn)

  field(:endTxnResponse, 57,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandEndTxnResponse
  )

  field(:endTxnOnPartition, 58,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandEndTxnOnPartition
  )

  field(:endTxnOnPartitionResponse, 59,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandEndTxnOnPartitionResponse
  )

  field(:endTxnOnSubscription, 60,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandEndTxnOnSubscription
  )

  field(:endTxnOnSubscriptionResponse, 61,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandEndTxnOnSubscriptionResponse
  )

  field(:tcClientConnectRequest, 62,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandTcClientConnectRequest
  )

  field(:tcClientConnectResponse, 63,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandTcClientConnectResponse
  )

  field(:watchTopicList, 64,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandWatchTopicList
  )

  field(:watchTopicListSuccess, 65,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandWatchTopicListSuccess
  )

  field(:watchTopicUpdate, 66,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandWatchTopicUpdate
  )

  field(:watchTopicListClose, 67,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandWatchTopicListClose
  )

  field(:topicMigrated, 68,
    optional: true,
    type: Pulsar.Protocol.Binary.Pulsar.Proto.CommandTopicMigrated
  )
end
