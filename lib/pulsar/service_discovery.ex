defmodule Pulsar.ServiceDiscovery do
  @doc """
  Extends the generic connection behaviour with support for service discovery
  functionality.
  """
  use Pulsar.Connection

  # API
  
  def start_link(host, opts, start_opts \\ []) do
    Pulsar.Connection.start_link(__MODULE__, host, opts, start_opts)
  end

  def lookup_topic(conn, topic, authoritative \\ false, timeout \\ 5_000) do
    :gen_statem.call(conn, {:lookup_topic, topic, authoritative}, timeout)
  end
  
  def partitioned_topic_metadata(conn, topic, timeout \\ 5_000) do
    :gen_statem.call(conn, {:partitioned_topic_metadata, topic}, timeout)
  end

  # connection callback functions

  def handle_call(from, {:lookup_topic, topic, authoritative}, conn) do
    %Pulsar.Connection{requests: requests} = conn
    request_id = System.unique_integer([:positive, :monotonic])
    conn = %Pulsar.Connection{conn| requests: Map.put(requests, request_id, from)}

    command = %Binary.CommandLookupTopic{
      topic: topic,
      request_id: request_id,
      authoritative: authoritative
    }
    
    Pulsar.Connection.send_command(conn, command)
  end
  def handle_call(from, {:partitioned_topic_metadata, topic}, conn) do
    %Pulsar.Connection{requests: requests} = conn
    request_id = System.unique_integer([:positive, :monotonic])
    conn = %Pulsar.Connection{conn| requests: Map.put(requests, request_id, from)}

    command = %Binary.CommandPartitionedTopicMetadata{
      topic: topic,
      request_id: request_id
    }

    Pulsar.Connection.send_command(conn, command)
  end

  def handle_command(%Binary.CommandLookupTopicResponse{} = command, conn) do
    # TO-DO: Handle non-final responses
    %Binary.CommandLookupTopicResponse{request_id: request_id} = command

    # TO-DO: Simplify response
    reply = {:ok, command}
    Pulsar.Connection.reply(conn, request_id, reply)

    :keep_state_and_data
  end
  def handle_command(%Binary.CommandLookupTopicResponse{} = command, conn) do
    %Binary.CommandLookupTopicResponse{request_id: request_id} = command

    # TO-DO: Simplify response
    reply = {:ok, command}
    Pulsar.Connection.reply(conn, request_id, reply)

    :keep_state_and_data
  end  
end
