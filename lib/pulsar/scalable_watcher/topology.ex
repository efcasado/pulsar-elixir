defmodule Pulsar.ScalableWatcher.Topology do
  @moduledoc """
  Queue-model source for `Pulsar.ScalableWatcher`.

  Opens a DAG watch session (`CommandScalableTopicLookup`) and turns each pushed
  `CommandScalableTopicUpdate` into the set of *all* consumable segments — both
  active and sealed, so sealed segments keep draining until the controller GC's
  them out of the DAG.
  """

  @behaviour Pulsar.ScalableWatcher.Source

  alias Pulsar.Broker
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.ServiceDiscovery

  require Logger

  @default_client :default

  defstruct [:topic, :client, :session_id, :resolved_topic_name]

  @impl true
  def init(opts) do
    %__MODULE__{
      topic: Keyword.fetch!(opts, :topic),
      client: Keyword.get(opts, :client, @default_client),
      session_id: System.unique_integer([:positive, :monotonic])
    }
  end

  @impl true
  def open(state, owner) do
    close_command = %Binary.CommandScalableTopicClose{session_id: state.session_id}

    with {:ok, broker_pid} <- ServiceDiscovery.lookup_topic(state.topic, client: state.client),
         :ok <- Broker.register_watcher(broker_pid, state.session_id, owner, close_command) do
      # Fire-and-forget: the initial reply and all later updates arrive as routed
      # CommandScalableTopicUpdate messages, not a synchronous response.
      Broker.send_command(broker_pid, %Binary.CommandScalableTopicLookup{
        session_id: state.session_id,
        topic: state.topic
      })

      {:ok, broker_pid, state, nil}
    end
  end

  @impl true
  def handle_update(%Binary.CommandScalableTopicUpdate{error: error} = update, _state) when error != nil do
    Logger.warning("Scalable topology watch error: #{error} #{update.message}")
    :ignore
  end

  def handle_update(%Binary.CommandScalableTopicUpdate{dag: nil}, _state), do: :ignore

  def handle_update(%Binary.CommandScalableTopicUpdate{dag: dag} = update, state) do
    resolved = update.resolved_topic_name || state.resolved_topic_name
    desired = desired_segments(dag, resolved, state.topic)
    {:update, dag.epoch, desired, %{state | resolved_topic_name: resolved}}
  end

  def handle_update(_message, _state), do: :ignore

  # %{segment_id => consume_topic} for every consumable segment in the DAG.
  defp desired_segments(dag, resolved_topic_name, topic) do
    Enum.reduce(dag.segments, %{}, fn segment, acc ->
      case consume_target(segment, resolved_topic_name) do
        {:ok, consume_topic} ->
          Map.put(acc, segment.segment_id, consume_topic)

        {:error, reason} ->
          Logger.warning("Scalable topology: skipping segment #{segment.segment_id} for #{topic}: #{inspect(reason)}")
          acc
      end
    end)
  end

  # Legacy segments carry the backing persistent:// topic; native segments are
  # addressed by a computed segment://<topic>/<hash_start:04x>-<hash_end:04x>-<id>
  # URI derived from the canonical (topic://…) name, which classic subscribe accepts.
  defp consume_target(%Binary.SegmentInfoProto{legacy_topic_name: name}, _resolved) when is_binary(name) and name != "" do
    {:ok, name}
  end

  defp consume_target(%Binary.SegmentInfoProto{} = segment, resolved_topic_name) when is_binary(resolved_topic_name) do
    base = String.replace_prefix(resolved_topic_name, "topic://", "")
    descriptor = "#{hex4(segment.hash_start)}-#{hex4(segment.hash_end)}-#{segment.segment_id}"
    {:ok, "segment://#{base}/#{descriptor}"}
  end

  defp consume_target(%Binary.SegmentInfoProto{segment_id: id}, _resolved) do
    {:error, {:unresolved_segment, id}}
  end

  defp hex4(n), do: n |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(4, "0")
end
