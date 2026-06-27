defmodule Pulsar.ScalableWatcher.Assignment do
  @moduledoc """
  Stream-model source for `Pulsar.ScalableWatcher`.

  Registers with the controller (`CommandScalableTopicSubscribe`, type STREAM),
  taking the initial assignment from the response, and turns each pushed
  `CommandScalableTopicAssignmentUpdate` into the assigned subset of segments.
  Each assigned segment arrives as a ready-made `segment://…` topic.

  Ordering across split/merge is the broker's job: the controller withholds a
  child segment until its parent has drained, so this source only ever reports
  whatever it is currently assigned.
  """

  @behaviour Pulsar.ScalableWatcher.Source

  alias Pulsar.Broker
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.ServiceDiscovery

  @default_client :default

  defstruct [:topic, :client, :subscription, :consumer_name, :consumer_id, :consumer_type]

  @impl true
  def init(opts) do
    consumer_id = System.unique_integer([:positive, :monotonic])

    %__MODULE__{
      topic: Keyword.fetch!(opts, :topic),
      client: Keyword.get(opts, :client, @default_client),
      subscription: Keyword.fetch!(opts, :subscription),
      # The controller keys each consumer by name in a metadata path, so it must
      # be non-empty and unique within the subscription.
      consumer_name: Keyword.get(opts, :consumer_name) || "consumer-#{consumer_id}",
      consumer_id: consumer_id,
      consumer_type: Keyword.get(opts, :consumer_type, :STREAM)
    }
  end

  @impl true
  def open(state, owner) do
    with {:ok, broker_pid} <- ServiceDiscovery.lookup_topic(state.topic, client: state.client),
         :ok <- Broker.register_watcher(broker_pid, state.consumer_id, owner),
         {:ok, %Binary.CommandScalableTopicSubscribeResponse{error: nil} = response} <-
           Broker.send_request(broker_pid, %Binary.CommandScalableTopicSubscribe{
             topic: state.topic,
             subscription: state.subscription,
             consumer_name: state.consumer_name,
             consumer_id: state.consumer_id,
             consumer_type: state.consumer_type
           }) do
      {:ok, broker_pid, state, to_desired(response.assignment)}
    else
      {:ok, %Binary.CommandScalableTopicSubscribeResponse{error: error, message: message}} ->
        {:error, {:scalable_subscribe_failed, error, message}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def handle_update(%Binary.CommandScalableTopicAssignmentUpdate{} = update, state) do
    {epoch, desired} = to_desired(update.assignment)
    {:update, epoch, desired, state}
  end

  def handle_update(_message, _state), do: :ignore

  defp to_desired(nil), do: {0, %{}}

  defp to_desired(%Binary.ScalableConsumerAssignment{} = assignment) do
    desired = Map.new(assignment.segments, fn segment -> {segment.segment_id, segment.segment_topic} end)
    {assignment.layout_epoch, desired}
  end
end
