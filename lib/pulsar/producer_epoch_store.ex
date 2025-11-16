defmodule Pulsar.ProducerEpochStore do
  @moduledoc """
  Manages persistent storage of producer topic epochs across restarts.

  This module provides an abstraction layer over ETS for storing and retrieving
  topic epochs for producers. The epochs are used to detect when a producer has
  been fenced by a newer producer with ExclusiveWithFencing access mode.

  The store is initialized when the Pulsar application starts and persists for
  the lifetime of the application.
  """

  @table_name :pulsar_producer_epochs

  @doc """
  Initializes the epoch store.

  Creates the ETS table if it doesn't already exist. This should be called
  once when the Pulsar application starts.

  ## Returns

  `:ok`
  """
  @spec init() :: :ok
  def init do
    case :ets.whereis(@table_name) do
      :undefined ->
        :ets.new(@table_name, [:set, :public, :named_table])
        :ok

      _ ->
        :ok
    end
  end

  @doc """
  Retrieves the stored epoch for a producer.

  ## Parameters

  - `topic` - The topic name
  - `producer_name` - The producer name
  - `access_mode` - The producer access mode

  ## Returns

  - `{:ok, epoch}` if an epoch is found
  - `:error` if no epoch is stored

  ## Examples

      iex> ProducerEpochStore.get("my-topic", "my-producer", :Exclusive)
      {:ok, 5}

      iex> ProducerEpochStore.get("new-topic", "new-producer", :Shared)
      :error
  """
  @spec get(String.t(), String.t(), atom()) :: {:ok, integer()} | :error
  def get(topic, producer_name, access_mode) do
    case :ets.lookup(@table_name, {topic, producer_name, access_mode}) do
      [{_, epoch}] -> {:ok, epoch}
      [] -> :error
    end
  end

  @doc """
  Stores the epoch for a producer.

  ## Parameters

  - `topic` - The topic name
  - `producer_name` - The producer name
  - `access_mode` - The producer access mode
  - `epoch` - The topic epoch to store

  ## Returns

  `:ok` if successful, `:error` if the table doesn't exist

  ## Examples

      iex> ProducerEpochStore.put("my-topic", "my-producer", :Exclusive, 5)
      :ok
  """
  @spec put(String.t(), String.t(), atom(), integer()) :: :ok | :error
  def put(topic, producer_name, access_mode, epoch) do
    if :ets.whereis(@table_name) == :undefined do
      :error
    else
      :ets.insert(@table_name, {{topic, producer_name, access_mode}, epoch})
      :ok
    end
  end

  @doc """
  Deletes the stored epoch for a producer.

  ## Parameters

  - `topic` - The topic name
  - `producer_name` - The producer name
  - `access_mode` - The producer access mode

  ## Returns

  `:ok` if successful, `:error` if the table doesn't exist

  ## Examples

      iex> ProducerEpochStore.delete("my-topic", "my-producer", :Exclusive)
      :ok
  """
  @spec delete(String.t(), String.t(), atom()) :: :ok | :error
  def delete(topic, producer_name, access_mode) do
    if :ets.whereis(@table_name) == :undefined do
      :error
    else
      :ets.delete(@table_name, {topic, producer_name, access_mode})
      :ok
    end
  end

  @doc """
  Cleans up the epoch store.

  Deletes the ETS table. This should be called when the Pulsar application stops.

  ## Returns

  `:ok`
  """
  @spec cleanup() :: :ok
  def cleanup do
    if :ets.whereis(@table_name) != :undefined do
      :ets.delete(@table_name)
    end

    :ok
  end
end
