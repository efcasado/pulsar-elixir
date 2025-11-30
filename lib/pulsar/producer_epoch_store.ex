defmodule Pulsar.ProducerEpochStore do
  @moduledoc """
  Manages persistent storage of producer topic epochs across restarts.

  This module provides an abstraction layer over ETS for storing and retrieving
  topic epochs for producers. The epochs are used to detect when a producer has
  been fenced by a newer producer with ExclusiveWithFencing access mode.

  Each client maintains its own isolated epoch store, created when the client
  starts and cleaned up when the client stops.
  """

  @doc """
  Returns the ETS table name for a given client.
  """
  @spec table_name(atom()) :: atom()
  def table_name(client_name) do
    Module.concat([__MODULE__, client_name])
  end

  @doc """
  Initializes the epoch store for a client.

  Creates the ETS table if it doesn't already exist. This is called
  when a Pulsar client starts.

  ## Parameters

  - `client_name` - The name of the client

  ## Returns

  `:ok`
  """
  @spec init(atom()) :: :ok
  def init(client_name) do
    table = table_name(client_name)

    case :ets.whereis(table) do
      :undefined ->
        :ets.new(table, [:set, :public, :named_table])
        :ok

      _ ->
        :ok
    end
  end

  @doc """
  Retrieves the stored epoch for a producer.

  ## Parameters

  - `client_name` - The name of the client
  - `topic` - The topic name
  - `producer_name` - The producer name
  - `access_mode` - The producer access mode

  ## Returns

  - `{:ok, epoch}` if an epoch is found
  - `:error` if no epoch is stored

  ## Examples

      iex> ProducerEpochStore.get(:default, "my-topic", "my-producer", :Exclusive)
      {:ok, 5}

      iex> ProducerEpochStore.get(:default, "new-topic", "new-producer", :Shared)
      :error
  """
  @spec get(atom(), String.t(), String.t(), atom()) :: {:ok, integer()} | :error
  def get(client_name, topic, producer_name, access_mode) do
    table = table_name(client_name)

    case :ets.lookup(table, {topic, producer_name, access_mode}) do
      [{_, epoch}] -> {:ok, epoch}
      [] -> :error
    end
  end

  @doc """
  Stores the epoch for a producer.

  ## Parameters

  - `client_name` - The name of the client
  - `topic` - The topic name
  - `producer_name` - The producer name
  - `access_mode` - The producer access mode
  - `epoch` - The topic epoch to store

  ## Returns

  `:ok` if successful, `:error` if the table doesn't exist

  ## Examples

      iex> ProducerEpochStore.put(:default, "my-topic", "my-producer", :Exclusive, 5)
      :ok
  """
  @spec put(atom(), String.t(), String.t(), atom(), integer()) :: :ok | :error
  def put(client_name, topic, producer_name, access_mode, epoch) do
    table = table_name(client_name)

    if :ets.whereis(table) == :undefined do
      :error
    else
      :ets.insert(table, {{topic, producer_name, access_mode}, epoch})
      :ok
    end
  end

  @doc """
  Deletes the stored epoch for a producer.

  ## Parameters

  - `client_name` - The name of the client
  - `topic` - The topic name
  - `producer_name` - The producer name
  - `access_mode` - The producer access mode

  ## Returns

  `:ok` if successful, `:error` if the table doesn't exist

  ## Examples

      iex> ProducerEpochStore.delete(:default, "my-topic", "my-producer", :Exclusive)
      :ok
  """
  @spec delete(atom(), String.t(), String.t(), atom()) :: :ok | :error
  def delete(client_name, topic, producer_name, access_mode) do
    table = table_name(client_name)

    if :ets.whereis(table) == :undefined do
      :error
    else
      :ets.delete(table, {topic, producer_name, access_mode})
      :ok
    end
  end
end
