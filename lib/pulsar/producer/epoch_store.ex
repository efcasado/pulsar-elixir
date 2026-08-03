defmodule Pulsar.Producer.EpochStore do
  @moduledoc false

  # Topic epochs per client, in ETS, so a restarting producer can tell whether a newer one
  # fenced it under the :ExclusiveWithFencing access mode. The table belongs to the client.

  @doc "Returns the ETS table name for a client."
  @spec table_name(atom()) :: atom()
  def table_name(client_name) do
    Module.concat([__MODULE__, client_name])
  end

  @doc "Initializes the epoch store for a client if it does not already exist."
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

  @doc "Retrieves the stored epoch for a producer."
  @spec get(atom(), String.t(), String.t(), atom()) :: {:ok, integer()} | :error
  def get(client_name, topic, producer_name, access_mode) do
    table = table_name(client_name)

    if :ets.whereis(table) == :undefined do
      :error
    else
      case :ets.lookup(table, {topic, producer_name, access_mode}) do
        [{_, epoch}] -> {:ok, epoch}
        [] -> :error
      end
    end
  end

  @doc "Stores the epoch for a producer, or returns `:error` if the client has no store."
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

  @doc "Deletes the stored epoch for a producer, or returns `:error` if the client has no store."
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
