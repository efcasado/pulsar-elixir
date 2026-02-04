defmodule Pulsar.Integration.Client.CommonTest do
  use ExUnit.Case, async: true

  alias Pulsar.Test.Support.System

  require Logger

  @moduletag :integration

  test "can spawn multiple clients at once" do
    client_one = :common_client_1
    client_two = :common_client_2

    {:ok, _pid} = Pulsar.start_client(client_one, host: System.broker().service_url)
    {:ok, _pid} = Pulsar.start_client(client_two, host: System.broker().service_url)

    assert Process.alive?(Process.whereis(client_one))
    assert Process.alive?(Process.whereis(client_two))
  end
end
