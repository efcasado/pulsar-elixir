defmodule PulsarTest do
  use ExUnit.Case
  doctest Pulsar

  test "greets the world" do
    assert Pulsar.hello() == :world
  end
end
