defmodule Pulsar.TopicTest do
  use ExUnit.Case, async: true

  alias Pulsar.Topic

  doctest Topic

  test "builds an internal partition name from an atom resource name" do
    assert Topic.partition(:audit, 2) == "audit-partition-2"
  end
end
