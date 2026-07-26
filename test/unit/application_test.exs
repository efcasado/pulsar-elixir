defmodule Pulsar.ApplicationTest do
  use ExUnit.Case, async: true

  describe "client_opts/1" do
    test "keeps application-level configuration away from the client" do
      opts =
        Pulsar.Application.client_opts(
          host: "pulsar://localhost:6650",
          clients: [],
          consumers: [a: []],
          producers: [b: []]
        )

      assert opts == [host: "pulsar://localhost:6650"]
    end

    test "forwards an unrecognised option so the client reports it" do
      # Filtering to the client's known keys hid typos on this path while
      # multi-client mode warned about them.
      opts = Pulsar.Application.client_opts(host: "pulsar://localhost:6650", conn_timout: 500)

      assert opts[:conn_timout] == 500
    end
  end
end
