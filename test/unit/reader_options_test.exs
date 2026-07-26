defmodule Pulsar.ReaderOptionsTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  # stream/2 is lazy, so the options are only validated once something enumerates it.
  defp start(opts), do: "persistent://public/default/unread" |> Pulsar.Reader.stream(opts) |> Enum.take(1)

  test "warns about unknown options rather than raising, as the other surfaces do" do
    log =
      capture_log(fn ->
        assert_raise ArgumentError, fn -> start(timeut: 5, host: "pulsar://127.0.0.1:1", client: :other) end
      end)

    assert log =~ "Pulsar.Reader ignoring unknown options"
    assert log =~ ":timeut"
  end

  test "rejects an option of the wrong type" do
    assert_raise NimbleOptions.ValidationError, ~r/:timeout/, fn -> start(timeout: :soon) end
    assert_raise NimbleOptions.ValidationError, ~r/:flow_permits/, fn -> start(flow_permits: 0) end

    assert_raise NimbleOptions.ValidationError, ~r/:start_message_id/, fn ->
      start(start_message_id: 1)
    end
  end

  test "still refuses a host and a client together" do
    assert_raise ArgumentError, ~r/cannot specify both :host and :client/, fn ->
      start(host: "pulsar://127.0.0.1:1", client: :other)
    end
  end

  describe "socket options" do
    test "accepts options that are not keyword pairs" do
      # The :host/:client conflict is checked after the schema, so reaching it proves
      # these got through validation.
      assert_raise ArgumentError, ~r/cannot specify both :host and :client/, fn ->
        start(host: "pulsar://127.0.0.1:1", client: :other, socket_opts: [:inet6, {:raw, 6, 1, <<1::32>>}])
      end
    end

    test "still rejects socket options that are not a list" do
      assert_raise NimbleOptions.ValidationError, ~r/:socket_opts/, fn -> start(socket_opts: :inet6) end
    end
  end
end
