defmodule Pulsar.ReaderOptionsTest do
  use ExUnit.Case, async: true

  # stream/2 is lazy, so the options are only validated once something enumerates it.
  defp start(opts), do: "persistent://public/default/unread" |> Pulsar.Reader.stream(opts) |> Enum.take(1)

  test "rejects unknown options" do
    assert_raise NimbleOptions.ValidationError, ~r/unknown options \[:timeut\]/, fn ->
      start(timeut: 5)
    end
  end

  test "rejects an option of the wrong type" do
    assert_raise NimbleOptions.ValidationError, ~r/:timeout/, fn -> start(timeout: :soon) end
    assert_raise NimbleOptions.ValidationError, ~r/:startup_timeout/, fn -> start(startup_timeout: :soon) end
    assert_raise NimbleOptions.ValidationError, ~r/:flow_permits/, fn -> start(flow_permits: 0) end
    assert_raise NimbleOptions.ValidationError, ~r/:start_message_id/, fn -> start(start_message_id: 1) end
  end

  test "no longer takes the options that configured an implicit client" do
    # A reader reads through a client someone else started, so :host, :auth and
    # :socket_opts are the client's business rather than the reader's.
    docs = Pulsar.Reader.stream_options_docs()

    for gone <- ["`:host`", "`:auth`", "`:socket_opts`"], do: refute(docs =~ gone)
    assert docs =~ "`:client`"
    assert docs =~ "`:startup_timeout`"
  end
end
