defmodule Pulsar.ClientTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Pulsar.Client

  describe "start_link/1 option validation" do
    test "requires a name and a host" do
      assert_raise NimbleOptions.ValidationError, ~r/required :name option not found/, fn ->
        Client.start_link(host: "pulsar://localhost:6650")
      end

      assert_raise NimbleOptions.ValidationError, ~r/required :host option not found/, fn ->
        Client.start_link(name: :missing_host)
      end
    end

    test "rejects an option of the wrong type" do
      assert_raise NimbleOptions.ValidationError, ~r/invalid value for :conn_timeout/, fn ->
        Client.start_link(name: :bad_type, host: "pulsar://localhost:6650", conn_timeout: "soon")
      end
    end

    test "warns about unknown options rather than failing" do
      log =
        capture_log(fn ->
          {:ok, _pid} =
            Client.start_link(
              name: :unknown_opts,
              host: "pulsar://127.0.0.1:1",
              conn_timout: 500,
              bogus: true
            )

          Client.stop(:unknown_opts)
        end)

      assert log =~ "ignoring unknown options"
      assert log =~ ":conn_timout"
      assert log =~ ":bogus"
    end
  end

  describe "broker option precedence" do
    test "falls back to the application environment when an option is omitted" do
      Application.put_env(:pulsar, :max_frame_size, 777_777)
      on_exit(fn -> Application.delete_env(:pulsar, :max_frame_size) end)

      {:ok, _pid} = Client.start_link(name: :from_env, host: "pulsar://127.0.0.1:1")

      assert Client.get_broker_opts(:from_env) == [max_frame_size: 777_777]

      Client.stop(:from_env)
    end

    test "prefers an option passed to start_link over the application environment" do
      Application.put_env(:pulsar, :max_frame_size, 777_777)
      on_exit(fn -> Application.delete_env(:pulsar, :max_frame_size) end)

      {:ok, _pid} =
        Client.start_link(name: :from_opts, host: "pulsar://127.0.0.1:1", max_frame_size: 111_111)

      assert Client.get_broker_opts(:from_opts) == [max_frame_size: 111_111]

      Client.stop(:from_opts)
    end
  end

  describe "conn_timeout" do
    test "accepts :infinity to wait indefinitely" do
      start_supervised!({Client, name: :infinite_conn_timeout, host: "pulsar://127.0.0.1:6650", conn_timeout: :infinity})

      assert Client.get_broker_opts(:infinite_conn_timeout)[:conn_timeout] == :infinity
    end

    test "still rejects a value that is not a timeout" do
      assert_raise NimbleOptions.ValidationError, ~r/:conn_timeout/, fn ->
        Client.start_link(name: :bad_conn_timeout, host: "pulsar://127.0.0.1:6650", conn_timeout: -1)
      end
    end
  end

  describe "socket options" do
    test "accepts options that are not keyword pairs" do
      # :inet6 and {:raw, ...} are valid for :gen_tcp.connect/4 and :ssl.connect/4,
      # and neither is a keyword pair.
      opts = [:inet6, {:raw, 6, 1, <<1::32>>}, {:nodelay, true}]

      start_supervised!({Client, name: :raw_socket_opts, host: "pulsar://127.0.0.1:6650", socket_opts: opts})

      assert Client.get_broker_opts(:raw_socket_opts)[:socket_opts] == opts
    end

    test "still rejects socket options that are not a list" do
      assert_raise NimbleOptions.ValidationError, ~r/:socket_opts/, fn ->
        Client.start_link(name: :bad_socket_opts, host: "pulsar://127.0.0.1:6650", socket_opts: :inet6)
      end
    end
  end
end
