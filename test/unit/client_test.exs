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
      assert_raise NimbleOptions.ValidationError, ~r/expected positive integer/, fn ->
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

  describe "supported_options/0" do
    test "covers everything the schema accepts" do
      assert :name in Client.supported_options()
      assert :host in Client.supported_options()
      assert :max_frame_size in Client.supported_options()
    end

    test "excludes application configuration the client has no use for" do
      refute :consumers in Client.supported_options()
      refute :producers in Client.supported_options()
      refute :clients in Client.supported_options()
    end
  end
end
