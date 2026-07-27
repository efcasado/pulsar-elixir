defmodule Pulsar.ClientTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Pulsar.Client

  describe "start_link/1 option validation" do
    test "requires a host" do
      assert_raise NimbleOptions.ValidationError, ~r/required :host option not found/, fn ->
        Client.start_link(name: :missing_host)
      end
    end

    test "names the client :default when not told otherwise" do
      # The same name consumers and producers select by default, so a single-client
      # application needs to say it in neither place.
      start_supervised!({Client, host: "pulsar://127.0.0.1:6650"})

      assert Client.child_spec(host: "h").id == :default
      assert is_pid(Process.whereis(:default))
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

  describe "declared consumers and producers" do
    test "defaults to none" do
      start_supervised!({Client, name: :no_resources, host: "pulsar://127.0.0.1:1"})

      assert DynamicSupervisor.which_children(Client.consumer_supervisor(:no_resources)) == []
      assert DynamicSupervisor.which_children(Client.producer_supervisor(:no_resources)) == []
    end

    test "rejects a consumer option the consumer schema does not accept" do
      assert_raise NimbleOptions.ValidationError, ~r/invalid value for :subscription_type/, fn ->
        Client.start_link(
          name: :bad_consumer,
          host: "pulsar://127.0.0.1:1",
          consumers: [
            [topic: "t", subscription_name: "s", callback_module: MyApp.H, subscription_type: :Nonsense]
          ]
        )
      end
    end

    test "rejects a consumer entry missing a required option" do
      assert_raise NimbleOptions.ValidationError, ~r/required :callback_module option not found/, fn ->
        Client.start_link(
          name: :incomplete_consumer,
          host: "pulsar://127.0.0.1:1",
          consumers: [[topic: "t", subscription_name: "s"]]
        )
      end
    end

    test "rejects a producer option the producer schema does not accept" do
      assert_raise NimbleOptions.ValidationError, ~r/invalid value for :producer_count/, fn ->
        Client.start_link(
          name: :bad_producer,
          host: "pulsar://127.0.0.1:1",
          producers: [[topic: "t", producer_count: :many]]
        )
      end
    end

    test "rejects entries that are not keyword lists" do
      assert_raise NimbleOptions.ValidationError, ~r/invalid list in :consumers option/, fn ->
        Client.start_link(name: :bad_shape, host: "pulsar://127.0.0.1:1", consumers: ["a topic"])
      end
    end

    test "raises before starting anything" do
      assert_raise NimbleOptions.ValidationError, fn ->
        Client.start_link(name: :never_started, host: "pulsar://127.0.0.1:1", producers: [[]])
      end

      refute is_pid(Process.whereis(:never_started))
    end
  end

  describe "broker options" do
    test "carries the connection tunables to the broker with their defaults" do
      start_supervised!({Client, name: :broker_defaults, host: "pulsar://127.0.0.1:1"})

      opts = Client.get_broker_opts(:broker_defaults)

      assert opts[:conn_timeout] == 1_000
      assert opts[:max_frame_size] == Pulsar.Protocol.default_max_frame_size()
      assert opts[:ping_interval] == 60_000
      assert opts[:cleanup_interval] == 30_000
      assert opts[:request_timeout] == 60_000
      assert opts[:max_backoff] == 30_000
    end

    test "prefers an option passed to start_link over the default" do
      start_supervised!({Client, name: :broker_explicit, host: "pulsar://127.0.0.1:1", max_frame_size: 111_111})

      assert Client.get_broker_opts(:broker_explicit)[:max_frame_size] == 111_111
    end

    test "ignores the application environment" do
      # These were read from the application environment until it stopped being a
      # tuning layer; a client is now configured only through its own options.
      Application.put_env(:pulsar, :max_frame_size, 777_777)
      Application.put_env(:pulsar, :ping_interval, 777)
      on_exit(fn -> Enum.each([:max_frame_size, :ping_interval], &Application.delete_env(:pulsar, &1)) end)

      start_supervised!({Client, name: :ignores_env, host: "pulsar://127.0.0.1:1"})

      opts = Client.get_broker_opts(:ignores_env)

      assert opts[:max_frame_size] == Pulsar.Protocol.default_max_frame_size()
      assert opts[:ping_interval] == 60_000
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
