defmodule Pulsar.ClientTest do
  use ExUnit.Case, async: true

  alias Pulsar.Client
  alias Pulsar.Test.Support.Utils

  defmodule StoppingResourceSupervisor do
    @moduledoc false
    use GenServer

    def start(name, reason), do: GenServer.start(__MODULE__, reason, name: name)

    @impl true
    def init(reason), do: {:ok, reason}

    @impl true
    def handle_call({:start_child, _child_spec}, _from, reason), do: {:stop, reason, reason}

    def handle_call(:which_children, _from, reason), do: {:stop, reason, reason}
  end

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

    test "runs several clients at once, as long as they are named apart" do
      start_supervised!({Client, name: :first_of_several, host: "pulsar://127.0.0.1:1"})
      start_supervised!({Client, name: :second_of_several, host: "pulsar://127.0.0.1:1"})

      assert Process.alive?(Process.whereis(:first_of_several))
      assert Process.alive?(Process.whereis(:second_of_several))
    end

    test "refuses a name another client is already running under" do
      start_supervised!({Client, name: :taken_name, host: "pulsar://127.0.0.1:1"})

      assert {:error, {:already_started, _pid}} =
               Client.start_link(name: :taken_name, host: "pulsar://127.0.0.1:1")
    end

    test "rejects an option of the wrong type" do
      assert_raise NimbleOptions.ValidationError, ~r/invalid value for :conn_timeout/, fn ->
        Client.start_link(name: :bad_type, host: "pulsar://localhost:6650", conn_timeout: "soon")
      end
    end

    test "rejects malformed and unsupported broker URLs" do
      for {name, url} <- [
            {:missing_scheme, "localhost:6650"},
            {:unsupported_scheme, "http://localhost:6650"},
            {:missing_hostname, "pulsar://"},
            {:invalid_port, "pulsar://localhost:not-a-port"}
          ] do
        assert_raise NimbleOptions.ValidationError,
                     ~r/must be a valid pulsar:\/\/ or pulsar\+ssl:\/\/ broker URL/,
                     fn -> Client.start_link(name: name, host: url) end
      end
    end

    test "rejects unknown options" do
      assert_raise NimbleOptions.ValidationError, ~r/unknown options.*:conn_timout.*:bogus/, fn ->
        Client.start_link(
          name: :unknown_opts,
          host: "pulsar://127.0.0.1:1",
          conn_timout: 500,
          bogus: true
        )
      end
    end
  end

  describe "declared consumers and producers" do
    test "defaults to none" do
      start_supervised!({Client, name: :no_resources, host: "pulsar://127.0.0.1:1"})

      assert Client.consumers(:no_resources) == []
      assert Client.producers(:no_resources) == []
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
      assert_raise NimbleOptions.ValidationError, ~r/invalid value for :access_mode/, fn ->
        Client.start_link(
          name: :bad_producer,
          host: "pulsar://127.0.0.1:1",
          producers: [[topic: "t", access_mode: :Nonsense]]
        )
      end
    end

    test "rejects the removed producer count option in a client declaration" do
      assert_raise NimbleOptions.ValidationError, ~r/unknown options.*:producer_count/, fn ->
        Client.start_link(
          name: :producer_count,
          host: "pulsar://127.0.0.1:1",
          producers: [[topic: "t", producer_count: 2]]
        )
      end
    end

    test "rejects entries that are not keyword lists" do
      assert_raise NimbleOptions.ValidationError, ~r/invalid list in :consumers option/, fn ->
        Client.start_link(name: :bad_shape, host: "pulsar://127.0.0.1:1", consumers: ["a topic"])
      end
    end

    test "rejects the same explicit name on different declarations" do
      # Starting the second reports the first as already started, so it would be discarded.
      assert_raise ArgumentError, ~r/declares more than one Pulsar.Producer named/, fn ->
        Client.start_link(
          name: :dup_producers,
          host: "pulsar://127.0.0.1:1",
          producers: [[topic: "a", name: :duplicate], [topic: "b", name: :duplicate]]
        )
      end
    end

    test "rejects duplicates that come from the default name rather than an explicit one" do
      assert_raise ArgumentError, ~r/declares more than one Pulsar.Consumer named/, fn ->
        Client.start_link(
          name: :dup_consumers,
          host: "pulsar://127.0.0.1:1",
          consumers: [
            [topic: "t", subscription_name: "s", callback_module: MyApp.H],
            [topic: "t", subscription_name: "s", callback_module: MyApp.H, consumer_count: 2]
          ]
        )
      end
    end

    test "allows two resources on one topic when they are named apart" do
      start_supervised!(
        {Client,
         name: :distinct_names,
         host: "pulsar://127.0.0.1:1",
         producers: [[topic: "t", name: :audit], [topic: "t", name: :billing]]}
      )

      assert is_pid(Process.whereis(:distinct_names))
    end

    test "raises before starting anything" do
      assert_raise NimbleOptions.ValidationError, fn ->
        Client.start_link(name: :never_started, host: "pulsar://127.0.0.1:1", producers: [[]])
      end

      refute is_pid(Process.whereis(:never_started))
    end
  end

  describe "operations against a client that is not running" do
    # Registry.lookup/2 raises when the registry is absent, which is the normal state while a
    # client is down or restarting. The facades promise an error tuple, not an exit.
    test "report not found rather than raising" do
      assert Client.lookup_broker("pulsar://127.0.0.1:6650", client: :never_started) == {:error, :not_found}
      assert Client.consumers(:never_started) == []
      assert Client.producers(:never_started) == []
    end

    test "send and stop keep their contracts" do
      assert Pulsar.Producer.send(:absent, "payload", client: :never_started) == {:error, :not_found}
      assert Pulsar.Producer.stop(:absent, client: :never_started) == {:error, :not_found}
      assert Pulsar.Consumer.stop("absent", client: :never_started) == {:error, :not_found}
    end

    test "resource starts report the missing client rather than exiting" do
      assert Pulsar.Producer.start(topic: "t", client: :never_started) == {:error, :client_not_found}

      assert Pulsar.Consumer.start(
               topic: "t",
               subscription_name: "s",
               callback_module: MyApp.Handler,
               client: :never_started
             ) == {:error, :client_not_found}
    end

    test "resource starts report a restarting resource branch rather than exiting" do
      client = :restarting_resources

      {:ok, _producer_supervisor} =
        StoppingResourceSupervisor.start(
          Client.resource_supervisor(:producers, client),
          :shutdown
        )

      assert Pulsar.Producer.start(topic: "t", client: client) == {:error, :client_not_found}

      {:ok, _consumer_supervisor} =
        StoppingResourceSupervisor.start(
          Client.resource_supervisor(:consumers, client),
          {:shutdown, :restarting}
        )

      assert Pulsar.Consumer.start(
               topic: "t",
               subscription_name: "s",
               callback_module: MyApp.Handler,
               client: client
             ) == {:error, :client_not_found}
    end

    test "resource listings tolerate a branch shutting down during traversal" do
      client = :shutting_down_resource_listing

      {:ok, _supervisor} =
        StoppingResourceSupervisor.start(
          Client.resource_supervisor(:consumers, client),
          :shutdown
        )

      assert Client.consumers(client) == []
    end

    test "resource listings surface unexpected branch exits" do
      client = :crashing_resource_listing

      {:ok, _supervisor} =
        StoppingResourceSupervisor.start(
          Client.resource_supervisor(:producers, client),
          :unexpected
        )

      assert catch_exit(Client.producers(client))
    end
  end

  describe "runtime resource initialization" do
    test "registers stable roots while their broker is unavailable" do
      client = :async_runtime_resources
      start_supervised!({Client, name: client, host: "pulsar://127.0.0.1:1"})

      assert {:ok, producer} =
               Pulsar.Producer.start(
                 topic: "persistent://public/default/producer",
                 name: :async_producer,
                 client: client
               )

      assert {:ok, consumer} =
               Pulsar.Consumer.start(
                 topic: "persistent://public/default/consumer",
                 subscription_name: "sub",
                 callback_module: MyApp.Handler,
                 name: :async_consumer,
                 client: client
               )

      assert Client.producers(client) == [producer]
      assert Client.consumers(client) == [consumer]

      assert Pulsar.Producer.send(producer, "payload") == {:error, :not_ready}

      assert Pulsar.Consumer.topic(consumer) == "persistent://public/default/consumer"
      assert Pulsar.Consumer.send_flow(consumer, 1) == {:error, :not_ready}
    end

    test "consumer and producer facades reject each other's roots" do
      client = :resource_kind_validation
      start_supervised!({Client, name: client, host: "pulsar://127.0.0.1:1"})

      assert {:ok, producer} =
               Pulsar.Producer.start(topic: "producer", name: :kind_producer, client: client)

      assert {:ok, consumer} =
               Pulsar.Consumer.start(
                 topic: "consumer",
                 subscription_name: "sub",
                 callback_module: MyApp.Handler,
                 name: :kind_consumer,
                 client: client
               )

      assert Pulsar.Consumer.stop(producer) == {:error, :not_found}
      assert Pulsar.Producer.stop(consumer) == {:error, :not_found}
      assert Process.alive?(producer)
      assert Process.alive?(consumer)
    end

    test "a reader reports a client that was never started rather than starting one" do
      assert [{:error, _reason}] =
               "persistent://public/default/reader"
               |> Pulsar.Reader.stream(client: :never_started, timeout: 100)
               |> Enum.take(1)
    end

    test "a reader reports a startup timeout and removes its consumer" do
      client = :reader_unavailable
      start_supervised!({Client, name: client, host: "pulsar://127.0.0.1:1"})

      assert [{:error, :reader_start_timeout}] =
               "persistent://public/default/reader"
               |> Pulsar.Reader.stream(client: client, startup_timeout: 50)
               |> Enum.take(1)

      assert Client.consumers(client) == []
    end
  end

  describe "restart intensity" do
    test "a client that does not configure them, and one that never started, answer the defaults" do
      start_supervised!({Client, name: :intensity_defaults, host: "pulsar://127.0.0.1:1"})

      assert Client.restart_intensity(:intensity_defaults, :worker) == [max_restarts: 100, max_seconds: 60]
      assert Client.restart_intensity(:intensity_defaults, :resource) == [max_restarts: 3, max_seconds: 60]
      assert Client.restart_intensity(:never_started, :resource) == [max_restarts: 3, max_seconds: 60]
    end

    test "what a client configures reaches its resource supervisors" do
      start_supervised!(
        {Client,
         name: :intensity_configured,
         host: "pulsar://127.0.0.1:1",
         worker_restart_intensity: [max_restarts: 7, max_seconds: 11],
         resource_restart_intensity: [max_restarts: 1, max_seconds: 5]}
      )

      assert Client.restart_intensity(:intensity_configured, :worker) == [max_restarts: 7, max_seconds: 11]
      assert Client.restart_intensity(:intensity_configured, :resource) == [max_restarts: 1, max_seconds: 5]

      supervisor = Process.whereis(Client.resource_supervisor(:consumers, :intensity_configured))
      state = :sys.get_state(supervisor)

      assert state.max_restarts == 1
      assert state.max_seconds == 5
    end
  end

  describe "broker options" do
    test "owns the initial broker in a client-level broker branch" do
      client = :initial_broker_owner
      url = "pulsar://127.0.0.1:1"
      client_pid = start_supervised!({Client, name: client, host: url})

      brokers = child_pid(client_pid, :brokers)
      dynamic = Process.whereis(Client.broker_supervisor(client))
      initial = child_pid(brokers, {:broker, url})

      assert Enum.any?(Supervisor.which_children(brokers), &match?({_id, ^dynamic, :supervisor, _modules}, &1))
      assert DynamicSupervisor.which_children(dynamic) == []
      assert Client.lookup_broker(url, client: client) == {:ok, initial}
      assert Client.random_broker(client) == initial

      redirected_url = "pulsar://127.0.0.1:2"
      assert {:ok, redirected} = Client.start_broker(redirected_url, client: client)
      assert Enum.any?(DynamicSupervisor.which_children(dynamic), &match?({_id, ^redirected, :worker, _modules}, &1))
      assert Client.lookup_broker(redirected_url, client: client) == {:ok, redirected}
      assert Client.random_broker(client) in [initial, redirected]
    end

    test "carries the connection tunables to the broker with their defaults" do
      start_supervised!({Client, name: :broker_defaults, host: "pulsar://127.0.0.1:1"})

      opts = Client.get_broker_opts(:broker_defaults)

      assert opts[:conn_timeout] == 1_000
      assert opts[:max_frame_size] == Pulsar.Protocol.default_max_frame_size()
      assert opts[:ping_interval] == 60_000
      assert opts[:cleanup_interval] == 30_000
      assert opts[:request_timeout] == 60_000
    end

    test "prefers an option passed to start_link over the default" do
      start_supervised!({Client, name: :broker_explicit, host: "pulsar://127.0.0.1:1", max_frame_size: 111_111})

      assert Client.get_broker_opts(:broker_explicit)[:max_frame_size] == 111_111
    end

    test "preserves broker options when a supervised client is cycled" do
      client = :broker_opts_after_restart

      old_client =
        start_supervised!({Client, name: client, host: "pulsar://127.0.0.1:1", max_frame_size: 111_111})

      assert :ok = Client.stop(client)

      {restarted_client, broker_opts} =
        Utils.wait_for(
          fn ->
            restarted_client = Process.whereis(client)
            {restarted_client, Client.get_broker_opts(client)}
          end,
          until: fn {pid, opts} ->
            is_pid(pid) and pid != old_client and opts[:max_frame_size] == 111_111
          end,
          description: "supervised client to restart"
        )

      assert is_pid(restarted_client)
      assert broker_opts[:max_frame_size] == 111_111
    end

    test "erases broker options when a directly started client stops" do
      client = :broker_opts_after_stop
      {:ok, client_pid} = Client.start_link(name: client, host: "pulsar://127.0.0.1:1", max_frame_size: 111_111)
      on_exit(fn -> Client.stop(client) end)

      ref = Process.monitor(client_pid)
      assert Client.get_broker_opts(client)[:max_frame_size] == 111_111
      assert :ok = Client.stop(client)
      assert_receive {:DOWN, ^ref, :process, ^client_pid, :normal}
      assert Client.get_broker_opts(client) == []
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

  defp child_pid(supervisor, id) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.find_value(fn {child_id, pid, _type, _modules} -> child_id == id && pid end)
  end
end
