defmodule Pulsar.BrokerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Pulsar.Broker
  alias Pulsar.Protocol
  alias Pulsar.Protocol.Binary.Pulsar.Proto

  test "only buffers data from the current socket" do
    broker = %Broker{
      socket: :current_socket,
      buffer: <<>>,
      max_frame_size: 1_024
    }

    fragment = <<0, 0, 0>>

    assert {:keep_state, %Broker{buffer: buffer}, []} =
             Broker.connected(:info, {:tcp, :current_socket, fragment}, broker)

    assert buffered(buffer) == fragment

    assert :keep_state_and_data =
             Broker.connected(:info, {:tcp, :stale_socket, fragment}, broker)
  end

  test "drops a half-read frame when the connection goes" do
    broker = %Broker{
      socket: :dead_socket,
      socket_module: :gen_tcp,
      # four bytes claiming a 99-byte frame that never arrived
      buffer: {<<0, 0, 0, 99>>, [], 4},
      prev_backoff: 0,
      requests: %{},
      consumers: %{},
      producers: %{},
      max_frame_size: 5_000_000
    }

    assert {:keep_state, cleared, _actions} = Broker.disconnected(:enter, :connected, broker)
    assert buffered(cleared.buffer) == <<>>

    # Were the stale prefix still there, it would be read as the head of the next
    # frame and desynchronise everything after it.
    ping = Protocol.encode(%Proto.CommandPing{})
    reconnected = %{cleared | socket: :new_socket}

    assert {:keep_state, _broker, actions} = Broker.connected(:info, {:tcp, :new_socket, ping}, reconnected)
    assert [{:next_event, :internal, {:command, %Proto.CommandPing{}}}] = actions
  end

  test "discards unexpected messages rather than crashing the connection" do
    broker = %Broker{socket: :current_socket}

    messages = [
      :stray_atom,
      {:hello, :world},
      {:ssl_passive, :current_socket},
      {:EXIT, self(), :normal}
    ]

    for message <- messages do
      assert :keep_state_and_data = Broker.connected(:info, message, broker),
             "expected #{inspect(message)} to be discarded"
    end
  end

  defp buffered({head, pending, _size}), do: :erlang.iolist_to_binary([head | pending])
  defp buffered(binary) when is_binary(binary), do: binary

  defp down(broker, monitor_ref, pid \\ self()) do
    Broker.connected(:info, {:DOWN, monitor_ref, :process, pid, :shutdown}, broker)
  end

  defp age_requests(broker, by) do
    requests = Map.new(broker.requests, fn {id, {from, timestamp}} -> {id, {from, timestamp - by}} end)

    %{broker | requests: requests}
  end

  defp dead_pid do
    pid = spawn(fn -> :ok end)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}

    pid
  end

  defp socket_pair do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(listen)
    {:ok, client} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])
    {:ok, server} = :gen_tcp.accept(listen, 1_000)
    :ok = :gen_tcp.close(listen)

    {client, server}
  end

  defp read_command(server) do
    {:ok, <<size::32>> = head} = :gen_tcp.recv(server, 4, 1_000)
    {:ok, body} = :gen_tcp.recv(server, size, 1_000)

    Protocol.decode(head <> body)
  end

  test "ignores lifecycle events from stale sockets" do
    broker = %Broker{socket: :current_socket}

    events = [
      {:tcp_closed, :stale_socket},
      {:ssl_closed, :stale_socket},
      {:tcp_error, :stale_socket, :closed},
      {:ssl_error, :stale_socket, :closed}
    ]

    for event <- events do
      assert :keep_state_and_data = Broker.connected(:info, event, broker)
    end
  end

  describe "a monitored process exiting" do
    setup do
      {client, server} = socket_pair()

      %{broker: %Broker{socket_module: :gen_tcp, socket: client}, server: server}
    end

    test "closes the consumer it was registered as", %{broker: broker, server: server} do
      monitor_ref = make_ref()
      broker = %{broker | consumers: %{7 => {self(), monitor_ref}}}

      {result, log} = with_log(fn -> down(broker, monitor_ref) end)

      assert {:keep_state, updated} = result
      assert updated.consumers == %{}
      assert {:ok, %Proto.CommandCloseConsumer{consumer_id: 7}} = read_command(server)
      assert log =~ "Consumer 7 exited: :shutdown, sending CloseConsumer to server"
    end

    test "closes the producer it was registered as", %{broker: broker, server: server} do
      monitor_ref = make_ref()
      broker = %{broker | producers: %{3 => {self(), monitor_ref}}}

      {result, log} = with_log(fn -> down(broker, monitor_ref) end)

      assert {:keep_state, updated} = result
      assert updated.producers == %{}
      assert {:ok, %Proto.CommandCloseProducer{producer_id: 3}} = read_command(server)
      assert log =~ "Producer 3 exited: :shutdown, sending CloseProducer to server"
    end

    test "leaves the other registrations alone when a consumer exits", %{broker: broker, server: server} do
      monitor_ref = make_ref()
      sibling = {spawn(fn -> :ok end), make_ref()}
      producer = {spawn(fn -> :ok end), make_ref()}
      broker = %{broker | consumers: %{7 => {self(), monitor_ref}, 8 => sibling}, producers: %{3 => producer}}

      assert {:keep_state, updated} = down(broker, monitor_ref)
      assert updated.consumers == %{8 => sibling}
      assert updated.producers == %{3 => producer}
      assert {:ok, %Proto.CommandCloseConsumer{consumer_id: 7}} = read_command(server)
    end

    test "leaves the other registrations alone when a producer exits", %{broker: broker, server: server} do
      monitor_ref = make_ref()
      sibling = {spawn(fn -> :ok end), make_ref()}
      consumer = {spawn(fn -> :ok end), make_ref()}
      broker = %{broker | producers: %{3 => {self(), monitor_ref}, 4 => sibling}, consumers: %{7 => consumer}}

      assert {:keep_state, updated} = down(broker, monitor_ref)
      assert updated.producers == %{4 => sibling}
      assert updated.consumers == %{7 => consumer}
      assert {:ok, %Proto.CommandCloseProducer{producer_id: 3}} = read_command(server)
    end

    test "sends nothing for a process that was never registered", %{broker: broker, server: server} do
      assert {:keep_state, updated} = down(broker, make_ref())
      assert updated == broker

      # TCP preserves order, so anything the broker had written would arrive ahead of this.
      :ok = :gen_tcp.send(broker.socket, "sentinel")
      assert {:ok, "sentinel"} = :gen_tcp.recv(server, 8, 1_000)
    end

    test "deregisters even when the close cannot be sent", %{broker: broker, server: server} do
      :ok = :gen_tcp.close(server)
      :ok = :gen_tcp.close(broker.socket)

      monitor_ref = make_ref()
      broker = %{broker | consumers: %{7 => {self(), monitor_ref}}}

      {result, log} = with_log(fn -> down(broker, monitor_ref) end)

      assert {:keep_state, updated} = result
      assert updated.consumers == %{}
      assert log =~ "Failed to send CloseConsumer for consumer 7"
    end

    test "registers the close so its reply is not reported as unmatched", %{broker: broker, server: server} do
      monitor_ref = make_ref()
      broker = %{broker | consumers: %{7 => {self(), monitor_ref}}}

      assert {:keep_state, updated} = down(broker, monitor_ref)
      assert {:ok, %Proto.CommandCloseConsumer{request_id: request_id}} = read_command(server)
      assert Map.has_key?(updated.requests, request_id)

      success = %Proto.CommandSuccess{request_id: request_id}

      {result, log} = with_log(fn -> Broker.connected(:internal, {:command, success}, updated) end)

      assert {:keep_state, replied} = result
      assert replied.requests == %{}
      refute log =~ "No requester found"
    end

    test "lets the sweeper time out a close the server never acknowledged", %{broker: broker, server: server} do
      monitor_ref = make_ref()
      consumers = %{7 => {dead_pid(), monitor_ref}}
      broker = %{broker | consumers: consumers, request_timeout: 1_000, cleanup_interval: 60_000}

      assert {:keep_state, pending} = down(broker, monitor_ref, dead_pid())
      assert {:ok, %Proto.CommandCloseConsumer{}} = read_command(server)
      assert map_size(pending.requests) == 1

      aged = age_requests(pending, 60_000)

      assert {:keep_state, cleaned, _actions} =
               Broker.connected({:timeout, :cleanup_stale_requests}, nil, aged)

      assert cleaned.requests == %{}
    end
  end

  test "warns about a reply nobody is waiting for" do
    broker = %Broker{}
    success = %Proto.CommandSuccess{request_id: 12_345}

    {result, log} = with_log(fn -> Broker.connected(:internal, {:command, success}, broker) end)

    assert {:keep_state, _broker} = result
    assert log =~ "No requester found for request 12345"
  end

  describe "drop_ssl_opts/1" do
    test "strips TLS-only options before a plain TCP connect" do
      opts = [verify: :verify_peer, cacertfile: "/ca.pem", certfile: "/c.pem", keyfile: "/k.pem"]

      assert Broker.drop_ssl_opts(opts ++ [nodelay: true]) == [nodelay: true]
    end

    test "keeps socket options that are not keyword pairs" do
      # Keyword.drop/2 raised a FunctionClauseError on every one of these.
      opts = [:inet6, {:raw, 6, 1, <<1::32>>}, {:verify, :verify_peer}, {:nodelay, true}]

      assert Broker.drop_ssl_opts(opts) == [:inet6, {:raw, 6, 1, <<1::32>>}, {:nodelay, true}]
    end
  end
end
