defmodule Pulsar.BrokerTest do
  use ExUnit.Case, async: true

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
