defmodule Pulsar.BrokerTest do
  use ExUnit.Case, async: true

  alias Pulsar.Broker

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
end
