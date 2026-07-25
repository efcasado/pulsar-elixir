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

    assert {:keep_state, %Broker{buffer: ^fragment}, []} =
             Broker.connected(:info, {:tcp, :current_socket, fragment}, broker)

    assert :keep_state_and_data =
             Broker.connected(:info, {:tcp, :stale_socket, fragment}, broker)
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
end
