defmodule Pulsar.Bench.Transport do
  @moduledoc "Loopback receiver that acknowledges each complete frame, without Pulsar persistence."
  use GenServer

  alias Pulsar.Broker

  def init({mod, opts}) do
    {:ok, listener} = mod.listen(0, [:binary, active: false, nodelay: true] ++ opts)
    {:ok, {_address, port}} = sockname(mod, listener)
    owner = self()

    receiver =
      Task.async(fn ->
        socket = accept(mod, listener)
        send(owner, :accepted)
        receive_frames(mod, socket)
      end)

    {:ok, socket} = mod.connect(~c"localhost", port, [:binary, active: false, nodelay: true] ++ client_opts(mod), 5_000)

    receive do
      :accepted -> :ok
    after
      5_000 -> raise "loopback receiver failed to accept"
    end

    :ok = mod.close(listener)
    broker = %Broker{socket_module: mod, socket: socket, max_message_size: 6 * 1024 * 1024}
    {:ok, %{broker: broker, receiver: receiver}}
  end

  def handle_call({:publish_message, frame}, from, %{broker: broker} = state) do
    {:keep_state, ^broker, [{:reply, ^from, :ok}]} =
      Broker.connected({:call, from}, {:publish_message, frame}, broker)

    {:ok, <<1>>} = broker.socket_module.recv(broker.socket, 1, 5_000)
    {:reply, :ok, state}
  end

  def terminate(_reason, %{broker: broker, receiver: receiver}) do
    broker.socket_module.close(broker.socket)
    Task.await(receiver, 5_000)
    :ok
  end

  defp sockname(:gen_tcp, listener), do: :inet.sockname(listener)
  defp sockname(:ssl, listener), do: :ssl.sockname(listener)

  defp client_opts(:gen_tcp), do: []
  # Only the ephemeral localhost benchmark uses an unverified certificate.
  defp client_opts(:ssl), do: [verify: :verify_none]

  defp accept(:gen_tcp, listener) do
    {:ok, socket} = :gen_tcp.accept(listener, 5_000)
    socket
  end

  defp accept(:ssl, listener) do
    {:ok, transport} = :ssl.transport_accept(listener, 5_000)
    {:ok, socket} = :ssl.handshake(transport, 5_000)
    socket
  end

  defp receive_frames(mod, socket) do
    case mod.recv(socket, 4, :infinity) do
      {:ok, <<size::32>>} ->
        {:ok, _frame} = mod.recv(socket, size, 5_000)
        :ok = mod.send(socket, <<1>>)
        receive_frames(mod, socket)

      {:error, :closed} ->
        :ok
    end
  end
end
