defmodule Pulsar.Producer.CompressionTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Pulsar.Producer.Options
  alias Pulsar.Producer.Worker
  alias Pulsar.Protocol.Binary.Pulsar.Proto, as: Binary
  alias Pulsar.Test.Support.BrokerStub
  alias Pulsar.Test.Support.ProducerState

  for batched <- [false, true] do
    test "native zstd produces a binary wire payload (batching: #{batched})" do
      broker = start_supervised!({BrokerStub, self()})

      state =
        ProducerState.new(broker,
          compression: :zstd,
          compression_level: 3,
          batch_enabled: unquote(batched),
          batch_size: 1,
          send_timeout: false
        )

      for payload <- [<<>>, "a normal payload", :binary.copy("abcdefgh", 262_144)] do
        assert {:noreply, _state} = Worker.handle_cast({:send_message, payload, [], {self(), make_ref()}}, state)
        assert [entry] = BrokerStub.published()
        assert entry.metadata.compression == :ZSTD
        assert is_binary(entry.payload)

        plain = entry.payload |> :zstd.decompress() |> IO.iodata_to_binary()
        assert entry.metadata.uncompressed_size == byte_size(plain)

        if unquote(batched) do
          <<metadata_size::32, encoded_metadata::binary-size(metadata_size), actual::binary>> = plain
          metadata = Binary.SingleMessageMetadata.decode(encoded_metadata)
          assert entry.metadata.num_messages_in_batch == 1
          assert metadata.payload_size == byte_size(payload)
          assert actual == payload
        else
          assert plain == payload
        end
      end
    end
  end

  describe "zstd level" do
    test "defaults to 3 and follows a {:zstd, level: n} option" do
      for {compression, level} <- [{:zstd, 3}, {{:zstd, [level: 1]}, 1}, {{:zstd, [level: 22]}, 22}] do
        opts = Options.validate!(topic: "persistent://public/default/level", compression: compression)

        assert {:ok, state, _continue} = Worker.init(opts)
        assert state.compression == :zstd
        assert state.compression_level == level
      end
    end

    test "is what the payload is compressed at" do
      broker = start_supervised!({BrokerStub, self()})
      payload = :binary.copy("abcdefgh", 262_144)

      for level <- [1, 3, 22] do
        state = ProducerState.new(broker, compression: :zstd, compression_level: level, send_timeout: false)

        assert {:noreply, _state} = Worker.handle_cast({:send_message, payload, [], {self(), make_ref()}}, state)
        assert [entry] = BrokerStub.published()
        assert entry.payload == IO.iodata_to_binary(:zstd.compress(payload, %{compressionLevel: level}))
      end
    end
  end
end
