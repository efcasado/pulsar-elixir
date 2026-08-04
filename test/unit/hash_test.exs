defmodule Pulsar.HashTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias Pulsar.Hash

  # A key must route to the same partition whichever client published it, so these vectors are
  # lifted verbatim from the other clients' suites rather than generated here.
  #
  #   Java  https://github.com/apache/pulsar/blob/v4.2.4/
  #         pulsar-client/src/test/java/org/apache/pulsar/client/impl/HashTest.java
  #   Go    https://github.com/apache/pulsar-client-go/blob/v0.21.0/pulsar/internal/hash_test.go
  #
  # Upstream annotates the Java vectors "Same value as C++ client".

  @java_murmur3_vectors [
    {"k1", 2_110_152_746},
    {"k2", 1_479_966_664},
    {"key1", 462_881_061},
    {"key2", 1_936_800_180},
    {"key01", 39_696_932},
    {"key02", 751_761_803}
  ]

  # key1 overflows hashCode() as an unsigned int32, key2 is negative as a signed int32.
  @java_string_vectors [
    {"keykeykeykeykey1", 434_058_482},
    {"keykeykey2", 42_978_643}
  ]

  @go_murmur3_vectors [{"", 0x0}, {"hello", 0x248BFA47}, {"test", 0x3A6BD213}]
  @go_string_vectors [{"", 0x0}, {"hello", 0x5E918D2}, {"test", 0x364492}]

  # Every key above is 0, 1 or 2 bytes past a 4-byte block, leaving the 3-byte tail branch
  # unexercised. These are the canonical MurmurHash3 x86_32 seed-0 vectors, masked as Pulsar
  # masks them, and "abc" is the one that reaches it.
  @canonical_murmur3_vectors [
    {"a", 0x3C2569B2},
    {"ab", 0x9BBFD75F},
    {"abc", 0xB3DD93FA},
    {"abcd", 0x43ED676A}
  ]

  describe "murmur3_32/1" do
    test "matches the Java client's Murmur3Hash32 vectors" do
      for {key, expected} <- @java_murmur3_vectors do
        assert Hash.murmur3_32(key) == expected
      end
    end

    test "matches the Go client's Murmur3_32Hash vectors" do
      for {key, expected} <- @go_murmur3_vectors do
        assert Hash.murmur3_32(key) == expected
      end
    end

    test "hashes the UTF-8 bytes of a key" do
      assert Hash.murmur3_32("é") == Hash.murmur3_32(<<0xC3, 0xA9>>)
      assert Hash.murmur3_32("€") == Hash.murmur3_32(<<0xE2, 0x82, 0xAC>>)
    end

    test "matches the canonical vectors, which reach the 3-byte tail the others miss" do
      for {key, raw} <- @canonical_murmur3_vectors do
        assert Hash.murmur3_32(key) == band(raw, 0x7FFFFFFF)
      end
    end
  end

  describe "java_string_hash/1" do
    test "matches the Java client's JavaStringHash vectors" do
      for {key, expected} <- @java_string_vectors do
        assert Hash.java_string_hash(key) == expected
      end
    end

    test "matches the Go client's JavaStringHash vectors" do
      for {key, expected} <- @go_string_vectors do
        assert Hash.java_string_hash(key) == expected
      end
    end

    test "hashes a key outside the basic multilingual plane over its surrogate pair" do
      # U+1D11E encodes to UTF-16 as 0xD834 0xDD1E, which Java folds as 31 * 0xD834 + 0xDD1E.
      assert Hash.java_string_hash("𝄞") == 31 * 0xD834 + 0xDD1E
    end

    test "rejects a key that is not valid UTF-8" do
      assert_raise ArgumentError, fn -> Hash.java_string_hash(<<0xFF, 0xFE>>) end
    end
  end

  describe "partition/3" do
    test "reduces a Pulsar scheme with signSafeMod" do
      assert Hash.partition(:murmur3_32, "tenant-1", 8) == rem(Hash.murmur3_32("tenant-1"), 8)
      assert Hash.partition(:java_string_hash, "tenant-1", 8) == rem(Hash.java_string_hash("tenant-1"), 8)
    end

    test "falls back to the default scheme when a resource carries none" do
      assert Hash.partition(nil, "tenant-1", 8) == Hash.partition(Hash.default_scheme(), "tenant-1", 8)
    end

    test "answers a partition in range for every scheme" do
      for scheme <- Hash.schemes(),
          key <- ["", "tenant-1", "é", String.duplicate("k", 100)],
          partitions <- [1, 2, 3, 8, 64] do
        assert Hash.partition(scheme, key, partitions) in 0..(partitions - 1)
      end
    end

    # A key that routes under one scheme must route under all of them, or changing the option
    # would change which keys are publishable, and :phash2_legacy would accept keys that stop
    # working on the switch to :murmur3_32 it exists to migrate towards.
    test "rejects a non-binary key under every scheme" do
      for scheme <- Hash.schemes(), key <- [:tenant_a, 123, {:a, 1}, nil] do
        assert_raise ArgumentError, ~r/:partition_key/, fn -> Hash.partition(scheme, key, 8) end
      end
    end

    test "rejects a key that is not valid UTF-8 under every scheme" do
      for scheme <- Hash.schemes() do
        assert_raise ArgumentError, ~r/:partition_key/, fn ->
          Hash.partition(scheme, <<0xFF, 0xFE>>, 8)
        end
      end
    end
  end

  describe "partition/3 with :phash2_legacy" do
    test "reproduces the pre-3.0 routing exactly" do
      for key <- ["", "tenant-1", "key-7"], partitions <- [2, 3, 5, 8] do
        assert Hash.partition(:phash2_legacy, key, partitions) == :erlang.phash2(key, partitions)
      end
    end

    test "cannot be reduced like the Pulsar schemes, since phash2/2 is not a rem/2 over phash2/1" do
      assert Enum.any?(0..200, fn candidate ->
               key = "key-#{candidate}"
               :erlang.phash2(key, 7) != rem(:erlang.phash2(key), 7)
             end)
    end
  end

  describe "default_scheme/0" do
    test "is the interoperable scheme, not the legacy one" do
      assert Hash.default_scheme() == :murmur3_32
    end
  end
end
