defmodule Pulsar.HashTest do
  use ExUnit.Case, async: true

  alias Pulsar.Hash

  # The point of these schemes is that a key routes to the same partition whichever client
  # published it, so the vectors below are lifted verbatim from the other clients' own suites
  # rather than generated here. Both sources are pinned to a tag, so re-checking them years
  # from now compares against the same text this was written from.
  #
  #   Java  https://github.com/apache/pulsar/blob/v4.2.4/
  #         pulsar-client/src/test/java/org/apache/pulsar/client/impl/HashTest.java
  #   Go    https://github.com/apache/pulsar-client-go/blob/v0.21.0/pulsar/internal/hash_test.go
  #
  # The Java vectors are themselves annotated "Same value as C++ client", so they pin three
  # implementations at once.

  @java_murmur3_vectors [
    {"k1", 2_110_152_746},
    {"k2", 1_479_966_664},
    {"key1", 462_881_061},
    {"key2", 1_936_800_180},
    {"key01", 39_696_932},
    {"key02", 751_761_803}
  ]

  # key1 overflows hashCode() as an unsigned int32; key2 is negative as a signed int32. Both
  # are deliberate upstream edge cases for the masking.
  @java_string_vectors [
    {"keykeykeykeykey1", 434_058_482},
    {"keykeykey2", 42_978_643}
  ]

  @go_murmur3_vectors [{"", 0x0}, {"hello", 0x248BFA47}, {"test", 0x3A6BD213}]
  @go_string_vectors [{"", 0x0}, {"hello", 0x5E918D2}, {"test", 0x364492}]

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

    test "covers every tail length" do
      for length <- 0..8 do
        assert Hash.murmur3_32(String.duplicate("k", length)) in 0..0x7FFFFFFF
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
  end

  describe "partition/3 with :phash2_legacy" do
    test "reproduces the pre-3.0 routing exactly" do
      for key <- ["", "tenant-1", "key-7"], partitions <- [2, 3, 5, 8] do
        assert Hash.partition(:phash2_legacy, key, partitions) == :erlang.phash2(key, partitions)
      end
    end

    # phash2/2 takes its range directly and is not a rem/2 over phash2/1, so it cannot share
    # the reduction the Pulsar schemes use. This is the whole reason partition/3 owns it.
    test "differs from a rem/2 over the unranged hash, which is why it is not reduced like the others" do
      assert Enum.any?(0..200, fn candidate ->
               key = "key-#{candidate}"
               :erlang.phash2(key, 7) != rem(:erlang.phash2(key), 7)
             end)
    end

    test "is not interoperable, and so is not the default" do
      assert Hash.default_scheme() != :phash2_legacy
      assert :phash2_legacy in Hash.schemes()
    end
  end

  describe "default_scheme/0" do
    test "is murmur3_32, which every Pulsar client implements identically" do
      assert Hash.default_scheme() == :murmur3_32
      assert Hash.default_scheme() in Hash.schemes()
    end
  end
end
