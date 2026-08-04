defmodule Pulsar.Hash do
  @moduledoc false

  # Partition-key hashing, matching the schemes the other Pulsar clients implement so the same
  # key reaches the same partition regardless of which client published it.
  #
  # Both Pulsar schemes mask the result with Integer.MAX_VALUE, as the Java client does, so
  # reducing them to a partition is Pulsar's signSafeMod with no further sign handling.
  #
  # :phash2_legacy is not a Pulsar scheme and no other client can reproduce it. It is a
  # migration path off the pre-3.0 routing, not a peer of the other two.
  #
  # Reference implementations, which test/unit/hash_test.exs pins this module against. The
  # Java tag is the broker version the integration suite runs against.
  #
  # Under https://github.com/apache/pulsar/blob/v4.2.4/
  #
  #   scheme selection   pulsar-client/src/main/java/org/apache/pulsar/client/impl/MessageRouterBase.java
  #   murmur3 wrapper    pulsar-client/src/main/java/org/apache/pulsar/client/impl/Murmur3Hash32.java
  #   murmur3 core       pulsar-common/src/main/java/org/apache/pulsar/common/util/Murmur3_32Hash.java
  #   java string hash   pulsar-client/src/main/java/org/apache/pulsar/client/impl/JavaStringHash.java
  #   default scheme     pulsar-client/src/main/java/org/apache/pulsar/client/impl/conf/ProducerConfigurationData.java
  #
  # Under https://github.com/apache/pulsar-client-go/blob/v0.21.0/
  #
  #   both schemes       pulsar/internal/hash.go
  #   default scheme     pulsar/producer.go

  import Bitwise

  @schemes [:murmur3_32, :java_string_hash, :phash2_legacy]
  @default_scheme :murmur3_32

  @seed 0
  @c1 0xCC9E2D51
  @c2 0x1B873593
  @mask32 0xFFFFFFFF
  @max_int32 0x7FFFFFFF

  @type scheme :: :murmur3_32 | :java_string_hash | :phash2_legacy

  @spec schemes() :: [scheme()]
  def schemes, do: @schemes

  @spec default_scheme() :: scheme()
  def default_scheme, do: @default_scheme

  @doc """
  Picks the partition of a `partitions`-wide topic for a key, under the given scheme.

  Reducing the hash belongs here rather than in the caller because the schemes do not agree
  on it: the Pulsar ones take Pulsar's signSafeMod over a sign-masked hash, while `phash2`
  takes its range directly and does not give the same answer as a `rem/2` over `phash2/1`.

  Raises unless the key is a binary, which `:erlang.phash2/2` accepted anything in place of.
  Encoding is not checked: `:murmur3_32` hashes bytes, and a key that is not valid UTF-8 fails
  the send at `partition_key`'s string field either way.
  """
  @spec partition(scheme() | nil, term(), pos_integer()) :: non_neg_integer()
  def partition(scheme, key, partitions) when is_binary(key) do
    reduce(scheme, key, partitions)
  end

  def partition(_scheme, _key, _partitions) do
    raise ArgumentError, ":partition_key must be a binary"
  end

  defp reduce(nil, key, partitions), do: reduce(@default_scheme, key, partitions)
  defp reduce(:murmur3_32, key, partitions), do: rem(murmur3_32(key), partitions)
  defp reduce(:java_string_hash, key, partitions), do: rem(java_string_hash(key), partitions)
  defp reduce(:phash2_legacy, key, partitions), do: :erlang.phash2(key, partitions)

  @doc """
  MurmurHash3 x86 32-bit, as Pulsar's `Murmur3_32Hash` applies it.

  Every client implements this identically, which is why Pulsar recommends it whenever a topic
  is published to from more than one language.

  Seeded with zero and masked with `Integer.MAX_VALUE`, per `Murmur3_32Hash.makeHash/1`. A key
  is hashed as its UTF-8 bytes, per `Murmur3Hash32.makeHash/1`.
  """
  @spec murmur3_32(binary()) :: non_neg_integer()
  def murmur3_32(key) when is_binary(key) do
    key
    |> body(@seed)
    |> finalize(byte_size(key))
    |> band(@max_int32)
  end

  defp body(<<block::little-unsigned-32, rest::binary>>, h1) do
    k1 = block |> mul(@c1) |> rotl(15) |> mul(@c2)
    h1 = h1 |> bxor(k1) |> rotl(13) |> mul(5)

    body(rest, band(h1 + 0xE6546B64, @mask32))
  end

  defp body(tail, h1), do: {tail, h1}

  defp finalize({tail, h1}, length) do
    h1
    |> bxor(scramble(tail))
    |> bxor(length)
    |> fmix()
  end

  defp scramble(<<b0, b1, b2>>), do: scramble_k1(b2 <<< 16 ||| b1 <<< 8 ||| b0)
  defp scramble(<<b0, b1>>), do: scramble_k1(b1 <<< 8 ||| b0)
  defp scramble(<<b0>>), do: scramble_k1(b0)
  defp scramble(<<>>), do: 0

  defp scramble_k1(k1), do: k1 |> mul(@c1) |> rotl(15) |> mul(@c2)

  defp fmix(h) do
    h = h |> bxor(h >>> 16) |> mul(0x85EBCA6B)
    h = h |> bxor(h >>> 13) |> mul(0xC2B2AE35)

    bxor(h, h >>> 16)
  end

  defp mul(a, b), do: band(a * b, @mask32)

  defp rotl(value, count), do: band(value <<< count ||| value >>> (32 - count), @mask32)

  @doc """
  Java's `String.hashCode`, as Pulsar's `JavaStringHash` applies it.

  This is what the Java and Go clients route on by default, so it is the scheme to pick when
  co-locating keys with producers that left their own default alone. It iterates UTF-16 code
  units, so a key outside the basic multilingual plane hashes over its surrogate pair, and it
  is masked with `Integer.MAX_VALUE` after overflowing as a signed 32-bit int.
  """
  @spec java_string_hash(binary()) :: non_neg_integer()
  def java_string_hash(key) when is_binary(key) do
    case :unicode.characters_to_binary(key, :utf8, {:utf16, :big}) do
      utf16 when is_binary(utf16) -> utf16 |> fold_code_units(0) |> band(@max_int32)
      _invalid -> raise ArgumentError, ":partition_key must be valid UTF-8"
    end
  end

  defp fold_code_units(<<unit::big-unsigned-16, rest::binary>>, hash) do
    fold_code_units(rest, band(31 * hash + unit, @mask32))
  end

  defp fold_code_units(<<>>, hash), do: hash
end
