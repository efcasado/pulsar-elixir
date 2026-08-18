import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.CompressionType;

public final class ProducerBenchmark {
  private ProducerBenchmark() {}

  public static void main(String[] arguments) {
    Options options;
    try {
      options = Options.parse(arguments);
      options.validate();
    } catch (IllegalArgumentException error) {
      System.err.println(error.getMessage());
      System.exit(2);
      return;
    }

    RunResult run;
    try {
      run = run(options);
    } catch (Exception error) {
      System.err.println("benchmark failed: " + error);
      System.exit(1);
      return;
    }

    System.out.println(run.result().toJson());
    if (run.exitCode() != 0) {
      System.exit(run.exitCode());
    }
  }

  private static RunResult run(Options options) throws Exception {
    PulsarClient client =
        PulsarClient.builder()
            .serviceUrl(options.url())
            .operationTimeout(options.timeoutMs(), TimeUnit.MILLISECONDS)
            .connectionTimeout(options.timeoutMs(), TimeUnit.MILLISECONDS)
            .build();

    Producer<byte[]> producer = null;
    try {
      ProducerBuilder<byte[]> builder =
          client
              .newProducer(Schema.BYTES)
              .topic(options.topic())
              .producerName("benchmark-java-producer")
              .maxPendingMessages(options.inFlight())
              .blockIfQueueFull(false)
              .enableBatching(options.batching())
              .batchingMaxMessages(options.batchSize())
              .batchingMaxPublishDelay(options.batchDelayMs(), TimeUnit.MILLISECONDS)
              .sendTimeout(options.timeoutMs(), TimeUnit.MILLISECONDS)
              .compressionType(compressionType(options.compression()));
      producer = builder.create();
      long startedAt = System.nanoTime();
      Stats combined = publish(producer, options.messages(), options);
      long durationUs = Math.max((System.nanoTime() - startedAt) / 1_000L, 1L);
      Result result =
          Result.from(
              options,
              combined,
              durationUs,
              percentile(combined.latencies, 0.50),
              percentile(combined.latencies, 0.95),
              percentile(combined.latencies, 0.99));
      int exitCode = combined.acked == options.messages() && combined.errors == 0 ? 0 : 1;
      return new RunResult(result, exitCode);
    } finally {
      if (producer != null) {
        producer.close();
      }
      client.close();
    }
  }

  private static Stats publish(Producer<byte[]> producer, int count, Options options) {
    Stats output = new Stats();
    byte[] payload = payloadBytes(options.size(), options.payloadMode());

    for (int start = 0; start < count; start += options.inFlight()) {
      int end = Math.min(start + options.inFlight(), count);
      List<Pending> pending = new ArrayList<>(end - start);
      for (int index = start; index < end; index++) {
        long startedAt = System.nanoTime();
        try {
          CompletableFuture<?> future = producer.sendAsync(payload);
          pending.add(new Pending(future, startedAt));
        } catch (RuntimeException error) {
          output.errors++;
        }
      }

      long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.timeoutMs());
      for (Pending message : pending) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          output.errors++;
          continue;
        }

        try {
          message.future().get(remaining, TimeUnit.NANOSECONDS);
          output.acked++;
          output.latencies.add((System.nanoTime() - message.startedAt()) / 1_000L);
        } catch (InterruptedException error) {
          Thread.currentThread().interrupt();
          output.errors++;
          return output;
        } catch (ExecutionException | TimeoutException error) {
          output.errors++;
        }
      }
    }

    return output;
  }

  private static CompressionType compressionType(String value) {
    return value.equals("zstd") ? CompressionType.ZSTD : CompressionType.NONE;
  }

  private static byte[] payloadBytes(int size, String mode) {
    byte[] payload = new byte[size];
    if (mode.equals("zero")) {
      return payload;
    }

    int state = 0xA5A5A5A5;
    for (int index = 0; index < payload.length; index++) {
      state ^= state << 13;
      state ^= state >>> 17;
      state ^= state << 5;
      payload[index] = (byte) (state >>> 24);
    }
    return payload;
  }


  private static Long percentile(List<Long> values, double quantile) {
    if (values.isEmpty()) {
      return null;
    }
    List<Long> sorted = new ArrayList<>(values);
    Collections.sort(sorted);
    int index = Math.max((int) Math.ceil(sorted.size() * quantile) - 1, 0);
    return sorted.get(index);
  }

  private static double rate(double value, long durationUs) {
    return Math.round(value * 1_000_000d / durationUs * 1_000d) / 1_000d;
  }

  private record Pending(CompletableFuture<?> future, long startedAt) {}

  private static final class Stats {
    private final List<Long> latencies = new ArrayList<>();
    private int acked;
    private int errors;

    private void add(Stats other) {
      latencies.addAll(other.latencies);
      acked += other.acked;
      errors += other.errors;
    }
  }

  private record RunResult(Result result, int exitCode) {}

  private record Options(
      String url,
      String topic,
      int messages,
      int size,
      int partitions,
      int inFlight,
      int timeoutMs,
      boolean batching,
      int batchSize,
      int batchDelayMs,
      String compression,
      String payloadMode) {
    private static Options parse(String[] arguments) {
      Map<String, String> values = new HashMap<>();
      for (int index = 0; index < arguments.length; index++) {
        String argument = arguments[index];
        if (!argument.startsWith("--")) {
          throw new IllegalArgumentException("unexpected argument: " + argument);
        }
        String name = argument.substring(2);
        if (index + 1 >= arguments.length || arguments[index + 1].startsWith("--")) {
          values.put(name, "true");
        } else {
          values.put(name, arguments[++index]);
        }
      }

      return new Options(
          values.getOrDefault("url", "pulsar://localhost:6650"),
          values.getOrDefault("topic", "persistent://public/default/benchmark-producer"),
          integer(values, "messages", 100_000),
          integer(values, "size", 1_024),
          integer(values, "partitions", 1),
          integer(values, "in-flight", 1_000),
          integer(values, "timeout-ms", 30_000),
          Boolean.parseBoolean(values.getOrDefault("batching", "false")),
          integer(values, "batch-size", 100),
          integer(values, "batch-delay-ms", 10),
          values.getOrDefault("compression", "none"),
          values.getOrDefault("payload", "zero"));
    }

    private static int integer(Map<String, String> values, String name, int fallback) {
      String value = values.get(name);
      if (value == null) {
        return fallback;
      }
      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException error) {
        throw new IllegalArgumentException("--" + name + " must be an integer");
      }
    }

    private void validate() {
      if (messages <= 0) {
        throw new IllegalArgumentException("messages must be greater than zero");
      }
      if (size < 0) {
        throw new IllegalArgumentException("size must not be negative");
      }
      if (partitions <= 0) {
        throw new IllegalArgumentException("partitions must be greater than zero");
      }
      if (inFlight <= 0) {
        throw new IllegalArgumentException("in-flight must be greater than zero");
      }
      if (timeoutMs <= 0) {
        throw new IllegalArgumentException("timeout-ms must be greater than zero");
      }
      if (batchSize <= 0) {
        throw new IllegalArgumentException("batch-size must be greater than zero");
      }
      if (batchDelayMs <= 0) {
        throw new IllegalArgumentException("batch-delay-ms must be greater than zero");
      }
      if (!compression.equals("none") && !compression.equals("zstd")) {
        throw new IllegalArgumentException("compression must be one of: none, zstd");
      }
      if (!payloadMode.equals("zero") && !payloadMode.equals("high-entropy")) {
        throw new IllegalArgumentException("payload must be one of: zero, high-entropy");
      }
    }
  }

  private record Result(
      int schemaVersion,
      String operation,
      String client,
      String topic,
      int messagesRequested,
      int messagesAcked,
      int payloadBytes,
      int partitions,
      int inFlight,
      boolean batching,
      int batchSize,
      int batchDelayMs,
      String compression,
      String payloadMode,
      long durationUs,
      double messagesPerSecond,
      double bytesPerSecond,
      String latencyType,
      Long p50Us,
      Long p95Us,
      Long p99Us,
      int errors) {
    private static Result from(
        Options options,
        Stats stats,
        long durationUs,
        Long p50Us,
        Long p95Us,
        Long p99Us) {
      return new Result(
          1,
          "producer",
          "java",
          options.topic(),
          options.messages(),
          stats.acked,
          options.size(),
          options.partitions(),
          options.inFlight(),
          options.batching(),
          options.batchSize(),
          options.batchDelayMs(),
          options.compression(),
          options.payloadMode(),
          durationUs,
          rate(stats.acked, durationUs),
          rate((double) stats.acked * options.size(), durationUs),
          "producer_ack",
          p50Us,
          p95Us,
          p99Us,
          stats.errors);
    }

    private String toJson() {
      return "{"
          + "\"schema_version\":"
          + schemaVersion
          + ",\"operation\":\""
          + escape(operation)
          + "\",\"client\":\""
          + escape(client)
          + "\",\"topic\":\""
          + escape(topic)
          + "\",\"messages_requested\":"
          + messagesRequested
          + ",\"messages_acked\":"
          + messagesAcked
          + ",\"payload_bytes\":"
          + payloadBytes
          + ",\"partitions\":"
          + partitions
          + ",\"in_flight\":"
          + inFlight
          + ",\"batching\":"
          + batching
          + ",\"batch_size\":"
          + batchSize
          + ",\"batch_delay_ms\":"
          + batchDelayMs
          + ",\"compression\":\""
          + escape(compression)
          + "\",\"payload_mode\":\""
          + escape(payloadMode)
          + "\",\"duration_us\":"
          + durationUs
          + ",\"messages_per_second\":"
          + number(messagesPerSecond)
          + ",\"bytes_per_second\":"
          + number(bytesPerSecond)
          + ",\"latency_type\":\""
          + escape(latencyType)
          + "\",\"p50_us\":"
          + nullable(p50Us)
          + ",\"p95_us\":"
          + nullable(p95Us)
          + ",\"p99_us\":"
          + nullable(p99Us)
          + ",\"errors\":"
          + errors
          + "}";
    }

    private static String nullable(Long value) {
      return value == null ? "null" : value.toString();
    }

    private static String number(double value) {
      return String.format(Locale.ROOT, "%.3f", value);
    }

    private static String escape(String value) {
      return value
          .replace("\\", "\\\\")
          .replace("\"", "\\\"")
          .replace("\b", "\\b")
          .replace("\f", "\\f")
          .replace("\n", "\\n")
          .replace("\r", "\\r")
          .replace("\t", "\\t");
    }
  }
}
