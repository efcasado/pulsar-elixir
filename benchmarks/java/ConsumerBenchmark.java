import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

public final class ConsumerBenchmark {
  private ConsumerBenchmark() {}

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

    List<Consumer<byte[]>> consumers = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(options.consumersPerPartition());
    AtomicBoolean stop = new AtomicBoolean(false);
    AtomicInteger received = new AtomicInteger();
    AtomicInteger acked = new AtomicInteger();
    AtomicInteger errors = new AtomicInteger();
    List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch finished = new CountDownLatch(options.consumersPerPartition());
    long startedAt;

    try {
      for (int index = 0; index < options.consumersPerPartition(); index++) {
        Consumer<byte[]> consumer =
            client
                .newConsumer(Schema.BYTES)
                .topic(options.topic())
                .subscriptionName(options.subscriptionName())
                .subscriptionType(subscriptionType(options.subscriptionType()))
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(options.inFlight())
                .consumerName(options.subscriptionName() + "-" + index)
                .subscribe();
        consumers.add(consumer);
      }
      startedAt = System.nanoTime();

      for (Consumer<byte[]> consumer : consumers) {
        executor.submit(
            () -> {
              try {
                while (!stop.get()) {
                  Message<byte[]> message = consumer.receive(100, TimeUnit.MILLISECONDS);
                  if (message == null) {
                    continue;
                  }

                  int index = received.incrementAndGet();
                  if (index > options.messages()) {
                    consumer.acknowledge(message);
                    stop.set(true);
                    break;
                  }

                  String publishNs = message.getProperty("benchmark-publish-ns");
                  if (publishNs != null) {
                    try {
                      long published = Long.parseLong(publishNs);
                      latencies.add(Math.max(System.currentTimeMillis() * 1_000_000L - published, 0L) / 1_000L);
                    } catch (NumberFormatException ignored) {
                      // The metadata is optional for clients consuming a prepared topic.
                    }
                  }

                  try {
                    consumer.acknowledge(message);
                    acked.incrementAndGet();
                  } catch (Exception error) {
                    errors.incrementAndGet();
                  }

                  if (index == options.messages()) {
                    stop.set(true);
                    break;
                  }
                }
              } catch (Exception error) {
                if (!stop.get()) {
                  errors.incrementAndGet();
                }
              } finally {
                finished.countDown();
              }
            });
      }

      finished.await(options.timeoutMs(), TimeUnit.MILLISECONDS);
      stop.set(true);
      executor.shutdownNow();
      executor.awaitTermination(options.timeoutMs(), TimeUnit.MILLISECONDS);

      long durationUs = Math.max((System.nanoTime() - startedAt) / 1_000L, 1L);
      int receivedCount = Math.min(received.get(), options.messages());
      Result result =
          Result.from(
              options,
              receivedCount,
              acked.get(),
              errors.get(),
              durationUs,
              percentile(latencies, 0.50),
              percentile(latencies, 0.95),
              percentile(latencies, 0.99));
      int exitCode =
          receivedCount == options.messages()
                  && acked.get() == options.messages()
                  && errors.get() == 0
              ? 0
              : 1;
      return new RunResult(result, exitCode);
    } finally {
      stop.set(true);
      executor.shutdownNow();
      for (Consumer<byte[]> consumer : consumers) {
        consumer.close();
      }
      client.close();
    }
  }

  private static SubscriptionType subscriptionType(String value) {
    return switch (value) {
      case "exclusive" -> SubscriptionType.Exclusive;
      case "failover" -> SubscriptionType.Failover;
      case "key-shared" -> SubscriptionType.Key_Shared;
      default -> SubscriptionType.Shared;
    };
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

  private record RunResult(Result result, int exitCode) {}

  private record Options(
      String url,
      String topic,
      int messages,
      int size,
      int partitions,
      int inFlight,
      int timeoutMs,
      String subscriptionType,
      String subscriptionName,
      int consumersPerPartition) {
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
          values.getOrDefault("topic", "persistent://public/default/benchmark-consumer"),
          integer(values, "messages", 100_000),
          integer(values, "size", 1_024),
          integer(values, "partitions", 1),
          integer(values, "in-flight", 1_000),
          integer(values, "timeout-ms", 30_000),
          values.getOrDefault("subscription-type", "shared"),
          values.getOrDefault("subscription-name", "benchmark-consumer"),
          integer(values, "consumers-per-partition", 1));
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
      if (consumersPerPartition <= 0) {
        throw new IllegalArgumentException("consumers-per-partition must be greater than zero");
      }
      if (!subscriptionType.equals("shared")
          && !subscriptionType.equals("exclusive")
          && !subscriptionType.equals("failover")
          && !subscriptionType.equals("key-shared")) {
        throw new IllegalArgumentException(
            "subscription-type must be one of: shared, exclusive, failover, key-shared");
      }
    }
  }

  private record Result(
      int schemaVersion,
      String operation,
      String client,
      String topic,
      int messagesRequested,
      int messagesReceived,
      int messagesAcked,
      int payloadBytes,
      int partitions,
      String subscriptionType,
      String subscriptionName,
      int consumersPerPartition,
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
        int received,
        int acked,
        int errors,
        long durationUs,
        Long p50Us,
        Long p95Us,
        Long p99Us) {
      return new Result(
          1,
          "consumer",
          "java",
          options.topic(),
          options.messages(),
          received,
          acked,
          options.size(),
          options.partitions(),
          options.subscriptionType(),
          options.subscriptionName(),
          options.consumersPerPartition(),
          durationUs,
          rate(received, durationUs),
          rate((double) received * options.size(), durationUs),
          "consumer_e2e",
          p50Us,
          p95Us,
          p99Us,
          errors);
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
          + ",\"messages_received\":"
          + messagesReceived
          + ",\"messages_acked\":"
          + messagesAcked
          + ",\"payload_bytes\":"
          + payloadBytes
          + ",\"partitions\":"
          + partitions
          + ",\"subscription_type\":\""
          + escape(subscriptionType)
          + "\",\"subscription_name\":\""
          + escape(subscriptionName)
          + "\",\"consumers_per_partition\":"
          + consumersPerPartition
          + ",\"duration_us\":"
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
