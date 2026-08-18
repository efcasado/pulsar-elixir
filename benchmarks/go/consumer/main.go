package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type consumerOptions struct {
	url                   string
	topic                 string
	messages              int
	size                  int
	partitions            int
	inFlight              int
	timeoutMS             int
	subscriptionType      string
	subscriptionName      string
	consumersPerPartition int
}

type consumerResult struct {
	SchemaVersion         int     `json:"schema_version"`
	Operation             string  `json:"operation"`
	Client                string  `json:"client"`
	Topic                 string  `json:"topic"`
	MessagesRequested     int     `json:"messages_requested"`
	MessagesReceived      int     `json:"messages_received"`
	MessagesAcked         int     `json:"messages_acked"`
	PayloadBytes          int     `json:"payload_bytes"`
	Partitions            int     `json:"partitions"`
	SubscriptionType      string  `json:"subscription_type"`
	SubscriptionName      string  `json:"subscription_name"`
	ConsumersPerPartition int     `json:"consumers_per_partition"`
	DurationUS            int64   `json:"duration_us"`
	MessagesPerSecond     float64 `json:"messages_per_second"`
	BytesPerSecond        float64 `json:"bytes_per_second"`
	LatencyType           string  `json:"latency_type"`
	P50US                 *int64  `json:"p50_us"`
	P95US                 *int64  `json:"p95_us"`
	P99US                 *int64  `json:"p99_us"`
	Errors                int     `json:"errors"`
}

func main() {
	opts := parseConsumerOptions()
	if err := validateConsumerOptions(opts); err != nil {
		log.Print(err)
		os.Exit(2)
	}

	output, exitCode, err := runConsumer(opts)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}

	encoded, err := json.Marshal(output)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}

	fmt.Println(string(encoded))
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

func parseConsumerOptions() consumerOptions {
	var opts consumerOptions

	flag.StringVar(&opts.url, "url", "pulsar://localhost:6650", "Pulsar broker URL")
	flag.StringVar(&opts.topic, "topic", "persistent://public/default/benchmark-consumer", "Pulsar topic")
	flag.IntVar(&opts.messages, "messages", 100000, "number of messages")
	flag.IntVar(&opts.size, "size", 1024, "payload size in bytes")
	flag.IntVar(&opts.partitions, "partitions", 1, "number of topic partitions")
	flag.IntVar(&opts.inFlight, "in-flight", 1000, "consumer receiver queue size")
	flag.IntVar(&opts.timeoutMS, "timeout-ms", 30000, "consumer timeout in milliseconds")
	flag.StringVar(&opts.subscriptionType, "subscription-type", "shared", "subscription type")
	flag.StringVar(&opts.subscriptionName, "subscription-name", "benchmark-consumer", "subscription name")
	flag.IntVar(
		&opts.consumersPerPartition,
		"consumers-per-partition",
		1,
		"number of logical consumers per partition",
	)
	flag.Parse()

	if flag.NArg() != 0 {
		log.Fatalf("unexpected arguments: %v", flag.Args())
	}

	return opts
}

func validateConsumerOptions(opts consumerOptions) error {
	switch {
	case opts.messages <= 0:
		return fmt.Errorf("messages must be greater than zero")
	case opts.size < 0:
		return fmt.Errorf("size must not be negative")
	case opts.partitions <= 0:
		return fmt.Errorf("partitions must be greater than zero")
	case opts.inFlight <= 0:
		return fmt.Errorf("in-flight must be greater than zero")
	case opts.timeoutMS <= 0:
		return fmt.Errorf("timeout-ms must be greater than zero")
	case opts.consumersPerPartition <= 0:
		return fmt.Errorf("consumers-per-partition must be greater than zero")
	case opts.subscriptionType != "shared" &&
		opts.subscriptionType != "exclusive" &&
		opts.subscriptionType != "failover" &&
		opts.subscriptionType != "key-shared":
		return fmt.Errorf("subscription-type must be one of: shared, exclusive, failover, key-shared")
	default:
		return nil
	}
}

func runConsumer(opts consumerOptions) (consumerResult, int, error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               opts.url,
		OperationTimeout:  time.Duration(opts.timeoutMS) * time.Millisecond,
		ConnectionTimeout: time.Duration(opts.timeoutMS) * time.Millisecond,
	})
	if err != nil {
		return consumerResult{}, 1, fmt.Errorf("create Pulsar client: %w", err)
	}
	defer client.Close()

	consumers := make([]pulsar.Consumer, 0, opts.consumersPerPartition)
	for index := 0; index < opts.consumersPerPartition; index++ {
		consumer, subscribeErr := client.Subscribe(pulsar.ConsumerOptions{
			Topic:                       opts.topic,
			SubscriptionName:            opts.subscriptionName,
			SubscriptionProperties:      map[string]string{"benchmark-runner": "go"},
			Type:                        subscriptionType(opts.subscriptionType),
			SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
			ReceiverQueueSize:           opts.inFlight,
			Name:                        fmt.Sprintf("%s-%d", opts.subscriptionName, index),
		})
		if subscribeErr != nil {
			for _, existing := range consumers {
				existing.Close()
			}
			return consumerResult{}, 1, fmt.Errorf("create consumer: %w", subscribeErr)
		}
		consumers = append(consumers, consumer)
	}
	defer func() {
		for _, consumer := range consumers {
			consumer.Close()
		}
	}()

	startedAt := time.Now()
	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(opts.timeoutMS)*time.Millisecond,
	)
	defer cancel()

	var received int64
	var acked int64
	var errors int64
	var latenciesMu sync.Mutex
	latencies := make([]int64, 0, opts.messages)
	var workers sync.WaitGroup
	workers.Add(len(consumers))

	for _, consumer := range consumers {
		go func(consumer pulsar.Consumer) {
			defer workers.Done()

			for {
				if atomic.LoadInt64(&received) >= int64(opts.messages) {
					return
				}

				message, receiveErr := consumer.Receive(ctx)
				if receiveErr != nil {
					if ctx.Err() == nil {
						atomic.AddInt64(&errors, 1)
					}
					return
				}

				index := atomic.AddInt64(&received, 1)
				if index > int64(opts.messages) {
					_ = consumer.Ack(message)
					return
				}

				if publishNs, ok := message.Properties()["benchmark-publish-ns"]; ok {
					if parsed, parseErr := strconv.ParseInt(publishNs, 10, 64); parseErr == nil {
						latency := maxInt64(time.Now().UnixNano()-parsed, 0) / 1_000
						latenciesMu.Lock()
						latencies = append(latencies, latency)
						latenciesMu.Unlock()
					}
				}

				if ackErr := consumer.Ack(message); ackErr != nil {
					atomic.AddInt64(&errors, 1)
				} else {
					atomic.AddInt64(&acked, 1)
				}

				if index == int64(opts.messages) {
					cancel()
					return
				}
			}
		}(consumer)
	}

	workers.Wait()
	durationUS := maxInt64(time.Since(startedAt).Microseconds(), 1)
	receivedCount := int(atomic.LoadInt64(&received))
	ackedCount := int(atomic.LoadInt64(&acked))
	errorCount := int(atomic.LoadInt64(&errors))
	result := consumerResult{
		SchemaVersion:         1,
		Operation:             "consumer",
		Client:                "go",
		Topic:                 opts.topic,
		MessagesRequested:     opts.messages,
		MessagesReceived:      minInt(receivedCount, opts.messages),
		MessagesAcked:         ackedCount,
		PayloadBytes:          opts.size,
		Partitions:            opts.partitions,
		SubscriptionType:      opts.subscriptionType,
		SubscriptionName:      opts.subscriptionName,
		ConsumersPerPartition: opts.consumersPerPartition,
		DurationUS:            durationUS,
		MessagesPerSecond:     rate(float64(minInt(receivedCount, opts.messages)), durationUS),
		BytesPerSecond:        rate(float64(minInt(receivedCount, opts.messages)*opts.size), durationUS),
		LatencyType:           "consumer_e2e",
		P50US:                 percentile(latencies, 0.50),
		P95US:                 percentile(latencies, 0.95),
		P99US:                 percentile(latencies, 0.99),
		Errors:                errorCount,
	}

	if result.MessagesReceived != opts.messages || result.MessagesAcked != opts.messages || errorCount > 0 {
		return result, 1, nil
	}

	return result, 0, nil
}

func subscriptionType(value string) pulsar.SubscriptionType {
	switch value {
	case "exclusive":
		return pulsar.Exclusive
	case "failover":
		return pulsar.Failover
	case "key-shared":
		return pulsar.KeyShared
	default:
		return pulsar.Shared
	}
}

func percentile(values []int64, quantile float64) *int64 {
	if len(values) == 0 {
		return nil
	}

	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(left, right int) bool {
		return sorted[left] < sorted[right]
	})
	index := maxInt(int(math.Ceil(float64(len(sorted))*quantile))-1, 0)
	value := sorted[index]
	return &value
}

func rate(value float64, durationUS int64) float64 {
	return math.Round(value*1_000_000/float64(durationUS)*1_000) / 1_000
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}
