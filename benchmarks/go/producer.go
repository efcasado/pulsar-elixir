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
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type options struct {
	url          string
	topic        string
	messages     int
	size         int
	partitions   int
	inFlight     int
	timeoutMS    int
	batching     bool
	batchSize    int
	batchDelayMS int
	compression  string
	payloadMode  string
}

type ackResult struct {
	startedAt time.Time
	err       error
}

type stats struct {
	latencies []int64
	acked     int
	errors    int
}

type result struct {
	SchemaVersion     int     `json:"schema_version"`
	Operation         string  `json:"operation"`
	Client            string  `json:"client"`
	Topic             string  `json:"topic"`
	MessagesRequested int     `json:"messages_requested"`
	MessagesAcked     int     `json:"messages_acked"`
	PayloadBytes      int     `json:"payload_bytes"`
	Partitions        int     `json:"partitions"`
	InFlight          int     `json:"in_flight"`
	Batching          bool    `json:"batching"`
	BatchSize         int     `json:"batch_size"`
	BatchDelayMS      int     `json:"batch_delay_ms"`
	Compression       string  `json:"compression"`
	PayloadMode       string  `json:"payload_mode"`
	DurationUS        int64   `json:"duration_us"`
	MessagesPerSecond float64 `json:"messages_per_second"`
	BytesPerSecond    float64 `json:"bytes_per_second"`
	LatencyType       string  `json:"latency_type"`
	P50US             *int64  `json:"p50_us"`
	P95US             *int64  `json:"p95_us"`
	P99US             *int64  `json:"p99_us"`
	Errors            int     `json:"errors"`
}

func main() {
	opts := parseOptions()
	if err := validateOptions(opts); err != nil {
		log.Print(err)
		os.Exit(2)
	}

	output, exitCode, err := run(opts)
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

func parseOptions() options {
	var opts options

	flag.StringVar(&opts.url, "url", "pulsar://localhost:6650", "Pulsar broker URL")
	flag.StringVar(&opts.topic, "topic", "persistent://public/default/benchmark-producer", "Pulsar topic")
	flag.IntVar(&opts.messages, "messages", 100000, "number of messages")
	flag.IntVar(&opts.size, "size", 1024, "payload size in bytes")
	flag.IntVar(&opts.partitions, "partitions", 1, "number of topic partitions")
	flag.IntVar(&opts.inFlight, "in-flight", 1000, "maximum pending messages per producer")
	flag.IntVar(&opts.timeoutMS, "timeout-ms", 30000, "acknowledgement timeout in milliseconds")
	flag.BoolVar(&opts.batching, "batching", false, "enable producer batching")
	flag.IntVar(&opts.batchSize, "batch-size", 100, "maximum messages per batch")
	flag.IntVar(&opts.batchDelayMS, "batch-delay-ms", 10, "maximum batch publish delay in milliseconds")
	flag.StringVar(&opts.compression, "compression", "none", "compression: none or zstd")
	flag.StringVar(&opts.payloadMode, "payload", "zero", "payload mode: zero or high-entropy")
	flag.Parse()

	if flag.NArg() != 0 {
		log.Fatalf("unexpected arguments: %v", flag.Args())
	}

	return opts
}

func validateOptions(opts options) error {
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
	case opts.batchSize <= 0:
		return fmt.Errorf("batch-size must be greater than zero")
	case opts.batchDelayMS <= 0:
		return fmt.Errorf("batch-delay-ms must be greater than zero")
	case opts.compression != "none" && opts.compression != "zstd":
		return fmt.Errorf("compression must be one of: none, zstd")
	case opts.payloadMode != "zero" && opts.payloadMode != "high-entropy":
		return fmt.Errorf("payload must be one of: zero, high-entropy")
	default:
		return nil
	}
}

func run(opts options) (result, int, error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               opts.url,
		OperationTimeout:  time.Duration(opts.timeoutMS) * time.Millisecond,
		ConnectionTimeout: time.Duration(opts.timeoutMS) * time.Millisecond,
	})
	if err != nil {
		return result{}, 1, fmt.Errorf("create Pulsar client: %w", err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:                   opts.topic,
		Name:                    "benchmark-go-producer",
		MaxPendingMessages:      opts.inFlight,
		DisableBlockIfQueueFull: true,
		DisableBatching:         !opts.batching,
		BatchingMaxMessages:     uint(opts.batchSize),
		BatchingMaxPublishDelay: time.Duration(opts.batchDelayMS) * time.Millisecond,
		SendTimeout:             time.Duration(opts.timeoutMS) * time.Millisecond,
		CompressionType:         compressionType(opts.compression),
	})
	if err != nil {
		return result{}, 1, fmt.Errorf("create producer: %w", err)
	}
	defer producer.Close()

	startedAt := time.Now()
	combined := publish(producer, opts.messages, opts)

	durationUS := maxInt64(time.Since(startedAt).Microseconds(), 1)
	output := result{
		SchemaVersion:     1,
		Operation:         "producer",
		Client:            "go",
		Topic:             opts.topic,
		MessagesRequested: opts.messages,
		MessagesAcked:     combined.acked,
		PayloadBytes:      opts.size,
		Partitions:        opts.partitions,
		InFlight:          opts.inFlight,
		Batching:          opts.batching,
		BatchSize:         opts.batchSize,
		BatchDelayMS:      opts.batchDelayMS,
		Compression:       opts.compression,
		PayloadMode:       opts.payloadMode,
		DurationUS:        durationUS,
		MessagesPerSecond: rate(float64(combined.acked), durationUS),
		BytesPerSecond:    rate(float64(combined.acked*opts.size), durationUS),
		LatencyType:       "producer_ack",
		P50US:             percentile(combined.latencies, 0.50),
		P95US:             percentile(combined.latencies, 0.95),
		P99US:             percentile(combined.latencies, 0.99),
		Errors:            combined.errors,
	}

	if combined.acked != opts.messages || combined.errors > 0 {
		return output, 1, nil
	}

	return output, 0, nil
}

func publish(producer pulsar.Producer, count int, opts options) stats {
	output := stats{}
	payload := payloadBytes(opts.size, opts.payloadMode)
	ctx := context.Background()

	for start := 0; start < count; start += opts.inFlight {
		end := minInt(start+opts.inFlight, count)
		pending := make(chan ackResult, end-start)

		for index := start; index < end; index++ {
			startedAt := time.Now()
			producer.SendAsync(ctx, &pulsar.ProducerMessage{
				Payload: payload,
			}, func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
				pending <- ackResult{startedAt: startedAt, err: err}
			})
		}

		deadline := time.NewTimer(time.Duration(opts.timeoutMS) * time.Millisecond)
		for received := 0; received < end-start; received++ {
			select {
			case acknowledgement := <-pending:
				if acknowledgement.err != nil {
					output.errors++
					continue
				}
				output.acked++
				output.latencies = append(
					output.latencies,
					time.Since(acknowledgement.startedAt).Microseconds(),
				)
			case <-deadline.C:
				output.errors += end - start - received
				deadline.Stop()
				return output
			}
		}
		deadline.Stop()
	}

	return output
}

func compressionType(value string) pulsar.CompressionType {
	if value == "zstd" {
		return pulsar.ZSTD
	}
	return pulsar.NoCompression
}

func payloadBytes(size int, mode string) []byte {
	payload := make([]byte, size)
	if mode == "zero" {
		return payload
	}

	const seed uint32 = 0xA5A5A5A5
	state := seed
	for index := range payload {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		payload[index] = byte(state >> 24)
	}
	return payload
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
