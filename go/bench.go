package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"

	goavro "github.com/linkedin/goavro/v2"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	Mode        string
	Messages    int
	Batch       int
	Compression string
	Brokers     string
	Topic       string
	Large       bool
}

type Result struct {
	Lang          string  `json:"lang"`
	Mode          string  `json:"mode"`
	Topic         string  `json:"topic"`
	Produced      int     `json:"produced"`
	DurationSec   float64 `json:"durationSec"`
	Rate          float64 `json:"rate"`
	Consumed      int     `json:"consumed"`
	StringLenSum  int64   `json:"stringLenSum"`
	ConsumeDurSec float64 `json:"consumeDurSec"`
}

var textMessages = []string{
	"Hello world",
	"Quick brown fox jumps over the lazy dog",
	"Kafka benchmarking message",
	"Avro vs JSON throughput test",
	"Message compression experiment",
}

func parseFlags() Config {
	cfg := Config{}
	flag.StringVar(&cfg.Mode, "mode", "", "Mode: json|avro")
	flag.IntVar(&cfg.Messages, "messages", 100000, "Number of messages to produce")
	flag.IntVar(&cfg.Batch, "batch", 100, "Batch size for producer send")
	flag.StringVar(&cfg.Compression, "compression", "gzip", "Compression: none|gzip|snappy|lz4")
	flag.StringVar(&cfg.Brokers, "brokers", "localhost:9094", "Comma-separated list of bootstrap brokers")
	flag.StringVar(&cfg.Topic, "topic", "", "Topic to use (defaults to bench-json/bench-avro)")
	flag.BoolVar(&cfg.Large, "large", false, "Use large messages (1000x)")
	flag.Parse()

	return cfg
}

func defaultTopic(cfg Config) string {
	if cfg.Topic != "" {
		return cfg.Topic
	}
	if cfg.Mode == "avro" {
		return "bench-avro"
	}
	return "bench-json"
}

func buildRecord(i int, large bool) map[string]interface{} {
	// Rotate messages deterministically
	var msg string
	if large {
		msg = strings.Repeat(textMessages[i%len(textMessages)], 1000)
	} else {
		msg = textMessages[i%len(textMessages)]
	}
	return map[string]interface{}{
		"id":         fmt.Sprintf("%d", i),
		"ts_unix_ms": time.Now().UnixMilli(),
		"category":   "bench",
		"value":      float64((i%1000))/1000.0 + rand.Float64()*0.000001, // slight variation
		"message":    msg,
	}
}

func loadAvroCodec() (*goavro.Codec, error) {
	// schema path: ../schemas/event.avsc relative to this file
	exe, _ := os.Executable()
	base := filepath.Dir(filepath.Dir(exe))
	schemaPath := filepath.Join(base, "schemas", "event.avsc")
	// Fallback to repo-relative when running via `go run go/bench.go`
	if _, err := os.Stat(schemaPath); err != nil {
		// try repo root based on current working dir
		cwd, _ := os.Getwd()
		alt := filepath.Join(cwd, "schemas", "event.avsc")
		if _, err2 := os.Stat(alt); err2 == nil {
			schemaPath = alt
		}
	}
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, err
	}
	return goavro.NewCodec(string(data))
}

func serialize(record map[string]interface{}, mode string, codec *goavro.Codec) ([]byte, error) {
	if mode == "json" {
		return json.Marshal(record)
	}
	// goavro expects native map[string]interface{}
	return codec.BinaryFromNative(nil, record)
}

func deserialize(data []byte, mode string, codec *goavro.Codec) (map[string]interface{}, error) {
	if mode == "json" {
		var m map[string]interface{}
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		return m, nil
	}
	native, _, err := codec.NativeFromBinary(data)
	if err != nil {
		return nil, err
	}
	mp, ok := native.(map[string]interface{})
	if !ok {
		return nil, errors.New("invalid avro native type")
	}
	return mp, nil
}

func compressionCodec(name string) kafka.Compression {
	switch strings.ToLower(name) {
	case "gzip":
		return kafka.Gzip
	case "snappy":
		return kafka.Snappy
	case "lz4":
		return kafka.Lz4
	default:
		return 0
	}
}

func main() {
	cfg := parseFlags()
	if cfg.Mode != "json" && cfg.Mode != "avro" {
		fmt.Fprintln(os.Stderr, "--mode must be 'json' or 'avro'")
		os.Exit(1)
	}

	topic := defaultTopic(cfg)
	brokers := strings.Split(cfg.Brokers, ",")

	// Prepare Avro codec if needed
	var codec *goavro.Codec
	var err error
	if cfg.Mode == "avro" {
		codec, err = loadAvroCodec()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to load avro schema: %v\n", err)
			os.Exit(1)
		}
	}

	// ---------------------------------------------------------------------
	// Fast consumer: per-partition readers from current high watermark
	// ---------------------------------------------------------------------
	dialer := &kafka.Dialer{Timeout: 10 * time.Second}
	ctrl, err := dialer.DialContext(context.Background(), "tcp", brokers[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to dial broker: %v\n", err)
		os.Exit(1)
	}
	parts, err := ctrl.ReadPartitions(topic)
	_ = ctrl.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read partitions: %v\n", err)
		os.Exit(1)
	}

	// Gather high watermarks per partition
	type partStart struct {
		id     int
		offset int64
	}
	var starts []partStart
	for _, p := range parts {
		if p.Topic != topic {
			continue
		}
		leader, err := dialer.DialLeader(context.Background(), "tcp", brokers[0], topic, p.ID)
		if err != nil {
			continue
		}
		first, last, err := leader.ReadOffsets()
		_ = leader.Close()
		_ = first // not used, but kept for clarity
		if err != nil {
			continue
		}
		starts = append(starts, partStart{id: p.ID, offset: last})
	}

	var consumedCount int64
	var stringLenSum int64
	var consumeStart int64
	var consumeEnd int64

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		// spawn a reader per partition
		readers := make([]*kafka.Reader, 0, len(starts))
		for _, s := range starts {
			r := kafka.NewReader(kafka.ReaderConfig{
				Brokers:     brokers,
				Topic:       topic,
				Partition:   s.id,
				MinBytes:    1,
				MaxBytes:    10 * 1024 * 1024,
				StartOffset: s.offset,
				Dialer:      dialer,
			})
			readers = append(readers, r)
		}
		// fan-in reads
		for len(readers) > 0 {
			for i := 0; i < len(readers); i++ {
				m, err := readers[i].ReadMessage(ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						for _, r := range readers {
							_ = r.Close()
						}
						return
					}
					continue
				}
				if len(m.Value) == 0 {
					continue
				}
				if atomic.LoadInt64(&consumeStart) == 0 {
					atomic.StoreInt64(&consumeStart, time.Now().UnixMilli())
				}
				rec, err := deserialize(m.Value, cfg.Mode, codec)
				if err == nil {
					if msg, ok := rec["message"].(string); ok {
						atomic.AddInt64(&stringLenSum, int64(len(msg)))
					}
				}
				if atomic.AddInt64(&consumedCount, 1) >= int64(cfg.Messages) {
					atomic.StoreInt64(&consumeEnd, time.Now().UnixMilli())
					for _, r := range readers {
						_ = r.Close()
					}
					return
				}
			}
		}
	}()

	// ---------------------------------------------------------------------
	// Producer: batch sends
	// ---------------------------------------------------------------------
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		RequiredAcks: kafka.RequireOne,
		Balancer:     &kafka.Hash{},
		BatchSize:    cfg.Batch,
		Compression:  compressionCodec(cfg.Compression),
		Async:        true,
	}
	defer writer.Close()

	produceStart := time.Now()
	produced := 0
	messages := make([]kafka.Message, 0, cfg.Batch)
	for i := 0; i < cfg.Messages; i++ {
		rec := buildRecord(i, cfg.Large)
		val, err := serialize(rec, cfg.Mode, codec)
		if err != nil {
			// skip problematic serialization
			continue
		}
		messages = append(messages, kafka.Message{Value: val})
		if len(messages) >= cfg.Batch {
			if err := writer.WriteMessages(context.Background(), messages...); err == nil {
				produced += len(messages)
			}
			messages = messages[:0]
		}
	}
	if len(messages) > 0 {
		if err := writer.WriteMessages(context.Background(), messages...); err == nil {
			produced += len(messages)
		}
	}
	produceEnd := time.Now()

	// Wait for consumer to finish or timeout as safeguard
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		cancel()
	}
	cancel()

	// ---------------------------------------------------------------------
	// Output results
	// ---------------------------------------------------------------------
	produceDur := produceEnd.Sub(produceStart).Seconds()
	var consumeDur float64
	if s := atomic.LoadInt64(&consumeStart); s != 0 {
		if e := atomic.LoadInt64(&consumeEnd); e != 0 && e >= s {
			consumeDur = float64(e-s) / 1000.0
		}
	}

	res := Result{
		Lang:          "go",
		Mode:          cfg.Mode,
		Topic:         topic,
		Produced:      produced,
		DurationSec:   produceDur,
		Rate:          float64(produced) / maxFloat(produceDur, 0.000001),
		Consumed:      int(atomic.LoadInt64(&consumedCount)),
		StringLenSum:  atomic.LoadInt64(&stringLenSum),
		ConsumeDurSec: consumeDur,
	}

	out, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(out))
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
