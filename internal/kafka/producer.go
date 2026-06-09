// Package kafka wraps segmentio/kafka-go with domain-aware helpers.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Producer wraps a kafka.Writer with structured publishing helpers.
type Producer struct {
	writer *kafka.Writer
	log    *zap.Logger
}

// NewProducer creates a new Kafka producer connected to the given brokers.
func NewProducer(brokers string, log *zap.Logger) (*Producer, error) {
	brokerList := strings.Split(brokers, ",")

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:          brokerList,
		Balancer:         &kafka.Hash{}, // route by key (store_id) for partition affinity
		WriteTimeout:     10 * time.Second,
		ReadTimeout:      10 * time.Second,
		RequiredAcks:     int(kafka.RequireAll), // wait for full ISR
		Async:            false,
		CompressionCodec: kafka.Snappy.Codec(),
		BatchSize:        100,
		BatchTimeout:     5 * time.Millisecond,
	})

	return &Producer{writer: w, log: log}, nil
}

// Publish serialises v as JSON and sends it to the given topic.
// key is used for partition routing — use store_id to keep a store's events ordered.
func (p *Producer) Publish(ctx context.Context, topic, key string, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("kafka: marshal failed: %w", err)
	}

	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: payload,
		Time:  time.Now(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka: publish failed: %w", err)
	}
	return nil
}

// Close shuts down the producer gracefully, flushing pending messages.
func (p *Producer) Close() {
	if err := p.writer.Close(); err != nil {
		p.log.Warn("kafka producer close error", zap.Error(err))
	}
}
