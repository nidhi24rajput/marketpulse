package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// MessageHandler is a callback invoked for each consumed message.
// Return an error to skip committing the offset (message will be redelivered).
type MessageHandler func(ctx context.Context, msg kafka.Message) error

// Consumer wraps a kafka.Reader with graceful shutdown support.
type Consumer struct {
	reader *kafka.Reader
	log    *zap.Logger
}

// NewConsumer creates a consumer subscribed to the given topic via a consumer group.
func NewConsumer(brokers, groupID string, topics []string, log *zap.Logger) (*Consumer, error) {
	if len(topics) == 0 {
		return nil, fmt.Errorf("kafka: at least one topic required")
	}

	brokerList := strings.Split(brokers, ",")

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokerList,
		GroupID:        groupID,
		Topic:          topics[0], // segmentio/kafka-go uses one reader per topic
		MinBytes:       1024,      // 1KB
		MaxBytes:       10e6,      // 10MB
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0,         // manual commit — we call CommitMessages explicitly
		StartOffset:    kafka.FirstOffset,
		// Retry on temporary errors
		MaxAttempts: 3,
	})

	return &Consumer{reader: r, log: log}, nil
}

// Consume runs the poll loop until ctx is cancelled.
// Offsets are committed only after the handler returns nil (at-least-once delivery).
func (c *Consumer) Consume(ctx context.Context, handler MessageHandler) error {
	c.log.Info("kafka consumer started",
		zap.String("topic", c.reader.Config().Topic),
		zap.String("group", c.reader.Config().GroupID),
	)

	for {
		// FetchMessage does NOT auto-commit — we must call CommitMessages after success.
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// Context cancelled — clean shutdown
				c.log.Info("kafka consumer stopping")
				return nil
			}
			c.log.Error("kafka fetch error", zap.Error(err))
			continue
		}

		if err := handler(ctx, msg); err != nil {
			c.log.Error("message handler error",
				zap.String("topic", msg.Topic),
				zap.Int("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
			// Do not commit — message will be redelivered
			// TODO: add max-retry counter and route to dead-letter topic
			continue
		}

		// Commit only after successful processing
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.log.Warn("kafka commit failed", zap.Error(err))
		}
	}
}

// Close shuts down the consumer cleanly.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
