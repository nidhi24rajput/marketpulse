// Command processor is the Stream Processor service.
// It consumes events from Kafka, persists them to MongoDB,
// and maintains real-time aggregation state in Redis.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/nidhi24rajput/marketpulse/internal/config"
	mkafka "github.com/nidhi24rajput/marketpulse/internal/kafka"
	"github.com/nidhi24rajput/marketpulse/internal/mongodb"
	mredis "github.com/nidhi24rajput/marketpulse/internal/redis"
)

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync()

	cfg, err := config.Load()
	if err != nil {
		log.Fatal("config load failed", zap.Error(err))
	}

	// ── MongoDB ───────────────────────────────────────────────────────────────
	mongo, err := mongodb.New(cfg.Mongo.URI, cfg.Mongo.Database)
	if err != nil {
		log.Fatal("mongodb connect failed", zap.Error(err))
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*cfg.App.ShutdownTimeout)
		defer cancel()
		mongo.Disconnect(ctx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*cfg.App.ShutdownTimeout)
	if err := mongo.EnsureIndexes(ctx); err != nil {
		log.Warn("index creation failed", zap.Error(err))
	}
	cancel()

	// ── Redis ─────────────────────────────────────────────────────────────────
	redisClient, err := mredis.New(cfg.Redis)
	if err != nil {
		log.Fatal("redis connect failed", zap.Error(err))
	}
	defer redisClient.Close()

	// ── Repositories ──────────────────────────────────────────────────────────
	eventRepo := mongodb.NewEventRepo(mongo)
	orderRepo := mongodb.NewOrderRepo(mongo)
	aggRepo := mongodb.NewAggregateRepo(mongo)

	// ── Kafka Consumer ────────────────────────────────────────────────────────
	consumer, err := mkafka.NewConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.ConsumerGroup,
		[]string{cfg.Kafka.EventTopic},
		log,
	)
	if err != nil {
		log.Fatal("kafka consumer failed", zap.Error(err))
	}
	defer consumer.Close()

	// ── Processor ────────────────────────────────────────────────────────────
	processor := NewProcessor(eventRepo, orderRepo, aggRepo, redisClient, log)

	// ── Run ───────────────────────────────────────────────────────────────────
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	g, ctx := errgroup.WithContext(rootCtx)

	g.Go(func() error {
		log.Info("processor starting")
		return consumer.Consume(ctx, processor.Handle)
	})

	if err := g.Wait(); err != nil {
		log.Error("processor exited with error", zap.Error(err))
	}
	log.Info("processor stopped")
}
