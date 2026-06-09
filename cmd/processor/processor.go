package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/nidhi24rajput/marketpulse/internal/domain"
	"github.com/nidhi24rajput/marketpulse/internal/mongodb"
	mredis "github.com/nidhi24rajput/marketpulse/internal/redis"
)

// Processor handles Kafka messages and writes to MongoDB + Redis.
type Processor struct {
	events *mongodb.EventRepo
	orders *mongodb.OrderRepo
	aggs   *mongodb.AggregateRepo
	redis  *mredis.Client
	log    *zap.Logger
}

func NewProcessor(
	events *mongodb.EventRepo,
	orders *mongodb.OrderRepo,
	aggs *mongodb.AggregateRepo,
	redis *mredis.Client,
	log *zap.Logger,
) *Processor {
	return &Processor{events: events, orders: orders, aggs: aggs, redis: redis, log: log}
}

// Handle is the MessageHandler callback invoked by the Kafka consumer.
func (p *Processor) Handle(ctx context.Context, msg kafka.Message) error {
	var event domain.Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		// Bad message — log and skip (don't block consumer)
		p.log.Warn("failed to decode event message",
			zap.ByteString("key", msg.Key),
			zap.Error(err),
		)
		return nil
	}

	// Persist the raw event
	if err := p.events.Save(ctx, &event); err != nil {
		return fmt.Errorf("processor: save event failed: %w", err)
	}

	// Dispatch additional processing based on event type
	switch event.Type {
	case domain.EventTypeOrderPlaced, domain.EventTypeOrderPaid:
		if err := p.processOrder(ctx, &event); err != nil {
			p.log.Warn("order processing failed", zap.String("event_id", event.ID), zap.Error(err))
		}
	default:
	}

	// Update real-time counters in Redis
	if err := p.updateRealtime(ctx, &event); err != nil {
		p.log.Warn("realtime update failed", zap.String("event_id", event.ID), zap.Error(err))
	}

	// Update daily aggregate (best-effort, non-blocking)
	go func() {
		aggCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := p.updateDailyAggregate(aggCtx, &event); err != nil {
			p.log.Warn("daily aggregate update failed", zap.Error(err))
		}
	}()

	return nil
}

// processOrder extracts the nested Order from event properties and persists it.
func (p *Processor) processOrder(ctx context.Context, event *domain.Event) error {
	orderData, ok := event.Properties["order"]
	if !ok {
		return nil
	}

	// Re-marshal and decode to get a typed Order struct
	raw, err := json.Marshal(orderData)
	if err != nil {
		return err
	}
	var order domain.Order
	if err := json.Unmarshal(raw, &order); err != nil {
		return err
	}

	return p.orders.Save(ctx, &order)
}

// updateRealtime increments Redis counters for live dashboard data.
func (p *Processor) updateRealtime(ctx context.Context, event *domain.Event) error {
	pipe := p.redis.Pipeline(ctx)

	key := fmt.Sprintf("rt:events:%s", event.StoreID)
	_, _ = p.redis.IncrWithExpiry(ctx, key, 5*time.Minute)

	// Track active sessions (5-min sliding window)
	sessionKey := fmt.Sprintf("rt:sessions:%s", event.StoreID)
	_ = p.redis.AddToSetWithExpiry(ctx, sessionKey, event.SessionID, 5*time.Minute)

	if event.Type == domain.EventTypeOrderPlaced || event.Type == domain.EventTypeOrderPaid {
		orderKey := fmt.Sprintf("rt:orders:%s", event.StoreID)
		_, _ = p.redis.IncrWithExpiry(ctx, orderKey, 5*time.Minute)
	}

	_ = pipe
	return nil
}

// updateDailyAggregate performs an upsert on the daily rollup document.
func (p *Processor) updateDailyAggregate(ctx context.Context, event *domain.Event) error {
	day := event.Timestamp.Truncate(24 * time.Hour)
	id := fmt.Sprintf("%s:%s", event.StoreID, day.Format("2006-01-02"))

	// Fetch current aggregate or start fresh
	aggs, err := p.aggs.GetDailyAggregates(ctx, event.StoreID, day, day.Add(24*time.Hour))
	if err != nil {
		return err
	}

	var agg *domain.DailyAggregate
	if len(aggs) > 0 {
		agg = aggs[0]
	} else {
		agg = &domain.DailyAggregate{
			ID:      id,
			StoreID: event.StoreID,
			Date:    day,
		}
	}

	// Increment the relevant counter
	switch event.Type {
	case domain.EventTypePageView:
		agg.PageViews++
	case domain.EventTypeAddToCart:
		agg.AddToCarts++
	case domain.EventTypeOrderPlaced, domain.EventTypeOrderPaid:
		agg.Orders++
		if orderData, ok := event.Properties["order"]; ok {
			raw, _ := json.Marshal(orderData)
			var order domain.Order
			if err := json.Unmarshal(raw, &order); err == nil {
				agg.Revenue += order.Total
			}
		}
	default:
	}

	return p.aggs.UpsertDailyAggregate(ctx, agg)
}
