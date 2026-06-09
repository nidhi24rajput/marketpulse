// Package analytics implements the business logic for analytics queries.
// It sits between HTTP handlers and repositories, adding caching and
// computed fields (e.g. conversion rates, revenue trends).
package analytics

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/nidhi24rajput/marketpulse/internal/domain"
	"github.com/nidhi24rajput/marketpulse/internal/mongodb"
	mredis "github.com/nidhi24rajput/marketpulse/internal/redis"
)

// Service provides analytics queries with Redis caching.
type Service struct {
	events *mongodb.EventRepo
	orders *mongodb.OrderRepo
	aggs   *mongodb.AggregateRepo
	redis  *mredis.Client
	log    *zap.Logger
}

func NewService(
	events *mongodb.EventRepo,
	orders *mongodb.OrderRepo,
	aggs *mongodb.AggregateRepo,
	redis *mredis.Client,
	log *zap.Logger,
) *Service {
	return &Service{events: events, orders: orders, aggs: aggs, redis: redis, log: log}
}

// RevenueOverview returns daily revenue aggregates for the given date range.
func (s *Service) RevenueOverview(ctx context.Context, storeID string, from, to time.Time) ([]*domain.DailyAggregate, error) {
	cacheKey := fmt.Sprintf("analytics:revenue:%s:%s:%s", storeID, from.Format("20060102"), to.Format("20060102"))

	var cached []*domain.DailyAggregate
	if hit, _ := s.redis.Get(ctx, cacheKey, &cached); hit {
		return cached, nil
	}

	result, err := s.aggs.GetDailyAggregates(ctx, storeID, from, to)
	if err != nil {
		return nil, err
	}

	_ = s.redis.SetWithTTL(ctx, cacheKey, result, 2*time.Minute)
	return result, nil
}

// TopProducts returns the highest-revenue products for the given period.
func (s *Service) TopProducts(ctx context.Context, storeID string, from, to time.Time, limit int) ([]*domain.TopProduct, error) {
	cacheKey := fmt.Sprintf("analytics:top_products:%s:%s:%s:%d", storeID, from.Format("20060102"), to.Format("20060102"), limit)

	var cached []*domain.TopProduct
	if hit, _ := s.redis.Get(ctx, cacheKey, &cached); hit {
		return cached, nil
	}

	result, err := s.aggs.GetTopProducts(ctx, storeID, from, to, limit)
	if err != nil {
		return nil, err
	}

	_ = s.redis.SetWithTTL(ctx, cacheKey, result, 5*time.Minute)
	return result, nil
}

// ConversionFunnel computes the purchase funnel conversion rates.
func (s *Service) ConversionFunnel(ctx context.Context, storeID string, from, to time.Time) (*domain.Funnel, error) {
	cacheKey := fmt.Sprintf("analytics:funnel:%s:%s:%s", storeID, from.Format("20060102"), to.Format("20060102"))

	var cached domain.Funnel
	if hit, _ := s.redis.Get(ctx, cacheKey, &cached); hit {
		return &cached, nil
	}

	steps := []struct {
		name      string
		eventType domain.EventType
	}{
		{"Page View", domain.EventTypePageView},
		{"Product View", domain.EventTypeProductView},
		{"Add to Cart", domain.EventTypeAddToCart},
		{"Checkout Started", domain.EventTypeCheckoutStart},
		{"Order Placed", domain.EventTypeOrderPlaced},
	}

	funnelSteps := make([]domain.FunnelStep, 0, len(steps))
	var prevCount int64

	for i, step := range steps {
		count, err := s.events.CountByType(ctx, storeID, step.eventType, from, to)
		if err != nil {
			return nil, fmt.Errorf("funnel: count %s failed: %w", step.name, err)
		}

		conversion := 0.0
		if i == 0 {
			conversion = 100.0
		} else if prevCount > 0 {
			conversion = float64(count) / float64(prevCount) * 100
		}

		funnelSteps = append(funnelSteps, domain.FunnelStep{
			Step:       step.name,
			EventType:  step.eventType,
			Count:      count,
			Conversion: conversion,
		})
		prevCount = count
	}

	funnel := &domain.Funnel{
		StoreID:   storeID,
		StartDate: from,
		EndDate:   to,
		Steps:     funnelSteps,
	}

	_ = s.redis.SetWithTTL(ctx, cacheKey, funnel, 3*time.Minute)
	return funnel, nil
}

// RealtimeStats reads live counters from Redis.
func (s *Service) RealtimeStats(ctx context.Context, storeID string) (*domain.RealtimeStats, error) {
	stats := &domain.RealtimeStats{
		StoreID:   storeID,
		UpdatedAt: time.Now().UTC(),
	}

	eventKey := fmt.Sprintf("rt:events:%s", storeID)
	orderKey := fmt.Sprintf("rt:orders:%s", storeID)
	sessionKey := fmt.Sprintf("rt:sessions:%s", storeID)

	var eventCount, orderCount int64
	s.redis.Get(ctx, eventKey, &eventCount)  //nolint:errcheck
	s.redis.Get(ctx, orderKey, &orderCount)  //nolint:errcheck

	sessions, _ := s.redis.SCard(ctx, sessionKey)

	stats.EventsLast5Min = eventCount
	stats.OrdersLast5Min = orderCount
	stats.ActiveSessions = sessions

	return stats, nil
}

// TopSearchTerms returns frequently searched queries.
func (s *Service) TopSearchTerms(ctx context.Context, storeID string, from, to time.Time, limit int) ([]*domain.SearchTerm, error) {
	cacheKey := fmt.Sprintf("analytics:searches:%s:%s:%s", storeID, from.Format("20060102"), to.Format("20060102"))

	var cached []*domain.SearchTerm
	if hit, _ := s.redis.Get(ctx, cacheKey, &cached); hit {
		return cached, nil
	}

	result, err := s.aggs.GetTopSearchTerms(ctx, storeID, from, to, limit)
	if err != nil {
		return nil, err
	}

	_ = s.redis.SetWithTTL(ctx, cacheKey, result, 5*time.Minute)
	return result, nil
}
