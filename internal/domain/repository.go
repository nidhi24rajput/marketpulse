package domain

import (
	"context"
	"time"
)

// EventRepository is the storage contract for raw events.
// Concrete implementations live in internal/mongodb.
type EventRepository interface {
	Save(ctx context.Context, event *Event) error
	SaveBatch(ctx context.Context, events []*Event) error
	FindBySessionID(ctx context.Context, sessionID string) ([]*Event, error)
	FindByStoreAndTimeRange(ctx context.Context, storeID string, from, to time.Time, limit int) ([]*Event, error)
	CountByType(ctx context.Context, storeID string, eventType EventType, from, to time.Time) (int64, error)
}

// OrderRepository is the storage contract for orders.
type OrderRepository interface {
	Save(ctx context.Context, order *Order) error
	FindByID(ctx context.Context, id string) (*Order, error)
	FindByStore(ctx context.Context, storeID string, from, to time.Time, limit int) ([]*Order, error)
	UpdateStatus(ctx context.Context, id string, status OrderStatus) error
}

// AggregateRepository handles pre-computed analytics rollups.
type AggregateRepository interface {
	UpsertDailyAggregate(ctx context.Context, agg *DailyAggregate) error
	GetDailyAggregates(ctx context.Context, storeID string, from, to time.Time) ([]*DailyAggregate, error)
	GetTopProducts(ctx context.Context, storeID string, from, to time.Time, limit int) ([]*TopProduct, error)
	GetTopSearchTerms(ctx context.Context, storeID string, from, to time.Time, limit int) ([]*SearchTerm, error)
}
