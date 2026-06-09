package domain_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/nidhi24rajput/marketpulse/internal/domain"
)

func TestFunnelStep_ConversionRate(t *testing.T) {
	// Simulate typical e-commerce funnel numbers
	steps := []domain.FunnelStep{
		{Step: "Page View", Count: 10000, Conversion: 100.0},
		{Step: "Product View", Count: 4500, Conversion: 45.0},
		{Step: "Add to Cart", Count: 1200, Conversion: 26.7},
		{Step: "Checkout Started", Count: 600, Conversion: 50.0},
		{Step: "Order Placed", Count: 420, Conversion: 70.0},
	}

	funnel := domain.Funnel{
		StoreID:   "store-1",
		StartDate: time.Now().AddDate(0, 0, -7),
		EndDate:   time.Now(),
		Steps:     steps,
	}

	assert.Equal(t, "store-1", funnel.StoreID)
	assert.Len(t, funnel.Steps, 5)
	assert.Equal(t, int64(10000), funnel.Steps[0].Count)
	assert.Equal(t, 100.0, funnel.Steps[0].Conversion)
	assert.Equal(t, int64(420), funnel.Steps[4].Count)
}

func TestDailyAggregate_ID(t *testing.T) {
	// ID format must be storeID:YYYY-MM-DD for upsert uniqueness
	agg := domain.DailyAggregate{
		ID:      "store-abc:2024-01-15",
		StoreID: "store-abc",
		Date:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		Revenue: 1234.56,
		Orders:  42,
	}

	assert.Equal(t, "store-abc:2024-01-15", agg.ID)
	assert.Equal(t, float64(1234.56), agg.Revenue)
	assert.Equal(t, int64(42), agg.Orders)
}

func TestRealtimeStats_Fields(t *testing.T) {
	now := time.Now().UTC()
	stats := domain.RealtimeStats{
		StoreID:         "store-1",
		ActiveSessions:  42,
		EventsLast5Min:  380,
		OrdersLast5Min:  7,
		RevenueLast5Min: 843.50,
		UpdatedAt:       now,
	}

	assert.Equal(t, int64(42), stats.ActiveSessions)
	assert.Equal(t, int64(380), stats.EventsLast5Min)
	assert.InDelta(t, 843.50, stats.RevenueLast5Min, 0.01)
	assert.False(t, stats.UpdatedAt.IsZero())
}
