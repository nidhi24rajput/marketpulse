package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nidhi24rajput/marketpulse/internal/domain"
)

func TestOrder_CalculateTotals(t *testing.T) {
	order := &domain.Order{
		LineItems: []domain.LineItem{
			{Quantity: 2, UnitPrice: 49.99, TotalPrice: 99.98},
			{Quantity: 1, UnitPrice: 29.99, TotalPrice: 29.99},
		},
		Tax:      10.40,
		Shipping: 5.99,
	}

	order.CalculateTotals()

	assert.InDelta(t, 129.97, order.Subtotal, 0.01)
	assert.InDelta(t, 146.36, order.Total, 0.01)
}

func TestOrder_CalculateTotals_FreeShipping(t *testing.T) {
	order := &domain.Order{
		LineItems: []domain.LineItem{
			{Quantity: 1, UnitPrice: 399.99, TotalPrice: 399.99},
		},
		Tax:      32.00,
		Shipping: 0,
	}

	order.CalculateTotals()

	assert.InDelta(t, 399.99, order.Subtotal, 0.01)
	assert.InDelta(t, 431.99, order.Total, 0.01)
}

func TestOrder_Validate_Valid(t *testing.T) {
	order := &domain.Order{
		StoreID: "store-1",
		UserID:  "user-1",
		LineItems: []domain.LineItem{
			{ProductID: "prod-1", Quantity: 1, UnitPrice: 10.00, TotalPrice: 10.00},
		},
	}
	require.NoError(t, order.Validate())
	assert.Equal(t, "USD", order.Currency) // default applied
}

func TestOrder_Validate_MissingStoreID(t *testing.T) {
	order := &domain.Order{
		UserID: "user-1",
		LineItems: []domain.LineItem{
			{ProductID: "prod-1", Quantity: 1},
		},
	}
	err := order.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store_id")
}

func TestOrder_Validate_MissingUserID(t *testing.T) {
	order := &domain.Order{
		StoreID: "store-1",
		LineItems: []domain.LineItem{
			{ProductID: "prod-1", Quantity: 1},
		},
	}
	err := order.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "user_id")
}

func TestOrder_Validate_EmptyLineItems(t *testing.T) {
	order := &domain.Order{
		StoreID:   "store-1",
		UserID:    "user-1",
		LineItems: []domain.LineItem{},
	}
	err := order.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "line item")
}
