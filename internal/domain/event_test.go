package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nidhi24rajput/marketpulse/internal/domain"
)

func TestNewEvent_Valid(t *testing.T) {
	evt, err := domain.NewEvent(
		domain.EventTypePageView,
		"sess-abc",
		"store-1",
		"user-1",
		map[string]any{"page": "/"},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, evt.ID)
	assert.Equal(t, domain.EventTypePageView, evt.Type)
	assert.Equal(t, "sess-abc", evt.SessionID)
	assert.Equal(t, "store-1", evt.StoreID)
	assert.False(t, evt.Timestamp.IsZero())
}

func TestNewEvent_MissingType(t *testing.T) {
	_, err := domain.NewEvent("", "sess-abc", "store-1", "", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "event type")
}

func TestNewEvent_MissingSessionID(t *testing.T) {
	_, err := domain.NewEvent(domain.EventTypePageView, "", "store-1", "", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "session_id")
}

func TestNewEvent_MissingStoreID(t *testing.T) {
	_, err := domain.NewEvent(domain.EventTypePageView, "sess-1", "", "", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store_id")
}

func TestEvent_Validate_AllEventTypes(t *testing.T) {
	validTypes := []domain.EventType{
		domain.EventTypePageView,
		domain.EventTypeProductView,
		domain.EventTypeAddToCart,
		domain.EventTypeRemoveFromCart,
		domain.EventTypeCheckoutStart,
		domain.EventTypeOrderPlaced,
		domain.EventTypeOrderPaid,
		domain.EventTypeOrderShipped,
		domain.EventTypeSearch,
	}

	for _, et := range validTypes {
		t.Run(string(et), func(t *testing.T) {
			evt, err := domain.NewEvent(et, "sess-1", "store-1", "", nil)
			require.NoError(t, err)
			assert.Equal(t, et, evt.Type)
		})
	}
}
