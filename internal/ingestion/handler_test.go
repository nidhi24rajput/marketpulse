package ingestion_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/nidhi24rajput/marketpulse/internal/ingestion"
	"github.com/nidhi24rajput/marketpulse/internal/middleware"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// mockPublisher records published messages — no real Kafka needed.
type mockPublisher struct {
	mu       sync.Mutex
	messages []struct {
		topic, key string
		value      any
	}
	err error
}

func (m *mockPublisher) Publish(_ context.Context, topic, key string, v any) error {
	if m.err != nil {
		return m.err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, struct {
		topic, key string
		value      any
	}{topic, key, v})
	return nil
}

func (m *mockPublisher) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.messages)
}

func setupRouter(pub ingestion.Publisher) *gin.Engine {
	log := zap.NewNop()
	h := ingestion.NewHandler(pub, "test-topic", log)

	r := gin.New()
	r.Use(middleware.StoreIDRequired())
	r.POST("/v1/events", h.TrackEvent)
	r.POST("/v1/events/batch", h.TrackBatch)
	r.POST("/v1/orders", h.TrackOrder)
	return r
}

func TestTrackEvent_Success(t *testing.T) {
	pub := &mockPublisher{}
	router := setupRouter(pub)

	body := map[string]any{
		"type":       "page_view",
		"session_id": "sess-1",
		"properties": map[string]any{"page": "/home"},
	}
	w := postJSON(t, router, "/v1/events", body, "store-abc")

	assert.Equal(t, http.StatusAccepted, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "queued", resp["status"])
	assert.NotEmpty(t, resp["id"])
	assert.Equal(t, 1, pub.count())
}

func TestTrackEvent_MissingSessionID(t *testing.T) {
	pub := &mockPublisher{}
	router := setupRouter(pub)

	body := map[string]any{
		"type": "page_view",
		// session_id intentionally missing
	}
	w := postJSON(t, router, "/v1/events", body, "store-abc")

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, 0, pub.count())
}

func TestTrackEvent_MissingStoreHeader(t *testing.T) {
	pub := &mockPublisher{}
	router := setupRouter(pub)

	body := map[string]any{
		"type":       "page_view",
		"session_id": "sess-1",
	}
	// No X-Store-ID header
	data, _ := json.Marshal(body)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/events", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestTrackBatch_Success(t *testing.T) {
	pub := &mockPublisher{}
	router := setupRouter(pub)

	body := []map[string]any{
		{"type": "page_view", "session_id": "sess-1"},
		{"type": "product_view", "session_id": "sess-1", "properties": map[string]any{"product_id": "p1"}},
		{"type": "add_to_cart", "session_id": "sess-1"},
	}
	w := postJSON(t, router, "/v1/events/batch", body, "store-abc")

	assert.Equal(t, http.StatusAccepted, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, float64(3), resp["queued"])
	assert.Equal(t, 3, pub.count())
}

func TestTrackBatch_ExceedsLimit(t *testing.T) {
	pub := &mockPublisher{}
	router := setupRouter(pub)

	// Send 101 events — should be rejected
	body := make([]map[string]any, 101)
	for i := range body {
		body[i] = map[string]any{"type": "page_view", "session_id": "sess-1"}
	}
	w := postJSON(t, router, "/v1/events/batch", body, "store-abc")

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, 0, pub.count())
}

func TestTrackOrder_Success(t *testing.T) {
	pub := &mockPublisher{}
	router := setupRouter(pub)

	body := map[string]any{
		"session_id": "sess-1",
		"user_id":    "user-1",
		"line_items": []map[string]any{
			{
				"product_id":   "prod-1",
				"product_name": "Headphones",
				"sku":          "SKU-001",
				"category":     "Electronics",
				"quantity":     1,
				"unit_price":   249.99,
				"total_price":  249.99,
			},
		},
		"subtotal": 249.99,
		"tax":      20.00,
		"shipping": 0,
		"total":    269.99,
		"currency": "USD",
	}
	w := postJSON(t, router, "/v1/orders", body, "store-abc")

	assert.Equal(t, http.StatusAccepted, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "queued", resp["status"])
	assert.NotEmpty(t, resp["order_id"])
	assert.Equal(t, 1, pub.count())
}

func TestTrackOrder_MissingUserID(t *testing.T) {
	pub := &mockPublisher{}
	router := setupRouter(pub)

	body := map[string]any{
		"session_id": "sess-1",
		// user_id missing
		"line_items": []map[string]any{
			{"product_id": "p1", "quantity": 1, "unit_price": 10.0, "total_price": 10.0},
		},
	}
	w := postJSON(t, router, "/v1/orders", body, "store-abc")

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func postJSON(t *testing.T, router *gin.Engine, path string, body any, storeID string) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	if storeID != "" {
		req.Header.Set("X-Store-ID", storeID)
	}

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}
