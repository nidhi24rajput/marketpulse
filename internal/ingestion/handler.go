// Package ingestion contains the HTTP handlers for the Event Ingestion Service.
// Keeping handlers in an internal package makes them unit-testable without
// needing a running Kafka broker.
package ingestion

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/nidhi24rajput/marketpulse/internal/domain"
)

// Publisher is satisfied by both the real Kafka producer and the test mock.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, v any) error
}

// Handler contains the HTTP handlers for the ingestion service.
type Handler struct {
	publisher Publisher
	topic     string
	log       *zap.Logger
}

// NewHandler creates a new ingestion Handler.
func NewHandler(publisher Publisher, topic string, log *zap.Logger) *Handler {
	return &Handler{publisher: publisher, topic: topic, log: log}
}

// TrackEventRequest is the inbound payload for a single event.
type TrackEventRequest struct {
	Type       domain.EventType `json:"type" binding:"required"`
	SessionID  string           `json:"session_id" binding:"required"`
	UserID     string           `json:"user_id"`
	Properties map[string]any   `json:"properties"`
	Timestamp  *time.Time       `json:"timestamp"`
}

// TrackOrderRequest is the payload for a completed order event.
type TrackOrderRequest struct {
	OrderID   string            `json:"order_id"`
	SessionID string            `json:"session_id" binding:"required"`
	UserID    string            `json:"user_id" binding:"required"`
	LineItems []domain.LineItem `json:"line_items" binding:"required,min=1"`
	Subtotal  float64           `json:"subtotal"`
	Tax       float64           `json:"tax"`
	Shipping  float64           `json:"shipping"`
	Total     float64           `json:"total"`
	Currency  string            `json:"currency"`
	Country   string            `json:"country"`
}

// TrackEvent handles POST /v1/events — publishes a single event to Kafka.
func (h *Handler) TrackEvent(c *gin.Context) {
	var req TrackEventRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	storeID := c.GetString("store_id")

	ts := time.Now().UTC()
	if req.Timestamp != nil {
		ts = req.Timestamp.UTC()
	}

	event := &domain.Event{
		ID:         uuid.New().String(),
		Type:       req.Type,
		SessionID:  req.SessionID,
		UserID:     req.UserID,
		StoreID:    storeID,
		Properties: req.Properties,
		Timestamp:  ts,
		ReceivedAt: time.Now().UTC(),
		IP:         c.ClientIP(),
		UserAgent:  c.GetHeader("User-Agent"),
	}

	if err := h.publisher.Publish(c.Request.Context(), h.topic, event.StoreID, event); err != nil {
		h.log.Error("failed to publish event", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to queue event"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"id": event.ID, "status": "queued"})
}

// TrackBatch handles POST /v1/events/batch — ingests up to 100 events.
func (h *Handler) TrackBatch(c *gin.Context) {
	var reqs []TrackEventRequest
	if err := c.ShouldBindJSON(&reqs); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if len(reqs) > 100 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "batch size exceeds limit of 100"})
		return
	}

	storeID := c.GetString("store_id")
	ids := make([]string, 0, len(reqs))

	for _, req := range reqs {
		ts := time.Now().UTC()
		if req.Timestamp != nil {
			ts = req.Timestamp.UTC()
		}
		event := &domain.Event{
			ID:         uuid.New().String(),
			Type:       req.Type,
			SessionID:  req.SessionID,
			UserID:     req.UserID,
			StoreID:    storeID,
			Properties: req.Properties,
			Timestamp:  ts,
			ReceivedAt: time.Now().UTC(),
			IP:         c.ClientIP(),
			UserAgent:  c.GetHeader("User-Agent"),
		}
		if err := h.publisher.Publish(c.Request.Context(), h.topic, event.StoreID, event); err != nil {
			h.log.Error("batch publish partial failure", zap.String("event_id", event.ID), zap.Error(err))
			continue
		}
		ids = append(ids, event.ID)
	}

	c.JSON(http.StatusAccepted, gin.H{"queued": len(ids), "ids": ids})
}

// TrackOrder handles POST /v1/orders — wraps a structured order in an event and publishes it.
func (h *Handler) TrackOrder(c *gin.Context) {
	var req TrackOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	storeID := c.GetString("store_id")
	if req.OrderID == "" {
		req.OrderID = uuid.New().String()
	}

	order := &domain.Order{
		ID:        req.OrderID,
		StoreID:   storeID,
		SessionID: req.SessionID,
		UserID:    req.UserID,
		Status:    domain.OrderStatusPaid,
		LineItems: req.LineItems,
		Subtotal:  req.Subtotal,
		Tax:       req.Tax,
		Shipping:  req.Shipping,
		Total:     req.Total,
		Currency:  req.Currency,
		Country:   req.Country,
		PlacedAt:  time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	order.CalculateTotals()

	event := &domain.Event{
		ID:         uuid.New().String(),
		Type:       domain.EventTypeOrderPlaced,
		SessionID:  req.SessionID,
		UserID:     req.UserID,
		StoreID:    storeID,
		Properties: map[string]any{"order": order},
		Timestamp:  time.Now().UTC(),
		ReceivedAt: time.Now().UTC(),
		IP:         c.ClientIP(),
	}

	if err := h.publisher.Publish(c.Request.Context(), h.topic, event.StoreID, event); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to queue order"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"order_id": order.ID, "status": "queued"})
}
