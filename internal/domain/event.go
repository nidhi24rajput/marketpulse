// Package domain contains the core business entities and logic for MarketPulse.
// These types are intentionally framework-agnostic — they model the problem,
// not the infrastructure.
package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// EventType categorises what happened in the storefront.
type EventType string

const (
	EventTypePageView     EventType = "page_view"
	EventTypeProductView  EventType = "product_view"
	EventTypeAddToCart    EventType = "add_to_cart"
	EventTypeRemoveFromCart EventType = "remove_from_cart"
	EventTypeCheckoutStart EventType = "checkout_start"
	EventTypeOrderPlaced  EventType = "order_placed"
	EventTypeOrderPaid    EventType = "order_paid"
	EventTypeOrderShipped EventType = "order_shipped"
	EventTypeSearch       EventType = "search"
)

// Event is the fundamental unit of tracking — every user action emits one.
type Event struct {
	ID         string            `json:"id" bson:"_id"`
	Type       EventType         `json:"type" bson:"type"`
	SessionID  string            `json:"session_id" bson:"session_id"`
	UserID     string            `json:"user_id,omitempty" bson:"user_id,omitempty"`
	StoreID    string            `json:"store_id" bson:"store_id"`
	Properties map[string]any    `json:"properties" bson:"properties"`
	Timestamp  time.Time         `json:"timestamp" bson:"timestamp"`
	ReceivedAt time.Time         `json:"received_at" bson:"received_at"`
	IP         string            `json:"ip,omitempty" bson:"ip,omitempty"`
	UserAgent  string            `json:"user_agent,omitempty" bson:"user_agent,omitempty"`
	Country    string            `json:"country,omitempty" bson:"country,omitempty"`
}

// Validate checks that the event carries the minimum required fields.
func (e *Event) Validate() error {
	if e.Type == "" {
		return errors.New("event type is required")
	}
	if e.SessionID == "" {
		return errors.New("session_id is required")
	}
	if e.StoreID == "" {
		return errors.New("store_id is required")
	}
	return nil
}

// NewEvent creates a valid Event with a generated ID and server-side timestamp.
func NewEvent(eventType EventType, sessionID, storeID, userID string, props map[string]any) (*Event, error) {
	e := &Event{
		ID:         uuid.New().String(),
		Type:       eventType,
		SessionID:  sessionID,
		UserID:     userID,
		StoreID:    storeID,
		Properties: props,
		Timestamp:  time.Now().UTC(),
		ReceivedAt: time.Now().UTC(),
	}
	return e, e.Validate()
}
