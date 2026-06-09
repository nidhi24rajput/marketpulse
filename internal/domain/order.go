package domain

import (
	"errors"
	"time"
)

// OrderStatus represents the lifecycle of a customer order.
type OrderStatus string

const (
	OrderStatusPending   OrderStatus = "pending"
	OrderStatusPaid      OrderStatus = "paid"
	OrderStatusShipped   OrderStatus = "shipped"
	OrderStatusDelivered OrderStatus = "delivered"
	OrderStatusCancelled OrderStatus = "cancelled"
	OrderStatusRefunded  OrderStatus = "refunded"
)

// LineItem represents a single product within an order.
type LineItem struct {
	ProductID   string  `json:"product_id" bson:"product_id"`
	ProductName string  `json:"product_name" bson:"product_name"`
	SKU         string  `json:"sku" bson:"sku"`
	Category    string  `json:"category" bson:"category"`
	Quantity    int     `json:"quantity" bson:"quantity"`
	UnitPrice   float64 `json:"unit_price" bson:"unit_price"`
	TotalPrice  float64 `json:"total_price" bson:"total_price"`
}

// Order captures a completed customer purchase.
type Order struct {
	ID          string      `json:"id" bson:"_id"`
	StoreID     string      `json:"store_id" bson:"store_id"`
	SessionID   string      `json:"session_id" bson:"session_id"`
	UserID      string      `json:"user_id" bson:"user_id"`
	Status      OrderStatus `json:"status" bson:"status"`
	LineItems   []LineItem  `json:"line_items" bson:"line_items"`
	Subtotal    float64     `json:"subtotal" bson:"subtotal"`
	Tax         float64     `json:"tax" bson:"tax"`
	Shipping    float64     `json:"shipping" bson:"shipping"`
	Total       float64     `json:"total" bson:"total"`
	Currency    string      `json:"currency" bson:"currency"`
	Country     string      `json:"country" bson:"country"`
	PlacedAt    time.Time   `json:"placed_at" bson:"placed_at"`
	UpdatedAt   time.Time   `json:"updated_at" bson:"updated_at"`
}

// CalculateTotals recomputes subtotal and total from line items.
func (o *Order) CalculateTotals() {
	var subtotal float64
	for _, item := range o.LineItems {
		subtotal += item.TotalPrice
	}
	o.Subtotal = subtotal
	o.Total = subtotal + o.Tax + o.Shipping
}

// Validate ensures the order is well-formed before persistence.
func (o *Order) Validate() error {
	if o.StoreID == "" {
		return errors.New("store_id is required")
	}
	if o.UserID == "" {
		return errors.New("user_id is required")
	}
	if len(o.LineItems) == 0 {
		return errors.New("order must contain at least one line item")
	}
	if o.Currency == "" {
		o.Currency = "USD"
	}
	return nil
}
