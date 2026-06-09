package domain

import "time"

// RevenueMetric holds aggregated revenue for a time window.
type RevenueMetric struct {
	Date        time.Time `json:"date" bson:"date"`
	StoreID     string    `json:"store_id" bson:"store_id"`
	Revenue     float64   `json:"revenue" bson:"revenue"`
	OrderCount  int64     `json:"order_count" bson:"order_count"`
	AvgOrderVal float64   `json:"avg_order_value" bson:"avg_order_value"`
	Currency    string    `json:"currency" bson:"currency"`
}

// TopProduct represents a product ranked by a metric (sales, revenue, views).
type TopProduct struct {
	ProductID   string  `json:"product_id" bson:"_id"`
	ProductName string  `json:"product_name" bson:"product_name"`
	Category    string  `json:"category" bson:"category"`
	TotalSold   int64   `json:"total_sold" bson:"total_sold"`
	Revenue     float64 `json:"revenue" bson:"revenue"`
	ViewCount   int64   `json:"view_count" bson:"view_count"`
}

// FunnelStep represents conversion at each step of the purchase funnel.
type FunnelStep struct {
	Step        string  `json:"step"`
	EventType   EventType `json:"event_type"`
	Count       int64   `json:"count"`
	Conversion  float64 `json:"conversion_rate"` // relative to previous step
}

// Funnel is the full conversion funnel analysis for a store in a time range.
type Funnel struct {
	StoreID   string       `json:"store_id"`
	StartDate time.Time    `json:"start_date"`
	EndDate   time.Time    `json:"end_date"`
	Steps     []FunnelStep `json:"steps"`
}

// RealtimeStats is a snapshot of activity in the last N minutes.
type RealtimeStats struct {
	StoreID          string    `json:"store_id"`
	ActiveSessions   int64     `json:"active_sessions"`
	EventsLast5Min   int64     `json:"events_last_5min"`
	OrdersLast5Min   int64     `json:"orders_last_5min"`
	RevenueLast5Min  float64   `json:"revenue_last_5min"`
	TopPageLastHour  string    `json:"top_page_last_hour"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// DailyAggregate is a pre-computed rollup stored in MongoDB for fast reads.
type DailyAggregate struct {
	ID          string    `json:"id" bson:"_id"` // storeID:YYYY-MM-DD
	StoreID     string    `json:"store_id" bson:"store_id"`
	Date        time.Time `json:"date" bson:"date"`
	PageViews   int64     `json:"page_views" bson:"page_views"`
	Sessions    int64     `json:"sessions" bson:"sessions"`
	AddToCarts  int64     `json:"add_to_carts" bson:"add_to_carts"`
	Orders      int64     `json:"orders" bson:"orders"`
	Revenue     float64   `json:"revenue" bson:"revenue"`
	UniqueUsers int64     `json:"unique_users" bson:"unique_users"`
}

// SearchTerm tracks what users are searching for.
type SearchTerm struct {
	Term    string `json:"term" bson:"_id"`
	Count   int64  `json:"count" bson:"count"`
	StoreID string `json:"store_id" bson:"store_id"`
}
