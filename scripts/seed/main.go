// Command seed generates realistic e-commerce data for development and demos.
// It creates 90 days of orders, events, and search queries via the Ingestion API.
//
// Usage:
//
//	go run ./scripts/seed --store=demo-store --days=30 --orders=500
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"
)

var (
	storeID      = flag.String("store", "demo-store", "Store ID to seed")
	ingestionURL = flag.String("url", "http://localhost:8080", "Ingestion service base URL")
	days         = flag.Int("days", 30, "Number of days to seed")
	numOrders    = flag.Int("orders", 200, "Number of orders to create")
)

// Realistic product catalog.
var products = []struct {
	ID       string
	Name     string
	Category string
	Price    float64
}{
	{"prod-001", "Wireless Noise-Cancelling Headphones", "Electronics", 249.99},
	{"prod-002", "Mechanical Keyboard", "Electronics", 129.99},
	{"prod-003", "4K Webcam", "Electronics", 89.99},
	{"prod-004", "Standing Desk Mat", "Office", 59.99},
	{"prod-005", "Ergonomic Chair", "Office", 399.99},
	{"prod-006", "USB-C Hub 7-in-1", "Electronics", 49.99},
	{"prod-007", "Monitor Light Bar", "Office", 39.99},
	{"prod-008", "Laptop Stand Adjustable", "Office", 34.99},
	{"prod-009", "Running Shoes Pro", "Sports", 119.99},
	{"prod-010", "Yoga Mat Premium", "Sports", 45.99},
	{"prod-011", "Resistance Bands Set", "Sports", 24.99},
	{"prod-012", "Protein Powder Vanilla 2kg", "Nutrition", 54.99},
	{"prod-013", "Stainless Steel Water Bottle", "Sports", 29.99},
	{"prod-014", "Coffee Grinder Manual", "Kitchen", 44.99},
	{"prod-015", "Cast Iron Skillet 12\"", "Kitchen", 39.99},
}

var searchTerms = []string{
	"wireless headphones", "mechanical keyboard", "standing desk", "ergonomic chair",
	"USB-C hub", "webcam 4k", "running shoes", "yoga mat", "protein powder",
	"laptop stand", "monitor light", "water bottle", "coffee grinder", "skillet",
	"noise cancelling", "bluetooth speaker", "gaming mouse", "desk organizer",
}

var countries = []string{"US", "CA", "GB", "DE", "AU", "FR", "NL", "SE"}

func main() {
	flag.Parse()

	rng := rand.New(rand.NewSource(42)) // deterministic for reproducible demos
	client := &http.Client{Timeout: 10 * time.Second}

	log.Printf("Seeding store=%s for %d days, %d orders via %s", *storeID, *days, *numOrders, *ingestionURL)

	start := time.Now().AddDate(0, 0, -*days)
	ordersPerDay := *numOrders / *days

	totalEvents := 0
	totalOrders := 0

	for d := 0; d < *days; d++ {
		dayStart := start.AddDate(0, 0, d)

		// Simulate realistic daily traffic (more on weekends, peaks midday)
		trafficMultiplier := 1.0
		if dayStart.Weekday() == time.Saturday || dayStart.Weekday() == time.Sunday {
			trafficMultiplier = 1.4
		}

		sessionsToday := int(float64(30+rng.Intn(50)) * trafficMultiplier)

		for s := 0; s < sessionsToday; s++ {
			sessionID := fmt.Sprintf("sess-%d-%d", d, s)
			userID := fmt.Sprintf("user-%d", rng.Intn(500))
			sessionStart := dayStart.Add(time.Duration(rng.Intn(86400)) * time.Second)
			country := countries[rng.Intn(len(countries))]

			// Page view
			sendEvent(client, "page_view", sessionID, userID, country, sessionStart, map[string]any{
				"page":     "/",
				"referrer": "google.com",
			})
			totalEvents++

			// Search (40% of sessions)
			if rng.Float64() < 0.4 {
				term := searchTerms[rng.Intn(len(searchTerms))]
				sendEvent(client, "search", sessionID, userID, country, sessionStart.Add(15*time.Second), map[string]any{
					"query": term,
				})
				totalEvents++
			}

			// Product view (70% of sessions)
			if rng.Float64() < 0.7 {
				product := products[rng.Intn(len(products))]
				sendEvent(client, "product_view", sessionID, userID, country, sessionStart.Add(30*time.Second), map[string]any{
					"product_id":   product.ID,
					"product_name": product.Name,
					"category":     product.Category,
					"price":        product.Price,
				})
				totalEvents++

				// Add to cart (45% of product viewers)
				if rng.Float64() < 0.45 {
					sendEvent(client, "add_to_cart", sessionID, userID, country, sessionStart.Add(60*time.Second), map[string]any{
						"product_id":   product.ID,
						"product_name": product.Name,
						"quantity":     1 + rng.Intn(3),
						"price":        product.Price,
					})
					totalEvents++
				}
			}
		}

		// Create orders for the day
		ordersToday := int(float64(ordersPerDay) * trafficMultiplier)
		for o := 0; o < ordersToday; o++ {
			orderTime := dayStart.Add(time.Duration(rng.Intn(86400)) * time.Second)
			numItems := 1 + rng.Intn(4)
			lineItems := make([]map[string]any, numItems)
			var total float64

			for i := 0; i < numItems; i++ {
				p := products[rng.Intn(len(products))]
				qty := 1 + rng.Intn(3)
				itemTotal := p.Price * float64(qty)
				total += itemTotal
				lineItems[i] = map[string]any{
					"product_id":   p.ID,
					"product_name": p.Name,
					"sku":          fmt.Sprintf("SKU-%s", p.ID),
					"category":     p.Category,
					"quantity":     qty,
					"unit_price":   p.Price,
					"total_price":  itemTotal,
				}
			}

			tax := total * 0.08
			shipping := 5.99
			if total > 75 {
				shipping = 0
			}

			order := map[string]any{
				"session_id": fmt.Sprintf("sess-%d-order-%d", d, o),
				"user_id":    fmt.Sprintf("user-%d", rng.Intn(500)),
				"line_items": lineItems,
				"subtotal":   total,
				"tax":        tax,
				"shipping":   shipping,
				"total":      total + tax + shipping,
				"currency":   "USD",
				"country":    countries[rng.Intn(len(countries))],
			}

			if err := sendOrder(client, order, orderTime); err != nil {
				log.Printf("warn: order send failed: %v", err)
			} else {
				totalOrders++
			}
		}

		if d%7 == 0 {
			log.Printf("Progress: day %d/%d — %d events, %d orders seeded", d+1, *days, totalEvents, totalOrders)
		}
	}

	log.Printf("✓ Seeding complete: %d events, %d orders for store=%s", totalEvents, totalOrders, *storeID)
}

func sendEvent(client *http.Client, eventType, sessionID, userID, country string, ts time.Time, props map[string]any) {
	payload := map[string]any{
		"type":       eventType,
		"session_id": sessionID,
		"user_id":    userID,
		"properties": props,
		"timestamp":  ts,
	}
	_ = post(client, "/v1/events", payload)
}

func sendOrder(client *http.Client, order map[string]any, ts time.Time) error {
	order["placed_at"] = ts
	return post(client, "/v1/orders", order)
}

func post(client *http.Client, path string, body any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, *ingestionURL+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Store-ID", *storeID)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d from %s", resp.StatusCode, path)
	}
	return nil
}
