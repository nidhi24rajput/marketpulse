package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/nidhi24rajput/marketpulse/internal/domain"
)

const (
	collectionDailyAgg = "daily_aggregates"
	collectionOrders2  = "orders"
	collectionEvents2  = "events"
)

// AggregateRepo handles pre-computed rollups and on-the-fly aggregation queries.
type AggregateRepo struct {
	client *Client
}

func NewAggregateRepo(client *Client) *AggregateRepo {
	return &AggregateRepo{client: client}
}

func (r *AggregateRepo) UpsertDailyAggregate(ctx context.Context, agg *domain.DailyAggregate) error {
	_, err := r.client.Collection(collectionDailyAgg).UpdateOne(ctx,
		bson.M{"_id": agg.ID},
		bson.M{"$set": agg},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("aggregate_repo: upsert daily failed: %w", err)
	}
	return nil
}

func (r *AggregateRepo) GetDailyAggregates(ctx context.Context, storeID string, from, to time.Time) ([]*domain.DailyAggregate, error) {
	cursor, err := r.client.Collection(collectionDailyAgg).Find(ctx,
		bson.M{
			"store_id": storeID,
			"date":     bson.M{"$gte": from, "$lte": to},
		},
		options.Find().SetSort(bson.D{{Key: "date", Value: 1}}),
	)
	if err != nil {
		return nil, fmt.Errorf("aggregate_repo: get daily failed: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()
	var aggs []*domain.DailyAggregate
	if err := cursor.All(ctx, &aggs); err != nil {
		return nil, err
	}
	return aggs, nil
}

// GetTopProducts runs a MongoDB aggregation pipeline on the orders collection.
func (r *AggregateRepo) GetTopProducts(ctx context.Context, storeID string, from, to time.Time, limit int) ([]*domain.TopProduct, error) {
	pipeline := mongo.Pipeline{
		// Stage 1: filter orders in range
		{{Key: "$match", Value: bson.M{
			"store_id":  storeID,
			"placed_at": bson.M{"$gte": from, "$lte": to},
			"status":    bson.M{"$nin": []string{"cancelled", "refunded"}},
		}}},
		// Stage 2: unwind line items
		{{Key: "$unwind", Value: "$line_items"}},
		// Stage 3: group by product
		{{Key: "$group", Value: bson.M{
			"_id":          "$line_items.product_id",
			"product_name": bson.M{"$first": "$line_items.product_name"},
			"category":     bson.M{"$first": "$line_items.category"},
			"total_sold":   bson.M{"$sum": "$line_items.quantity"},
			"revenue":      bson.M{"$sum": "$line_items.total_price"},
		}}},
		// Stage 4: sort by revenue descending
		{{Key: "$sort", Value: bson.D{{Key: "revenue", Value: -1}}}},
		{{Key: "$limit", Value: limit}},
	}

	cursor, err := r.client.Collection(collectionOrders2).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate_repo: top products failed: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()
	var products []*domain.TopProduct
	if err := cursor.All(ctx, &products); err != nil {
		return nil, err
	}
	return products, nil
}

// GetTopSearchTerms aggregates search events to find trending queries.
func (r *AggregateRepo) GetTopSearchTerms(ctx context.Context, storeID string, from, to time.Time, limit int) ([]*domain.SearchTerm, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"store_id":  storeID,
			"type":      domain.EventTypeSearch,
			"timestamp": bson.M{"$gte": from, "$lte": to},
		}}},
		{{Key: "$group", Value: bson.M{
			"_id":      "$properties.query",
			"count":    bson.M{"$sum": 1},
			"store_id": bson.M{"$first": "$store_id"},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "count", Value: -1}}}},
		{{Key: "$limit", Value: limit}},
	}

	cursor, err := r.client.Collection(collectionEvents2).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate_repo: top searches failed: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()
	var terms []*domain.SearchTerm
	if err := cursor.All(ctx, &terms); err != nil {
		return nil, err
	}
	return terms, nil
}
