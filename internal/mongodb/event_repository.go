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

const collectionEvents = "events"

// EventRepo implements domain.EventRepository backed by MongoDB.
type EventRepo struct {
	coll *mongo.Collection
}

// NewEventRepo constructs a repository bound to the events collection.
func NewEventRepo(client *Client) *EventRepo {
	return &EventRepo{coll: client.Collection(collectionEvents)}
}

func (r *EventRepo) Save(ctx context.Context, event *domain.Event) error {
	_, err := r.coll.InsertOne(ctx, event)
	if err != nil {
		return fmt.Errorf("event_repo: save failed: %w", err)
	}
	return nil
}

func (r *EventRepo) SaveBatch(ctx context.Context, events []*domain.Event) error {
	if len(events) == 0 {
		return nil
	}
	docs := make([]any, len(events))
	for i, e := range events {
		docs[i] = e
	}
	_, err := r.coll.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if err != nil {
		return fmt.Errorf("event_repo: batch save failed: %w", err)
	}
	return nil
}

func (r *EventRepo) FindBySessionID(ctx context.Context, sessionID string) ([]*domain.Event, error) {
	cursor, err := r.coll.Find(ctx,
		bson.M{"session_id": sessionID},
		options.Find().SetSort(bson.D{{Key: "timestamp", Value: 1}}).SetLimit(500),
	)
	if err != nil {
		return nil, fmt.Errorf("event_repo: find by session failed: %w", err)
	}
	return decodeEvents(ctx, cursor)
}

func (r *EventRepo) FindByStoreAndTimeRange(ctx context.Context, storeID string, from, to time.Time, limit int) ([]*domain.Event, error) {
	cursor, err := r.coll.Find(ctx,
		bson.M{
			"store_id":  storeID,
			"timestamp": bson.M{"$gte": from, "$lte": to},
		},
		options.Find().
			SetSort(bson.D{{Key: "timestamp", Value: -1}}).
			SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, fmt.Errorf("event_repo: find by store failed: %w", err)
	}
	return decodeEvents(ctx, cursor)
}

func (r *EventRepo) CountByType(ctx context.Context, storeID string, eventType domain.EventType, from, to time.Time) (int64, error) {
	count, err := r.coll.CountDocuments(ctx, bson.M{
		"store_id":  storeID,
		"type":      eventType,
		"timestamp": bson.M{"$gte": from, "$lte": to},
	})
	if err != nil {
		return 0, fmt.Errorf("event_repo: count failed: %w", err)
	}
	return count, nil
}

func decodeEvents(ctx context.Context, cursor *mongo.Cursor) ([]*domain.Event, error) {
	defer func() { _ = cursor.Close(ctx) }()
	var events []*domain.Event
	if err := cursor.All(ctx, &events); err != nil {
		return nil, err
	}
	return events, nil
}
