// Package mongodb provides MongoDB client setup and repository implementations.
package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// Client wraps the official MongoDB client with convenience helpers.
type Client struct {
	client *mongo.Client
	db     *mongo.Database
}

// New connects to MongoDB and pings to verify the connection.
func New(uri, database string) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := options.Client().
		ApplyURI(uri).
		SetConnectTimeout(10 * time.Second).
		SetServerSelectionTimeout(5 * time.Second).
		SetMaxPoolSize(100).
		SetMinPoolSize(5)

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("mongodb: connect failed: %w", err)
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("mongodb: ping failed: %w", err)
	}

	return &Client{client: client, db: client.Database(database)}, nil
}

// Collection returns the named collection.
func (c *Client) Collection(name string) *mongo.Collection {
	return c.db.Collection(name)
}

// Disconnect cleanly closes all connections.
func (c *Client) Disconnect(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}

// EnsureIndexes creates all required indexes on first run.
// Safe to call multiple times — Mongo is idempotent for identical index specs.
func (c *Client) EnsureIndexes(ctx context.Context) error {
	type indexSpec struct {
		collection string
		models     []mongo.IndexModel
	}

	specs := []indexSpec{
		{
			collection: "events",
			models: []mongo.IndexModel{
				{Keys: bson.D{{Key: "store_id", Value: 1}, {Key: "timestamp", Value: -1}}},
				{Keys: bson.D{{Key: "session_id", Value: 1}}},
				{Keys: bson.D{{Key: "type", Value: 1}, {Key: "store_id", Value: 1}, {Key: "timestamp", Value: -1}}},
				{Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "timestamp", Value: -1}},
					Options: options.Index().SetSparse(true)},
				// TTL index — raw events expire after 90 days to control storage
				{Keys: bson.D{{Key: "timestamp", Value: 1}},
					Options: options.Index().SetExpireAfterSeconds(90 * 24 * 60 * 60)},
			},
		},
		{
			collection: "orders",
			models: []mongo.IndexModel{
				{Keys: bson.D{{Key: "store_id", Value: 1}, {Key: "placed_at", Value: -1}}},
				{Keys: bson.D{{Key: "user_id", Value: 1}}},
				{Keys: bson.D{{Key: "status", Value: 1}}},
			},
		},
		{
			collection: "daily_aggregates",
			models: []mongo.IndexModel{
				{Keys: bson.D{{Key: "store_id", Value: 1}, {Key: "date", Value: -1}},
					Options: options.Index().SetUnique(true)},
			},
		},
	}

	for _, spec := range specs {
		coll := c.db.Collection(spec.collection)
		if _, err := coll.Indexes().CreateMany(ctx, spec.models); err != nil {
			return fmt.Errorf("mongodb: index creation failed on %s: %w", spec.collection, err)
		}
	}
	return nil
}
