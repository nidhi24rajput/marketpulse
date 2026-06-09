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

const collectionOrders = "orders"

// OrderRepo implements domain.OrderRepository backed by MongoDB.
type OrderRepo struct {
	coll *mongo.Collection
}

func NewOrderRepo(client *Client) *OrderRepo {
	return &OrderRepo{coll: client.Collection(collectionOrders)}
}

func (r *OrderRepo) Save(ctx context.Context, order *domain.Order) error {
	_, err := r.coll.InsertOne(ctx, order)
	if err != nil {
		return fmt.Errorf("order_repo: save failed: %w", err)
	}
	return nil
}

func (r *OrderRepo) FindByID(ctx context.Context, id string) (*domain.Order, error) {
	var order domain.Order
	if err := r.coll.FindOne(ctx, bson.M{"_id": id}).Decode(&order); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("order_repo: find by id failed: %w", err)
	}
	return &order, nil
}

func (r *OrderRepo) FindByStore(ctx context.Context, storeID string, from, to time.Time, limit int) ([]*domain.Order, error) {
	cursor, err := r.coll.Find(ctx,
		bson.M{
			"store_id": storeID,
			"placed_at": bson.M{"$gte": from, "$lte": to},
		},
		options.Find().
			SetSort(bson.D{{Key: "placed_at", Value: -1}}).
			SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, fmt.Errorf("order_repo: find by store failed: %w", err)
	}
	defer cursor.Close(ctx)
	var orders []*domain.Order
	if err := cursor.All(ctx, &orders); err != nil {
		return nil, err
	}
	return orders, nil
}

func (r *OrderRepo) UpdateStatus(ctx context.Context, id string, status domain.OrderStatus) error {
	result, err := r.coll.UpdateOne(ctx,
		bson.M{"_id": id},
		bson.M{"$set": bson.M{"status": status, "updated_at": time.Now().UTC()}},
	)
	if err != nil {
		return fmt.Errorf("order_repo: update status failed: %w", err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("order_repo: order %s not found", id)
	}
	return nil
}
