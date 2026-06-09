package kafka

import (
	"context"
	"sync"
)

// MockProducer records published messages for use in tests.
// It satisfies the same interface used by the Handler, but lives in the kafka
// package so tests can import it without a separate mock package.
type MockProducer struct {
	mu       sync.Mutex
	Messages []MockMessage
	Err      error // if set, Publish returns this error
}

// MockMessage holds a single captured publish call.
type MockMessage struct {
	Topic string
	Key   string
	Value any
}

// Publish records the message and returns MockProducer.Err.
func (m *MockProducer) Publish(_ context.Context, topic, key string, v any) error {
	if m.Err != nil {
		return m.Err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Messages = append(m.Messages, MockMessage{Topic: topic, Key: key, Value: v})
	return nil
}

// Count returns the number of messages published so far.
func (m *MockProducer) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.Messages)
}

// Reset clears recorded messages.
func (m *MockProducer) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Messages = nil
}
