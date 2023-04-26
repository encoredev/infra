// Package outbox implements the transactional outbox pattern
// for ensuring data consistency between a transactional data store
// and a Pub/Sub topic.
package outbox

import (
	"context"
	"fmt"

	"encore.dev/pubsub"
)

// Bind binds a topic reference with a persist function,
// Outbox topics can be used to publish messages with the same interface
// as a *pubsub.Topic, but where calls to Publish are recorded in the outbox
// instead of being published to the actual topic.
func Bind[T any](topic pubsub.Publisher[T], persist PersistFunc) pubsub.Publisher[T] {
	return &boundTopic[T]{
		Publisher: topic,
		persist:   persist,
		topicName: topic.Meta().Name,
	}
}

type boundTopic[T any] struct {
	pubsub.Publisher[T]
	persist   PersistFunc
	topicName string
}

// Publish publishes the given message to the outbox.
func (t *boundTopic[T]) Publish(ctx context.Context, msg T) (id string, err error) {
	id, err = t.persist(ctx, t.topicName, msg)
	if err != nil {
		err = fmt.Errorf("outbox topic %s: persist message: %v", t.topicName, err)
	}
	return id, err
}

type PersistFunc func(ctx context.Context, topicName string, msg any) (id string, err error)
