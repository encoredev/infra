// Package outbox implements the transactional outbox pattern
// for ensuring data consistency between a transactional data store
// and a Pub/Sub topic.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"encore.dev/pubsub"
	"encore.dev/rlog"
)

// A Relay polls an outbox for new messages and publishes them.
//
// Topics must be registered with the relay to be processed using [RegisterTopic].
type Relay struct {
	store      Store
	mu         sync.RWMutex
	topics     map[string]publishFunc
	topicNames []string
}

// NewRelay creates a new relay using the given store.
//
// To start polling for messages, call PollForMessages.
// At any point you may also call ProcessMessages to immediately
// process any messages that have been persisted.
func NewRelay(store Store) *Relay {
	return &Relay{
		store:  store,
		topics: make(map[string]publishFunc),
	}
}

// A publishFunc publishes a persisted message to its bound topic.
type publishFunc func(ctx context.Context, msg PersistedMessage) (id string, err error)

// RegisterTopic registers a topic with a relay,
// enabling the relay to process messages for that topic.
func RegisterTopic[T any](r *Relay, topic pubsub.Publisher[T]) {
	publish := func(ctx context.Context, pm PersistedMessage) (id string, err error) {
		var msg T
		if err := json.Unmarshal(pm.Data, &msg); err != nil {
			return "", fmt.Errorf("outbox relay: unmarshal message %v for topic %s: %v",
				pm.MessageID, pm.TopicName, err)
		}

		id, err = topic.Publish(ctx, msg)
		if err != nil {
			return "", fmt.Errorf("outbox relay: publish message %v for topic %s: %v",
				pm.MessageID, pm.TopicName, err)
		}
		return id, nil
	}

	topicName := topic.Meta().Name
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.topics[topicName] != nil {
		panic(fmt.Sprintf("outbox relay: topic %s already registered", topicName))
	}
	r.topics[topicName] = publish
	r.topicNames = append(r.topicNames, topicName)
}

// PollForMessages polls for new messages to publish, processing up to batchSize
// messages each time. If batchSize is <= 0 it defaults to 100.
//
// It blocks (does not return) until ctx is cancelled.
func (r *Relay) PollForMessages(ctx context.Context, batchSize int) {
	if batchSize <= 0 {
		batchSize = 100
	}

	for {
		numPublishSuccesses, numPublishErrs, storeErr := r.ProcessMessages(ctx, batchSize)

		// Determine how long to sleep.
		var sleepDelay time.Duration
		switch {
		case storeErr != nil:
			// We have an error reading/writing from the store;
			// wait a long time since the error is fatal.
			sleepDelay = 10 * time.Second

		case numPublishSuccesses >= batchSize:
			sleepDelay = 0 // we have a backlog; no need to sleep

		case numPublishErrs > 0:
			// Some messages failed to publish; back off
			sleepDelay = 3 * time.Second

		case numPublishSuccesses == 0:
			// There were no messages at all.
			sleepDelay = 5 * time.Second

		default:
			// Any other situation wait a small amount a time.
			sleepDelay = 1 * time.Second
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(sleepDelay):
		}
	}
}

// ProcessMessages processes new messages, publishing them to the appropriate Pub/Sub topics.
// It processes up to limit new messages. If limit is <= 0 it defaults to 100.
//
// It returns the number of topic publishing successes and errors, as well as any Store-related error,
// which can be used to adaptively change poll frequency.
func (r *Relay) ProcessMessages(ctx context.Context, limit int) (numPublishSuccesses, numPublishErrs int, storeErr error) {
	r.mu.Lock()
	names := r.topicNames
	r.mu.Unlock()

	if limit <= 0 {
		limit = 100
	}

	batch, err := r.store.CheckoutBatch(ctx, names, limit)
	if err != nil {
		return 0, 0, fmt.Errorf("checkout outbox relay batch: %w", err)
	} else if batch == nil {
		// No messages found; we're done.
		return 0, 0, nil
	}

	numPublishSuccesses, numPublishErrs, storeErr = r.processBatch(ctx, batch)
	return
}

// processBatch processes the messages in a batch, publishing them to their respective topics.
// It returns the number of errors encountered when publishing,
// as well as any Store-related error (which is cause for immediate abort).
func (r *Relay) processBatch(ctx context.Context, batch MessageBatch) (numPublishSuccesses, numPublishErrs int, storeErr error) {
	defer func() {
		if err2 := batch.Close(); err2 != nil && storeErr == nil {
			storeErr = fmt.Errorf("close message batch: %w", err2)
		}
	}()

	// topicErrs tracks errors by topic name, to make sure we don't publish
	// additional messages to topics that have errored.
	topicErrs := make(map[string]bool)

	msgs := batch.Messages()
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, msg := range msgs {
		if topicErrs[msg.TopicName] {
			continue
		}

		publish, ok := r.topics[msg.TopicName]
		if !ok {
			rlog.Error("outbox relay: got unknown topic in message batch",
				"topic", msg.TopicName)
			continue
		}

		publishID, err := publish(ctx, msg)
		if err != nil {
			rlog.Error("outbox relay: unable to publish message",
				"topic", msg.TopicName, "err", err)
			topicErrs[msg.TopicName] = true
			numPublishErrs++
			continue
		}
		numPublishSuccesses++

		if err := batch.MarkPublished(ctx, msg, publishID); err != nil {
			// If this fails we should abort as the error is likely to be
			// Store-wide and not specific to this message.
			storeErr = fmt.Errorf("mark message published: %v", err)
			return
		}
	}

	return
}

// A PersistedMessage represents a checked-out persisted message in a Store.
type PersistedMessage struct {
	// MessageID is the unique id of the persisted message.
	MessageID any

	// TopicName is the name of the topic the message is for.
	TopicName string

	// Data is the serialized message data.
	Data []byte
}

// The Store interface implements the persistence part of the outbox.
type Store interface {
	// CheckoutBatch checks out a batch of messages in the outbox.
	// The messages returned must only belong to the given topics.
	//
	// Messages in a batch must be held exclusively by the batch; other processes
	// are not allowed to check out the same messages while the batch is open.
	//
	// The provided limit is a hint for how many total messages should be returned maximum.
	//
	// CheckoutBatch may return (nil, nil) to indicate there were no matching messages in the outbox.
	CheckoutBatch(ctx context.Context, topicNames []string, limit int) (MessageBatch, error)
}

// MessageBatch is a batch of messages that have been checked out
// for publishing via a Relay.
type MessageBatch interface {
	// Messages returns the messages in the batch.
	// The messages must be returned in the order they were inserted into the outbox
	// within each topic (ordering is only defined within a topic, not between topics).
	//
	// Messages must be held exclusively by the batch; other processes
	// are not allowed to check out the same messages while the batch is open.
	Messages() []PersistedMessage

	// MarkPublished marks a persisted message as being successfully published.
	//
	// The store may choose what to do with the message, such as deleting it or
	// marking the message as having been published, as long as it is no longer
	// returned by subsequent calls to CheckoutBatch (assuming Close successfully commits
	// the batch's transaction, if applicable).
	//
	// If the batch is implemented in a transactional way it is recommended
	// the deletion to take effect on the subsequent call to Close.
	MarkPublished(ctx context.Context, msg PersistedMessage, publishID string) error

	// Close closes the batch, committing any MarkPublished changes (if the store is transactional)
	// and releasing the lock on the remaining persisted messages in the batch (if any).
	Close() error
}
