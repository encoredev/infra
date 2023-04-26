// Package outbox implements the transactional outbox pattern
// for Encore's pubsub package.
//
// It works by binding a pubsub topic to a transaction, translating
// all calls to Publish into inserting a database row in an outbox table.
// If/when the transaction later commits, the messages are picked up by
// a relay that polls the outbox table and publishes the messages to the
// actual Pub/Sub topic.
//
// Both the topic binding and the relay support pluggable storage backends,
// enabling using the outbox pattern with any transactional storage backend.
// The outbox package provides an implementation using Encore's built-in
// SQL database support.
//
// Note that the topics to process have to be registered with the relay
// using [RegisterTopic].
package outbox
