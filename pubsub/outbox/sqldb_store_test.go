package outbox_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"

	"x.encore.dev/infra/pubsub/outbox"

	"encore.dev/pubsub"
	"encore.dev/storage/sqldb"
)

var testdb = sqldb.NewDatabase("outbox-test", sqldb.DatabaseConfig{
	Migrations: "./testdata/test-migrations",
})

type Message struct {
	Contents string
}

var ExampleTopic = pubsub.NewTopic[*Message]("my-topic", pubsub.TopicConfig{
	DeliveryGuarantee: pubsub.AtLeastOnce,
})

var MyDatabase *sqldb.Database // for the Example below

func Example(t *testing.T) {
	// Somewhere define a pubsub topic and a database.
	//
	// type Message struct { Contents string }
	// var ExampleTopic = pubsub.NewTopic[*Message](...)
	// var MyDatabase = sqldb.NewDatabase(...)

	// Create a topic reference for use with the outbox.
	topicRef := pubsub.TopicRef[pubsub.Publisher[*Message]](ExampleTopic)

	// When processing a request that needs to publish messages,
	// open a transaction and bind the topic to the transaction.
	var tx *sqldb.Tx // obtained from somewhere
	topic := outbox.Bind(topicRef, outbox.TxPersister(tx))

	// Later, publish messages to the topic.
	// The Publish call inserts a row into the outbox table inside tx.
	id, err := topic.Publish(context.Background(), &Message{Contents: "hello!"})
	if err != nil {
		fmt.Println("unable to publish to outbox:", err)
		return
	}
	fmt.Println("published message to outbox with id", id)

	// Separately, create a relay that polls the outbox table.
	// The topics to process have to be registered with the relay.
	relay := outbox.NewRelay(outbox.SQLDBStore(MyDatabase))
	outbox.RegisterTopic(relay, topicRef)
	go relay.PollForMessages(context.Background(), 100)
}

func TestTxPersister(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	ref := pubsub.TopicRef[pubsub.Publisher[*Msg]](topic)

	tx, err := testdb.Begin(ctx)
	c.Assert(err, qt.IsNil)
	defer tx.Rollback()

	tt := outbox.Bind(ref, outbox.TxPersister(tx))
	msg := &Msg{Contents: "test"}
	now := time.Now()
	id, err := tt.Publish(ctx, msg)
	c.Assert(err, qt.IsNil)

	// Make sure the given id exists in our database
	var pm outbox.PersistedMessage
	var insertedAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT id, topic, data, inserted_at
		FROM outbox
		WHERE id = $1
	`, id).Scan(&pm.MessageID, &pm.TopicName, &pm.Data, &insertedAt)
	c.Assert(err, qt.IsNil)
	c.Assert(pm.TopicName, qt.Equals, topic.Meta().Name)
	c.Assert(pm.Data, qt.JSONEquals, msg)
	c.Assert(insertedAt, qt.CmpEquals(cmpopts.EquateApproxTime(time.Second)), now)
}

func TestSQLDBStore(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	ref := pubsub.TopicRef[pubsub.Publisher[*Msg]](topic)

	store := outbox.SQLDBStore(testdb)

	// Batch should be empty since we haven't published anything
	{
		batch, err := store.CheckoutBatch(ctx, []string{ref.Meta().Name}, 100)
		c.Assert(err, qt.IsNil)
		c.Assert(batch.Messages(), qt.HasLen, 0)
		c.Assert(batch.Close(), qt.IsNil)
	}

	// Publish a message.
	var id string
	{
		tx, err := testdb.Begin(ctx)
		c.Assert(err, qt.IsNil)
		tt := outbox.Bind(ref, outbox.TxPersister(tx))
		msg := &Msg{Contents: "test"}
		id, err = tt.Publish(ctx, msg)
		c.Assert(err, qt.IsNil)
		c.Assert(tx.Commit(), qt.IsNil)
	}

	// Batch should contain the message we just published
	{
		batch, err := store.CheckoutBatch(ctx, []string{ref.Meta().Name}, 100)
		c.Assert(err, qt.IsNil)
		msgs := batch.Messages()
		c.Assert(msgs, qt.HasLen, 1)
		c.Assert(msgs[0], qt.DeepEquals, outbox.PersistedMessage{
			MessageID: mustInt(id),
			TopicName: topic.Meta().Name,
			Data:      []byte(`{"Contents": "test"}`),
		})
		c.Assert(batch.Close(), qt.IsNil)
	}

	// If we don't mark it as published it should get picked up again.
	{
		batch, err := store.CheckoutBatch(ctx, []string{ref.Meta().Name}, 100)
		c.Assert(err, qt.IsNil)
		msgs := batch.Messages()
		c.Assert(msgs, qt.HasLen, 1)
		c.Assert(msgs[0], qt.DeepEquals, outbox.PersistedMessage{
			MessageID: mustInt(id),
			TopicName: topic.Meta().Name,
			Data:      []byte(`{"Contents": "test"}`),
		})
		c.Assert(batch.MarkPublished(ctx, msgs[0], "publish-id"), qt.IsNil)
		c.Assert(batch.Close(), qt.IsNil)
	}

	// Now that it's been marked as published, it shouldn't be checked out.
	{
		batch, err := store.CheckoutBatch(ctx, []string{ref.Meta().Name}, 100)
		c.Assert(err, qt.IsNil)
		c.Assert(batch.Messages(), qt.HasLen, 0)
		c.Assert(batch.Close(), qt.IsNil)
	}
}

func mustInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return i
}
