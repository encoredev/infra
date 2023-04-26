package outbox_test

import (
	"context"
	"encoding/json"
	"strconv"
	"sync/atomic"
	"testing"

	"x.encore.dev/infra/pubsub/outbox"

	qt "github.com/frankban/quicktest"

	"encore.dev/pubsub"
)

func TestRelay(t *testing.T) {
	c := qt.New(t)

	var (
		nextID int64
		pms    []outbox.PersistedMessage
	)

	insertFn := func(ctx context.Context, topicName string, msg any) (id string, err error) {
		data, err := json.Marshal(msg)
		if err != nil {
			return "", err
		}
		msgID := atomic.AddInt64(&nextID, 1)
		pm := outbox.PersistedMessage{
			MessageID: msgID,
			TopicName: topicName,
			Data:      data,
		}
		pms = append(pms, pm)
		return strconv.FormatInt(msgID, 10), nil
	}

	pub := pubsub.TopicRef[pubsub.Publisher[*Msg]](topic)
	bound := outbox.Bind(pub, insertFn)

	bound.Publish(context.Background(), &Msg{Contents: "test"})

	want := outbox.PersistedMessage{
		MessageID: int64(1),
		TopicName: topic.Meta().Name,
		Data:      []byte(`{"Contents":"test"}`),
	}

	c.Assert(pms, qt.HasLen, 1)
	c.Assert(pms[0], qt.DeepEquals, want)
}
