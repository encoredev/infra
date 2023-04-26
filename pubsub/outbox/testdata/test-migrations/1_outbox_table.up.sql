CREATE TABLE outbox (
	id BIGSERIAL PRIMARY KEY,
	topic TEXT NOT NULL,
	data JSONB NOT NULL,
	inserted_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX outbox_topic_idx ON outbox (topic, id);