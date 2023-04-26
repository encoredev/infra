package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"

	"encore.dev/storage/sqldb"
)

// SQLDBStore returns a Store implementation backed by a *sqldb.Database.
// The provided database must have a single table with the following structure:
//
//	CREATE TABLE outbox (
//		id BIGSERIAL PRIMARY KEY,
//		topic TEXT NOT NULL,
//		data JSONB NOT NULL,
//		inserted_at TIMESTAMPTZ NOT NULL
//	);
//	CREATE INDEX outbox_topic_idx ON outbox (topic, id);
func SQLDBStore(db *sqldb.Database) Store {
	return &sqldbStore{db: db}
}

type sqldbStore struct {
	db *sqldb.Database
}

func (s *sqldbStore) CheckoutBatch(ctx context.Context, topicNames []string, limit int) (MessageBatch, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("outbox store: begin tx: %v", err)
	}

	pms, err := s.queryBatch(ctx, tx, topicNames, limit)
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("outbox store: query batch: %v", err)
	}
	return &sqldbBatch{tx: tx, pms: pms}, nil
}

func (s *sqldbStore) queryBatch(ctx context.Context, tx *sqldb.Tx, topicNames []string, limit int) ([]PersistedMessage, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, topic, data
		FROM outbox
		WHERE topic = ANY($1)
		ORDER BY id ASC
		LIMIT $2
		FOR UPDATE
	`, topicNames, limit)
	if err != nil {
		return nil, fmt.Errorf("outbox store: query database: %v", err)
	}
	defer rows.Close()

	var pms []PersistedMessage
	for rows.Next() {
		var pm PersistedMessage
		var id int64 // ensure we have the right type (pm.MessageType is of type any)
		if err := rows.Scan(&id, &pm.TopicName, &pm.Data); err != nil {
			return nil, fmt.Errorf("outbox store: scan row: %v", err)
		}
		pm.MessageID = id
		pms = append(pms, pm)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox store: query database: %v", err)
	}
	return pms, nil
}

type sqldbBatch struct {
	tx         *sqldb.Tx
	didPublish bool
	pms        []PersistedMessage
}

func (b *sqldbBatch) Messages() []PersistedMessage {
	return b.pms
}

func (b *sqldbBatch) MarkPublished(ctx context.Context, msg PersistedMessage, publishID string) error {
	_, err := b.tx.Exec(ctx, `
		DELETE FROM outbox
		WHERE id = $1
	`, msg.MessageID)
	if err != nil {
		return fmt.Errorf("outbox store: mark message published: %v", err)
	}
	b.didPublish = true
	return nil
}

func (b *sqldbBatch) Close() error {
	if b.didPublish {
		return b.tx.Commit()
	} else {
		return b.tx.Rollback()
	}
}

// TxPersister returns a PersistFunc that inserts published messages
// within the given transaction.
func TxPersister(tx *sqldb.Tx) PersistFunc {
	insertFn := func(ctx context.Context, query string, args ...any) (id int64, err error) {
		err = tx.QueryRow(ctx, query, args...).Scan(&id)
		return
	}
	return txPersister(insertFn)
}

// PgxTxPersister is like TxPersister but for a [pgx.Tx].
func PgxTxPersister(tx pgx.Tx) PersistFunc {
	insertFn := func(ctx context.Context, query string, args ...any) (id int64, err error) {
		err = tx.QueryRow(ctx, query, args...).Scan(&id)
		return
	}
	return txPersister(insertFn)
}

// StdlibTxPersister is like TxPersister but for a [*sql.Tx].
func StdlibTxPersister(tx *sql.Tx) PersistFunc {
	insertFn := func(ctx context.Context, query string, args ...any) (id int64, err error) {
		err = tx.QueryRowContext(ctx, query, args...).Scan(&id)
		return
	}
	return txPersister(insertFn)
}

func txPersister(insertFn func(ctx context.Context, query string, args ...any) (int64, error)) PersistFunc {
	return func(ctx context.Context, topicName string, msg any) (string, error) {
		data, err := json.Marshal(msg)
		if err != nil {
			return "", err
		}

		id, err := insertFn(ctx, `
			INSERT INTO outbox (topic, data, inserted_at)
			VALUES ($1, $2, NOW())
			RETURNING id;
		`, topicName, data)
		return strconv.FormatInt(id, 10), err
	}
}
