// SQLite: храним только то, что реально лежит в очереди без живого получателя.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "modernc.org/sqlite" // sqlite без cgo — деплой попроще, без танцев с gcc
)

type Store struct {
	db *sql.DB
}

func Open(ctx context.Context, path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_foreign_keys=on&_journal_mode=WAL&_busy_timeout=5000", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
create table if not exists messages (
	id integer primary key autoincrement,
	queue text not null,
	body text not null
);
create index if not exists idx_messages_queue_id on messages(queue, id);
`)
	return err
}

// TryDequeue: забираем самое старое в транзакции, чтоб два GET не схватили одно и то же.
func (s *Store) TryDequeue(ctx context.Context, queue string) (body string, ok bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer func() { _ = tx.Rollback() }()

	var id int64
	q := `select id, body from messages where queue = ? order by id asc limit 1`
	row := tx.QueryRowContext(ctx, q, queue)
	switch err := row.Scan(&id, &body); {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, tx.Commit()
	case err != nil:
		return "", false, err
	}
	if _, err := tx.ExecContext(ctx, `delete from messages where id = ?`, id); err != nil {
		return "", false, err
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return body, true, nil
}

// Enqueue: просто дописали хвост в таблицу.
func (s *Store) Enqueue(ctx context.Context, queue, body string) error {
	_, err := s.db.ExecContext(ctx, `insert into messages(queue, body) values(?, ?)`, queue, body)
	return err
}
