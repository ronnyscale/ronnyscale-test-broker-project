// Package testutil — общий каркас для тестов: живой SQLite + HTTP как в проде.
package testutil

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/ronnyscale/ronnyscale-test-broker-project/testproject/internal/api"
	"github.com/ronnyscale/ronnyscale-test-broker-project/testproject/internal/broker"
	"github.com/ronnyscale/ronnyscale-test-broker-project/testproject/internal/store"
)

// NewHTTPServer поднимает тот же стек, что main, на httptest.
func NewHTTPServer(tb testing.TB) *httptest.Server {
	tb.Helper()
	db := filepath.Join(tb.TempDir(), "queue.db")
	st, err := store.Open(context.Background(), db)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = st.Close() })
	srv := httptest.NewServer(api.New(broker.New(st)))
	tb.Cleanup(srv.Close)
	return srv
}

// OpenStore — для контрактных тестов store без HTTP.
func OpenStore(tb testing.TB) *store.Store {
	tb.Helper()
	db := filepath.Join(tb.TempDir(), "queue.db")
	st, err := store.Open(context.Background(), db)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = st.Close() })
	return st
}
