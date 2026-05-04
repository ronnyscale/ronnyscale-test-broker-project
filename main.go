// Точка входа: брокер очередей.
// Порт — единственный аргумент CLI. Очередь лежит в queue.db (SQLite).
package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ronnyscale/ronnyscale-test-broker-project/testproject/internal/api"
	"github.com/ronnyscale/ronnyscale-test-broker-project/testproject/internal/broker"
	"github.com/ronnyscale/ronnyscale-test-broker-project/testproject/internal/store"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("usage: %s <listen-port>", os.Args[0])
	}
	port, err := strconv.Atoi(os.Args[1])
	if err != nil || port <= 0 || port > 65535 {
		log.Fatalf("invalid port: %s", os.Args[1])
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, "queue.db")
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()

	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(port),
		Handler:           api.New(broker.New(st)),
		ReadHeaderTimeout: 5 * time.Second,
		// Чтобы длинный GET с timeout не залипал при ctrl+c — контекст рвём вместе с сигналом.
		BaseContext: func(_ net.Listener) context.Context { return ctx },
	}

	go func() {
		<-ctx.Done()
		// Аккуратно гасим сервак, без паники; 10с хватит обычно.
		shCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx)
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
