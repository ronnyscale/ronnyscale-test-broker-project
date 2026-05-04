package store_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ronnyscale/ronnyscale-test-broker-project/testproject/tests/testutil"
)

// Проверяем, что порядок как в ТЗ: кто первый встал — того и сняли с диска.
func TestTryDequeue_FIFO(t *testing.T) {
	st := testutil.OpenStore(t)
	ctx := context.Background()
	if err := st.Enqueue(ctx, "q", "a"); err != nil {
		t.Fatal(err)
	}
	if err := st.Enqueue(ctx, "q", "b"); err != nil {
		t.Fatal(err)
	}
	got, ok, err := st.TryDequeue(ctx, "q")
	if err != nil || !ok || got != "a" {
		t.Fatalf("1st: ok=%v err=%v got=%q", ok, err, got)
	}
	got, ok, err = st.TryDequeue(ctx, "q")
	if err != nil || !ok || got != "b" {
		t.Fatalf("2nd: ok=%v err=%v got=%q", ok, err, got)
	}
	_, ok, err = st.TryDequeue(ctx, "q")
	if err != nil || ok {
		t.Fatalf("empty: ok=%v err=%v", ok, err)
	}
}

func TestQueuesIsolated(t *testing.T) {
	st := testutil.OpenStore(t)
	ctx := context.Background()
	_ = st.Enqueue(ctx, "a", "1")
	_ = st.Enqueue(ctx, "b", "2")
	got, ok, _ := st.TryDequeue(ctx, "a")
	if !ok || got != "1" {
		t.Fatalf("a: %q ok=%v", got, ok)
	}
	got, ok, _ = st.TryDequeue(ctx, "b")
	if !ok || got != "2" {
		t.Fatalf("b: %q", got)
	}
}

// Два десятка горутин дерут одну строку — выиграть должен ровно один.
func TestTryDequeue_ConcurrentSingleMessage(t *testing.T) {
	st := testutil.OpenStore(t)
	ctx := context.Background()
	if err := st.Enqueue(ctx, "race", "solo"); err != nil {
		t.Fatal(err)
	}
	var wins atomic.Int32
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, err := st.TryDequeue(ctx, "race")
			if err != nil {
				t.Error(err)
				return
			}
			if ok {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("exactly one winner want wins=1 got %d", wins.Load())
	}
}
