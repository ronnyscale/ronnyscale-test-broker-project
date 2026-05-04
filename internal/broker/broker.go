// Склеиваем SQLite (хвост очереди) и живых ждунов в памяти.
// Лайфхак: если кто-то в waiters — в базе пусто, иначе GET бы уже забрал сообщение, не висел бы.
package broker

import (
	"context"
	"sync"
	"time"

	"github.com/ronnyscale/ronnyscale-test-broker-project/testproject/internal/store"
)

type Broker struct {
	st   *store.Store
	hubs sync.Map // лениво создаём хаб на каждое имя очереди
}

func New(st *store.Store) *Broker { return &Broker{st: st} }

type queueHub struct {
	b       *Broker
	name    string
	mu      sync.Mutex
	waiters []chan string // кто раньше встал — того и тапки; буфер 1, чтоб PUT не зависал
}

func (b *Broker) hub(name string) *queueHub {
	v, _ := b.hubs.LoadOrStore(name, &queueHub{b: b, name: name})
	return v.(*queueHub)
}

// Push: ждун есть — шлём ему в лоб, нет — в базу. Дубля одного сообщения не делаем.
func (b *Broker) Push(ctx context.Context, queue, body string) error {
	h := b.hub(queue)
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.waiters) > 0 {
		ch := h.waiters[0]
		h.waiters = h.waiters[1:]
		ch <- body
		return nil
	}
	return b.st.Enqueue(ctx, queue, body)
}

// Pop: сначала база, пусто — ждём до timeout (если 0, сразу мимо).
func (b *Broker) Pop(ctx context.Context, queue string, timeout time.Duration) (string, bool, error) {
	h := b.hub(queue)
	ch := make(chan string, 1)

	h.mu.Lock()
	body, ok, err := b.st.TryDequeue(ctx, queue)
	if err != nil {
		h.mu.Unlock()
		return "", false, err
	}
	if ok {
		h.mu.Unlock()
		return body, true, nil
	}
	if timeout <= 0 {
		h.mu.Unlock()
		return "", false, nil
	}
	h.waiters = append(h.waiters, ch)
	h.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case body := <-ch:
		return body, true, nil
	case <-ctx.Done():
		h.removeWaiter(ch)
		return "", false, ctx.Err()
	case <-timer.C:
		// Таймер щёлкнул, а сообщение могло влететь в ту же миллисекунду — добираем с канала.
		h.removeWaiter(ch)
		select {
		case body := <-ch:
			return body, true, nil
		default:
			return "", false, nil
		}
	}
}

func (h *queueHub) removeWaiter(ch chan string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, w := range h.waiters {
		if w == ch {
			h.waiters = append(h.waiters[:i], h.waiters[i+1:]...)
			return
		}
	}
}
