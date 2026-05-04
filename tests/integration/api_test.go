package integration

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ronnyscale/ronnyscale-test-broker-project/testproject/tests/testutil"
)

func TestPutMissingV_BadRequest(t *testing.T) {
	srv := testutil.NewHTTPServer(t)
	req, err := http.NewRequest(http.MethodPut, srv.URL+"/pet", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", resp.StatusCode)
	}
	if n, _ := io.Copy(io.Discard, resp.Body); n != 0 {
		t.Fatalf("body should be empty")
	}
}

func TestPutGet_FIFOFromAssignment(t *testing.T) {
	srv := testutil.NewHTTPServer(t)
	mustPut(t, srv.URL, "/pet", "cat")
	mustPut(t, srv.URL, "/pet", "dog")
	if got := mustGet(t, srv.URL, "/pet"); got != "cat" {
		t.Fatalf("1st pet: %q", got)
	}
	if got := mustGet(t, srv.URL, "/pet"); got != "dog" {
		t.Fatalf("2nd pet: %q", got)
	}
	for i := 0; i < 2; i++ {
		resp, err := http.Get(srv.URL + "/pet")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("empty #%d: status=%d", i, resp.StatusCode)
		}
	}
	mustPut(t, srv.URL, "/role", "manager")
	mustPut(t, srv.URL, "/role", "executive")
	if got := mustGet(t, srv.URL, "/role"); got != "manager" {
		t.Fatalf("role: %q", got)
	}
	if got := mustGet(t, srv.URL, "/role"); got != "executive" {
		t.Fatalf("role: %q", got)
	}
	resp, _ := http.Get(srv.URL + "/role")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("empty role: %d", resp.StatusCode)
	}
}

func TestGetTimeout_ReceivesLatePut(t *testing.T) {
	srv := testutil.NewHTTPServer(t)
	done := make(chan string, 1)
	go func() {
		resp, err := http.Get(srv.URL + "/slow?timeout=3")
		if err != nil {
			done <- "err:" + err.Error()
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		done <- string(b) + "|" + resp.Status
	}()
	time.Sleep(200 * time.Millisecond)
	mustPut(t, srv.URL, "/slow", "payload")
	res := <-done
	if !strings.HasSuffix(res, "|200 OK") {
		t.Fatalf("want 200, got %q", res)
	}
	if !strings.HasPrefix(res, "payload|") {
		t.Fatalf("body: %q", res)
	}
}

func TestGetTimeout_NoMessage_404(t *testing.T) {
	srv := testutil.NewHTTPServer(t)
	resp, err := http.Get(srv.URL + "/ghost?timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestFIFO_WaitingReceivers(t *testing.T) {
	srv := testutil.NewHTTPServer(t)
	// Два GET с таймаутом: кто раньше ушёл в ожидание — тому первый PUT.
	r1 := make(chan string, 1)
	r2 := make(chan string, 1)
	go func() {
		resp, err := http.Get(srv.URL + "/fifo?timeout=10")
		if err != nil {
			r1 <- "err:" + err.Error()
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		r1 <- string(b) + "|" + resp.Status
	}()
	time.Sleep(150 * time.Millisecond)
	go func() {
		resp, err := http.Get(srv.URL + "/fifo?timeout=10")
		if err != nil {
			r2 <- "err:" + err.Error()
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		r2 <- string(b) + "|" + resp.Status
	}()
	time.Sleep(150 * time.Millisecond)
	mustPut(t, srv.URL, "/fifo", "alice")
	mustPut(t, srv.URL, "/fifo", "bob")
	g1 := <-r1
	g2 := <-r2
	if !strings.HasPrefix(g1, "alice|") || !strings.Contains(g1, "200") {
		t.Fatalf("first waiter got %q", g1)
	}
	if !strings.HasPrefix(g2, "bob|") || !strings.Contains(g2, "200") {
		t.Fatalf("second waiter got %q", g2)
	}
}

func TestInvalidTimeout_BadRequest(t *testing.T) {
	srv := testutil.NewHTTPServer(t)
	for _, q := range []string{"timeout=-1", "timeout=nan"} {
		resp, err := http.Get(srv.URL + "/x?" + q)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s: status=%d", q, resp.StatusCode)
		}
	}
}

func TestWrongMethod_405(t *testing.T) {
	srv := testutil.NewHTTPServer(t)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/q", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestBadPath_404(t *testing.T) {
	srv := testutil.NewHTTPServer(t)
	resp, err := http.Get(srv.URL + "/a/b")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestConcurrentPutGet_NoLostMessages(t *testing.T) {
	srv := testutil.NewHTTPServer(t)
	const n = 200
	errCh := make(chan error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			u := srv.URL + "/stress?v=" + url.QueryEscape(strconv.Itoa(i))
			req, err := http.NewRequest(http.MethodPut, u, nil)
			if err != nil {
				errCh <- err
				return
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				errCh <- err
				return
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errCh <- fmt.Errorf("put %d: status %d", i, resp.StatusCode)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[string]bool)
	for range n {
		body := mustGet(t, srv.URL, "/stress")
		if seen[body] {
			t.Fatalf("duplicate %q", body)
		}
		seen[body] = true
	}
	resp, _ := http.Get(srv.URL + "/stress")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("queue should be empty")
	}
}

func mustPut(t *testing.T, base, path, v string) {
	t.Helper()
	u := base + path + "?v=" + url.QueryEscape(v)
	req, err := http.NewRequest(http.MethodPut, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT %s: %d", u, resp.StatusCode)
	}
}

func mustGet(t *testing.T, base, path string) string {
	t.Helper()
	resp, err := http.Get(base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: %d", path, resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
