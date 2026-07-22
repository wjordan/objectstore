package objectstore

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// connCountingServer is an httptest TLS server (HTTP/2 enabled) that
// counts distinct accepted TCP connections.
func connCountingServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var conns atomic.Int64
	srv := httptest.NewUnstartedServer(handler)
	srv.EnableHTTP2 = true
	srv.Config.ConnState = func(c net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv, &conns
}

func testClientSet(t *testing.T, srv *httptest.Server, k int) *clientSet {
	t.Helper()
	tlsCfg := srv.Client().Transport.(*http.Transport).TLSClientConfig
	return newClientSetWith(k, func(counter *atomic.Int64) *http.Transport {
		tr := stripeTransport(false, counter)
		tr.TLSClientConfig = tlsCfg.Clone()
		return tr
	})
}

// TestClientSetStripesAcrossConnections is the automated stand-in for
// the design's `ss` check: K stripes must produce K distinct TCP
// connections even though every request could multiplex onto one
// HTTP/2 conn — the exact failure mode the client set exists to
// prevent.
func TestClientSetStripesAcrossConnections(t *testing.T) {
	srv, conns := connCountingServer(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	})
	const k = 4
	cs := testClientSet(t, srv, k)
	client := &http.Client{Transport: cs}
	var proto string
	for i := 0; i < 4*k; i++ {
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		proto = resp.Proto
	}
	if proto != "HTTP/2.0" {
		t.Fatalf("expected HTTP/2 negotiation (h2 benefits retained), got %s", proto)
	}
	if got := conns.Load(); got != k {
		t.Fatalf("expected %d distinct TCP conns for %d stripes, got %d", k, k, got)
	}
}

func TestStripeSickDetection(t *testing.T) {
	defer func(f func() time.Time) { clockNow = f }(clockNow)
	now := time.Unix(1000, 0)
	clockNow = func() time.Time { return now }

	s := newStripe(func(c *atomic.Int64) *http.Transport { return &http.Transport{} })
	floor, window := int64(100<<10), 10*time.Second

	if _, sick := s.sick(now, floor, window); sick {
		t.Fatal("fresh stripe must not be sick")
	}
	// In-flight but starved: below floor across the full window.
	s.inflight.Add(1)
	now = now.Add(3 * time.Second)
	if _, sick := s.sick(now, floor, window); sick {
		t.Fatal("3s below floor is not yet sick")
	}
	now = now.Add(8 * time.Second)
	if _, sick := s.sick(now, floor, window); !sick {
		t.Fatal("11s starved with in-flight requests must be sick")
	}
	// Healthy traffic resets the below-floor tracking.
	s2 := newStripe(func(c *atomic.Int64) *http.Transport { return &http.Transport{} })
	s2.sick(now, floor, window)
	s2.inflight.Add(1)
	now = now.Add(3 * time.Second)
	s2.bytes.Add(10 << 20)
	s2.sick(now, floor, window)
	now = now.Add(8 * time.Second)
	s2.bytes.Add(10 << 20)
	if _, sick := s2.sick(now, floor, window); sick {
		t.Fatal("fast stripe must not be sick")
	}
	// Idle (no in-flight) stripes are never sick.
	s3 := newStripe(func(c *atomic.Int64) *http.Transport { return &http.Transport{} })
	s3.sick(now, floor, window)
	now = now.Add(20 * time.Second)
	if _, sick := s3.sick(now, floor, window); sick {
		t.Fatal("idle stripe must not be sick")
	}
}

// TestClientSetEvictsPinnedStripe drives the full path: a stripe whose
// connection stalls (in-flight, no bytes) is evicted at request-issue
// time and replaced, and the replacement serves the request.
func TestClientSetEvictsPinnedStripe(t *testing.T) {
	var stalled sync.Once
	release := make(chan struct{})
	srv, _ := connCountingServer(t, func(w http.ResponseWriter, r *http.Request) {
		first := false
		stalled.Do(func() { first = true })
		if first {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-release // pin: headers sent, body never arrives
			return
		}
		io.WriteString(w, "ok")
	})
	defer close(release)

	cs := testClientSet(t, srv, 1)
	cs.floorBps = 1 << 30 // any real rate is below floor
	cs.window = 40 * time.Millisecond
	client := &http.Client{Transport: cs}

	orig := cs.stripes[0].Load()
	resp, err := client.Get(srv.URL) // pinned request; body will hang
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Poll: request-issue-time health checks must evict the stalled
	// stripe once it has been starved for a full window.
	deadline := time.Now().Add(2 * time.Second)
	for cs.stripes[0].Load() == orig {
		if time.Now().After(deadline) {
			t.Fatal("stripe was not evicted")
		}
		time.Sleep(5 * time.Millisecond)
		resp2, err := client.Get(srv.URL)
		if err != nil {
			continue // in-flight on the dying stripe; retried next loop
		}
		io.Copy(io.Discard, resp2.Body)
		resp2.Body.Close()
	}
	// The evicted stripe was replaced and subsequent requests succeed.
	resp3, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("request after eviction: %v", err)
	}
	body, _ := io.ReadAll(resp3.Body)
	resp3.Body.Close()
	if string(body) != "ok" {
		t.Fatalf("unexpected body %q", body)
	}
}
