package blobstore

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Striped-transport and healing defaults. Anycast object stores (Tigris)
// sit behind ECMP routing that pins each TCP 5-tuple to one network path
// for the connection's lifetime; a flow hashed onto a lossy path is
// clamped to tens of KB/s while every other flow is fine. With HTTP/2 a
// single pinned connection carries ALL multiplexed requests, so the
// mitigation is connection diversity plus rate-based eviction, not
// congestion control. Package vars so tests can tighten them.
var (
	defaultClients        = 4                // striped transports per S3 client
	defaultFloorBps int64 = 100 << 10        // sustained bytes/sec below this = pinned
	defaultWindow         = 10 * time.Second // how long below floor before acting

	clockNow = time.Now
)

// clientSet is an http.RoundTripper that stripes requests round-robin
// across K independent transports. Each transport keeps its own TCP
// connection(s) — with HTTP/2, one multiplexed conn per host — so the
// set spreads traffic across K ECMP hashes instead of concentrating it
// on one, while each connection retains full HTTP/2 stream fan-out.
//
// Per-transport health is measured at the socket: every dialed conn
// counts bytes into its stripe. A stripe with requests in flight whose
// aggregate read rate stays below floorBps for window is the pinned-path
// signature (invisible to per-request checks: each small multiplexed
// read still completes); it is swapped for a fresh transport (new dial =
// new 5-tuple) and the old one is abandoned. Health is evaluated at
// request-issue time, so there is no background goroutine or lifecycle;
// healing reads (heal.go) provide the per-request kill signal when no
// new requests arrive.
type clientSet struct {
	newTransport func(counter *atomic.Int64) *http.Transport
	floorBps     int64
	window       time.Duration

	next    atomic.Uint64
	stripes []atomic.Pointer[stripe]
	lastLog atomic.Int64 // unix nanos of last eviction WARN
}

func newClientSet(k int, forceIPv4 bool) *clientSet {
	return newClientSetWith(k, func(counter *atomic.Int64) *http.Transport {
		return stripeTransport(forceIPv4, counter)
	})
}

func newClientSetWith(k int, mk func(counter *atomic.Int64) *http.Transport) *clientSet {
	if k <= 0 {
		k = defaultClients
	}
	c := &clientSet{
		newTransport: mk,
		floorBps:     defaultFloorBps,
		window:       defaultWindow,
		stripes:      make([]atomic.Pointer[stripe], k),
	}
	for i := range c.stripes {
		c.stripes[i].Store(newStripe(c.newTransport))
	}
	return c
}

func (c *clientSet) RoundTrip(req *http.Request) (*http.Response, error) {
	i := int(c.next.Add(1) % uint64(len(c.stripes)))
	s := c.stripes[i].Load()
	if rate, sick := s.sick(clockNow(), c.floorBps, c.window); sick {
		fresh := newStripe(c.newTransport)
		if c.stripes[i].CompareAndSwap(s, fresh) {
			// New requests land on the fresh transport immediately; requests
			// still in flight on the pinned conns die by their own healing
			// watchdogs. CloseIdleConnections reaps whatever is already idle.
			s.rt.CloseIdleConnections()
			c.warnEvicted(i, rate, s.inflight.Load())
			s = fresh
		} else {
			s = c.stripes[i].Load()
		}
	}
	s.inflight.Add(1)
	resp, err := s.rt.RoundTrip(req)
	if err != nil {
		s.inflight.Add(-1)
		return nil, err
	}
	resp.Body = &stripeBody{ReadCloser: resp.Body, s: s}
	return resp, nil
}

func (c *clientSet) warnEvicted(i int, rate float64, inflight int64) {
	now := clockNow().UnixNano()
	last := c.lastLog.Load()
	if now-last < int64(5*time.Second) || !c.lastLog.CompareAndSwap(last, now) {
		return
	}
	slog.Warn("blobstore: evicting path-pinned transport",
		"stripe", i, "rate_bps", int64(rate), "inflight", inflight)
}

// stripe is one striped transport plus its socket-level accounting.
type stripe struct {
	rt       *http.Transport
	inflight atomic.Int64
	bytes    atomic.Int64 // bytes read across all of this transport's conns

	mu         sync.Mutex
	checkedAt  time.Time
	checkBytes int64
	belowSince time.Time // zero = not currently below floor
}

func newStripe(mk func(counter *atomic.Int64) *http.Transport) *stripe {
	s := &stripe{}
	s.rt = mk(&s.bytes)
	return s
}

// sick samples the stripe's aggregate read rate (at most every window/4)
// and reports whether it has stayed below floor with requests in flight
// for at least window. Idle stripes are never sick.
func (s *stripe) sick(now time.Time, floor int64, window time.Duration) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.checkedAt.IsZero() {
		s.checkedAt, s.checkBytes = now, s.bytes.Load()
		return 0, false
	}
	elapsed := now.Sub(s.checkedAt)
	if elapsed >= window/4 {
		b := s.bytes.Load()
		rate := float64(b-s.checkBytes) / elapsed.Seconds()
		s.checkedAt, s.checkBytes = now, b
		if s.inflight.Load() > 0 && rate < float64(floor) {
			if s.belowSince.IsZero() {
				// Below at least since the previous sample.
				s.belowSince = now.Add(-elapsed)
			}
		} else {
			s.belowSince = time.Time{}
		}
		if !s.belowSince.IsZero() && now.Sub(s.belowSince) >= window {
			return rate, true
		}
	}
	return 0, false
}

// stripeBody releases the stripe's in-flight slot when the response body
// is closed (idempotently; the SDK always closes bodies).
type stripeBody struct {
	io.ReadCloser
	s    *stripe
	done atomic.Bool
}

func (b *stripeBody) Close() error {
	if b.done.CompareAndSwap(false, true) {
		b.s.inflight.Add(-1)
	}
	return b.ReadCloser.Close()
}

// stripeTransport mirrors the SDK's default transport tuning with a
// byte-counting dialer (counting at the socket, below TLS, so HTTP/2
// framing and all multiplexed streams are captured).
func stripeTransport(forceIPv4 bool, counter *atomic.Int64) *http.Transport {
	d := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if forceIPv4 && (network == "tcp" || network == "tcp6") {
				network = "tcp4"
			}
			conn, err := d.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &countingConn{Conn: conn, n: counter}, nil
		},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

type countingConn struct {
	net.Conn
	n *atomic.Int64
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.n.Add(int64(n))
	return n, err
}
