package objectstore_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
)

// fastHeal keeps heal retries fast and the watchdog effectively off
// (huge window) so tests exercise only error-driven healing.
var fastHeal = objectstore.HealOpts{Backoff: time.Nanosecond, Window: time.Hour}

// scriptedBucket wraps a Bucket, records every GetRange, and lets a
// test mutate each returned body by call index (0-based).
type scriptedBucket struct {
	objectstore.Bucket
	mu      sync.Mutex
	nRange  int
	ranges  [][2]int64
	onRange func(call int, body io.ReadCloser) io.ReadCloser
	onGet   func(body io.ReadCloser) io.ReadCloser
}

func (s *scriptedBucket) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	body, etag, err := s.Bucket.Get(ctx, key)
	if err == nil && s.onGet != nil {
		body = s.onGet(body)
	}
	return body, etag, err
}

func (s *scriptedBucket) GetRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	body, err := s.Bucket.GetRange(ctx, key, off, length)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	call := s.nRange
	s.nRange++
	s.ranges = append(s.ranges, [2]int64{off, length})
	s.mu.Unlock()
	if s.onRange != nil {
		body = s.onRange(call, body)
	}
	return body, nil
}

func (s *scriptedBucket) recorded() [][2]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][2]int64(nil), s.ranges...)
}

var errInjected = errors.New("injected conn failure")

// errAfter yields n bytes of body, then an injected error.
func errAfter(n int64, body io.ReadCloser) io.ReadCloser {
	return &mutatedBody{r: io.LimitReader(body, n), c: body, then: errInjected}
}

// eofAfter yields n bytes, then a clean (truncating) EOF.
func eofAfter(n int64, body io.ReadCloser) io.ReadCloser {
	return &mutatedBody{r: io.LimitReader(body, n), c: body, then: io.EOF}
}

type mutatedBody struct {
	r    io.Reader
	c    io.Closer
	then error
}

func (m *mutatedBody) Read(p []byte) (int, error) {
	n, err := m.r.Read(p)
	if err == io.EOF {
		err = m.then
	}
	return n, err
}

func (m *mutatedBody) Close() error { return m.c.Close() }

func newScripted(t *testing.T, key, content string) (*scriptedBucket, objectstore.Bucket) {
	t.Helper()
	fs, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Put(context.Background(), key, strings.NewReader(content), int64(len(content)), nil); err != nil {
		t.Fatal(err)
	}
	sb := &scriptedBucket{Bucket: fs}
	return sb, objectstore.NewHealing(sb, fastHeal)
}

func TestHealResumesAfterMidStreamError(t *testing.T) {
	content := strings.Repeat("0123456789", 10) // 100 bytes
	sb, h := newScripted(t, "k", content)
	sb.onRange = func(call int, body io.ReadCloser) io.ReadCloser {
		if call == 0 {
			return errAfter(30, body)
		}
		return body
	}
	body, err := h.GetRange(context.Background(), "k", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != content {
		t.Fatalf("content mismatch: got %d bytes %q...", len(got), got[:10])
	}
	r := sb.recorded()
	if len(r) != 2 || r[1] != [2]int64{30, 70} {
		t.Fatalf("expected resume at [30,70], recorded %v", r)
	}
}

func TestHealGetResumesWithEtagGuard(t *testing.T) {
	content := strings.Repeat("abcdef", 50) // 300 bytes
	sb, h := newScripted(t, "k", content)
	sb.onGet = func(body io.ReadCloser) io.ReadCloser { return errAfter(120, body) }
	body, _, err := h.Get(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != content {
		t.Fatalf("content mismatch after heal: %d bytes", len(got))
	}
	r := sb.recorded()
	if len(r) != 1 || r[0] != [2]int64{120, 0} {
		t.Fatalf("expected one resume at [120,0], recorded %v", r)
	}
}

func TestHealSuffixRangeResume(t *testing.T) {
	content := strings.Repeat("x", 900) + strings.Repeat("y", 100)
	sb, h := newScripted(t, "k", content)
	sb.onRange = func(call int, body io.ReadCloser) io.ReadCloser {
		if call == 0 {
			return errAfter(40, body)
		}
		return body
	}
	body, err := h.GetRange(context.Background(), "k", -100, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != strings.Repeat("y", 100) {
		t.Fatalf("suffix content mismatch: %q", got)
	}
	r := sb.recorded()
	if len(r) != 2 || r[1] != [2]int64{-60, 0} {
		t.Fatalf("expected suffix resume at [-60,0], recorded %v", r)
	}
}

func TestHealTruncatedEOF(t *testing.T) {
	content := strings.Repeat("z", 200)
	sb, h := newScripted(t, "k", content)
	sb.onRange = func(call int, body io.ReadCloser) io.ReadCloser {
		if call == 0 {
			return eofAfter(50, body) // clean EOF short of the known length
		}
		return body
	}
	body, err := h.GetRange(context.Background(), "k", 0, 200)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 200 {
		t.Fatalf("expected 200 bytes despite truncated EOF, got %d", len(got))
	}
}

func TestHealAttemptsExhausted(t *testing.T) {
	sb, _ := newScripted(t, "k", strings.Repeat("q", 100))
	opts := fastHeal
	opts.Attempts = 3
	h := objectstore.NewHealing(sb, opts)
	sb.onRange = func(call int, body io.ReadCloser) io.ReadCloser {
		return errAfter(1, body) // every connection dies after 1 byte
	}
	body, err := h.GetRange(context.Background(), "k", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	_, err = io.ReadAll(body)
	if err == nil || !strings.Contains(err.Error(), "connections exhausted") {
		t.Fatalf("expected exhaustion error, got %v", err)
	}
	if !errors.Is(err, errInjected) {
		t.Fatalf("expected wrapped cause, got %v", err)
	}
	if got := len(sb.recorded()); got != 3 {
		t.Fatalf("expected 3 connections (Attempts), recorded %d", got)
	}
}

func TestHealAbortsWhenObjectChanges(t *testing.T) {
	content := strings.Repeat("m", 100)
	sb, h := newScripted(t, "k", content)
	sb.onRange = func(call int, body io.ReadCloser) io.ReadCloser {
		if call == 1 {
			// Mutate the object between heal 1 (etag captured) and heal 2.
			if _, err := sb.Bucket.Put(context.Background(), "k", strings.NewReader(strings.Repeat("M", 100)), 100, nil); err != nil {
				panic(err)
			}
		}
		return errAfter(10, body)
	}
	body, err := h.GetRange(context.Background(), "k", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	_, err = io.ReadAll(body)
	if err == nil || !strings.Contains(err.Error(), "changed mid-read") {
		t.Fatalf("expected etag-mismatch abort, got %v", err)
	}
}

// stallBody blocks reads after n bytes until closed — the shape of a
// path-pinned connection. Close unblocks the pending Read, mirroring
// net/http response bodies.
type stallBody struct {
	r      io.Reader
	c      io.Closer
	done   chan struct{}
	closed sync.Once
}

func newStallBody(n int64, body io.ReadCloser) *stallBody {
	return &stallBody{r: io.LimitReader(body, n), c: body, done: make(chan struct{})}
}

func (s *stallBody) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if err == io.EOF && n == 0 {
		<-s.done
		return 0, errors.New("stalled body closed")
	}
	return n, err
}

func (s *stallBody) Close() error {
	s.closed.Do(func() { close(s.done) })
	return s.c.Close()
}

type slowBody struct{ io.ReadCloser }

func (s slowBody) Read(p []byte) (int, error) {
	time.Sleep(4 * time.Millisecond)
	return s.ReadCloser.Read(p[:min(len(p), 1)])
}

func readRange(t *testing.T, b objectstore.Bucket, n int64) ([]byte, error) {
	t.Helper()
	body, err := b.GetRange(context.Background(), "k", 0, n)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	return io.ReadAll(body)
}

func TestWatchdogKillsStalledBodyAndHeals(t *testing.T) {
	content := strings.Repeat("w", 100)
	sb, _ := newScripted(t, "k", content)
	h := objectstore.NewHealing(sb, objectstore.HealOpts{
		FloorBps: 1 << 30, // any real rate is "too slow"
		Window:   40 * time.Millisecond,
		Backoff:  time.Nanosecond,
	})
	sb.onRange = func(call int, body io.ReadCloser) io.ReadCloser {
		if call == 0 {
			return newStallBody(20, body)
		}
		return body
	}
	body, err := h.GetRange(context.Background(), "k", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	start := time.Now()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("ReadAll after watchdog heal: %v", err)
	}
	if string(got) != content {
		t.Fatalf("content mismatch: %d bytes", len(got))
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("watchdog took %v; should trip within ~window", d)
	}
	r := sb.recorded()
	if len(r) != 2 || r[1] != [2]int64{20, 80} {
		t.Fatalf("expected resume at [20,80] after stall kill, recorded %v", r)
	}
}

func TestFinalAttemptAcceptsSlowProgress(t *testing.T) {
	content := strings.Repeat("s", 64)
	sb, _ := newScripted(t, "k", content)
	h := objectstore.NewHealing(sb, objectstore.HealOpts{
		FloorBps: 1 << 20, // slowBody is always below this floor
		Window:   80 * time.Millisecond,
		Attempts: 1,
	})
	sb.onRange = func(_ int, body io.ReadCloser) io.ReadCloser {
		return slowBody{body}
	}
	got, err := readRange(t, h, int64(len(content)))
	if err != nil {
		t.Fatalf("slow final attempt: %v", err)
	}
	if string(got) != content {
		t.Fatalf("content mismatch: got %d bytes", len(got))
	}
}

func TestFinalAttemptStillKillsStall(t *testing.T) {
	sb, _ := newScripted(t, "k", strings.Repeat("x", 100))
	h := objectstore.NewHealing(sb, objectstore.HealOpts{
		FloorBps: 1 << 30,
		Window:   40 * time.Millisecond,
		Attempts: 1,
	})
	sb.onRange = func(_ int, body io.ReadCloser) io.ReadCloser {
		return newStallBody(10, body)
	}
	if _, err := readRange(t, h, 100); err == nil || !strings.Contains(err.Error(), "connections exhausted") {
		t.Fatalf("expected final stall to exhaust connections, got %v", err)
	}
}

func TestHealingConformance(t *testing.T) {
	runConformance(t, "HealingFS", func(t *testing.T) objectstore.Bucket {
		fb, err := objectstore.OpenFS(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return objectstore.NewHealing(fb, objectstore.HealOpts{Backoff: time.Nanosecond})
	})
}
