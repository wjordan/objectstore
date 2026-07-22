package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"
)

// HealOpts configures self-healing reads. Zero values take defaults.
type HealOpts struct {
	FloorBps int64         // rotate a non-final body below this rate; default 100 KiB/s
	Window   time.Duration // how long below floor (or stalled on final) before killing; default 10s
	Attempts int           // max connections per read (first + heals); default 6
	Backoff  time.Duration // pause before each re-issue; default 100ms
}

func (o HealOpts) withDefaults() HealOpts {
	if o.FloorBps == 0 {
		o.FloorBps = defaultFloorBps
	}
	if o.Window == 0 {
		o.Window = defaultWindow
	}
	if o.Attempts == 0 {
		o.Attempts = 6
	}
	if o.Backoff == 0 {
		o.Backoff = 100 * time.Millisecond
	}
	return o
}

// NewHealing wraps b so Get and GetRange bodies transparently survive a
// path-pinned or broken connection: a body that stalls, drops below the
// throughput floor, or errors mid-stream is closed (killing its TCP
// connection — Go does not re-pool an undrained conn) and the remaining
// bytes are re-requested with a fresh Range read, which lands on a
// different striped transport (clientSet) and therefore a different
// ECMP hash. This converts a bad path hash from hours to seconds.
// The final allowed connection is kept while it makes forward progress,
// however slowly, so the throughput floor cannot make a read impossible.
//
// Objects are assumed immutable for the duration of a read (objectstore
// keys are content-addressed in practice); as a guard, the object's
// ETag is captured on the first heal and any later change aborts the
// read. Callers' integrity checks (sha verify) remain the backstop.
//
// All other Bucket methods delegate unchanged.
func NewHealing(b Bucket, opts HealOpts) Bucket {
	return &healingBucket{Bucket: b, opts: opts.withDefaults()}
}

type healingBucket struct {
	Bucket
	opts HealOpts
}

func (h *healingBucket) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	body, etag, err := h.Bucket.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	return newHealingBody(ctx, h, key, 0, 0, etag, body), etag, nil
}

func (h *healingBucket) GetRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	body, err := h.Bucket.GetRange(ctx, key, off, length)
	if err != nil {
		return nil, err
	}
	return newHealingBody(ctx, h, key, off, length, "", body), nil
}

// healingBody is the self-healing io.ReadCloser. A watchdog kills a
// non-final attempt below the throughput floor and the final attempt only
// when it makes no progress; Read then re-issues the remaining range.
type healingBody struct {
	ctx  context.Context
	b    *healingBucket
	key  string
	off  int64 // original request offset (may be negative: suffix range)
	len  int64 // original request length; 0 = to end
	etag string

	consumed int64
	attempts int // connections used so far
	cur      io.ReadCloser
	dog      *watchdog
	closed   bool
}

func newHealingBody(ctx context.Context, b *healingBucket, key string, off, length int64, etag string, body io.ReadCloser) *healingBody {
	r := &healingBody{ctx: ctx, b: b, key: key, off: off, len: length, etag: etag, attempts: 1, cur: body}
	r.armWatchdog(body)
	return r
}

func (r *healingBody) armWatchdog(body io.Closer) {
	floor := r.b.opts.FloorBps
	if r.attempts >= r.b.opts.Attempts {
		floor = 0 // final connection: require progress, not a minimum rate
	}
	r.dog = newWatchdog(body, floor, r.b.opts.Window)
}

func (r *healingBody) Read(p []byte) (int, error) {
	for {
		n, err := r.cur.Read(p)
		if n > 0 {
			r.consumed += int64(n)
			r.dog.progress(int64(n))
		}
		switch {
		case err == nil:
			return n, nil
		case errors.Is(err, io.EOF) && !r.truncated():
			r.dog.stop()
			return n, io.EOF
		}
		// Watchdog kill, mid-stream error, or short EOF: heal.
		if r.ctx.Err() != nil {
			return n, r.ctx.Err()
		}
		if r.remainingDone() {
			r.dog.stop()
			return n, io.EOF
		}
		if healErr := r.heal(err); healErr != nil {
			return n, healErr
		}
		if n > 0 {
			return n, nil
		}
	}
}

// truncated reports an EOF arriving before a known-length range was
// fully delivered (a killed conn can surface as a clean EOF). Ranges
// extending past the object end are legitimately clamped, so a short
// delivery that reaches the object's actual end is not truncation;
// that costs one Stat, only on the short-EOF path.
func (r *healingBody) truncated() bool {
	if r.off < 0 || r.len == 0 || r.consumed >= r.len {
		return false
	}
	info, err := r.b.Bucket.Stat(r.ctx, r.key)
	if err == nil && r.off+r.consumed >= info.Size {
		return false
	}
	return true
}

// remainingDone reports that everything requested has been consumed, so
// an error at this exact point needs no heal.
func (r *healingBody) remainingDone() bool {
	if r.len > 0 {
		return r.consumed >= r.len
	}
	if r.off < 0 {
		return r.off+r.consumed >= 0 // suffix range fully delivered
	}
	return false
}

func (r *healingBody) heal(cause error) error {
	r.dog.stop()
	r.cur.Close()
	if r.attempts >= r.b.opts.Attempts {
		return fmt.Errorf("objectstore: read %q: %d connections exhausted: %w", r.key, r.attempts, cause)
	}
	if err := sleepHeal(r.ctx, r.b.opts.Backoff); err != nil {
		return err
	}
	// ETag guard: capture on first heal, verify on later ones.
	info, err := r.b.Bucket.Stat(r.ctx, r.key)
	if err != nil {
		return fmt.Errorf("objectstore: heal %q: stat: %w", r.key, err)
	}
	if r.etag == "" {
		r.etag = info.ETag
	} else if info.ETag != r.etag {
		return fmt.Errorf("objectstore: heal %q: object changed mid-read (etag %s -> %s)", r.key, r.etag, info.ETag)
	}
	newOff := r.off + r.consumed
	var newLen int64
	if r.len > 0 {
		newLen = r.len - r.consumed
	}
	body, err := r.b.Bucket.GetRange(r.ctx, r.key, newOff, newLen)
	if err != nil {
		return fmt.Errorf("objectstore: heal %q: %w", r.key, err)
	}
	r.attempts++
	warnHeal(r.key, newOff, r.attempts, cause)
	r.cur = body
	r.armWatchdog(body)
	return nil
}

func (r *healingBody) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.dog.stop()
	return r.cur.Close()
}

var lastHealWarn atomic.Int64 // unix nanos, package-wide rate limit

func warnHeal(key string, off int64, attempt int, cause error) {
	now := clockNow().UnixNano()
	last := lastHealWarn.Load()
	if now-last < int64(5*time.Second) || !lastHealWarn.CompareAndSwap(last, now) {
		return
	}
	slog.Warn("objectstore: healing slow/broken read on a fresh connection",
		"key", key, "resume_off", off, "attempt", attempt, "cause", cause)
}

func sleepHeal(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// watchdog kills a body whose delivery rate stays below floor for a
// full window. A zero floor means only zero progress is below floor.
// It re-arms a timer per check rather than holding a goroutine, so the
// cost on small fast reads — the vast
// majority — is one timer that never fires. Closing the body unblocks
// a Read parked on a dead connection; Read then heals as it would for
// any mid-stream error.
type watchdog struct {
	target   io.Closer
	floor    int64
	interval time.Duration
	windows  int // consecutive below-floor intervals needed to trip

	bytes   atomic.Int64
	below   int
	stopped atomic.Bool
	timer   *time.Timer
	last    int64
}

func newWatchdog(target io.Closer, floor int64, window time.Duration) *watchdog {
	w := &watchdog{target: target, floor: floor, interval: window / 4, windows: 4}
	w.timer = time.AfterFunc(w.interval, w.check)
	return w
}

func (w *watchdog) progress(n int64) { w.bytes.Add(n) }

func (w *watchdog) stop() {
	w.stopped.Store(true)
	w.timer.Stop()
}

func (w *watchdog) check() {
	if w.stopped.Load() {
		return
	}
	b := w.bytes.Load()
	minimum := max(int64(1), int64(float64(w.floor)*w.interval.Seconds()))
	if b-w.last < minimum {
		w.below++
	} else {
		w.below = 0
	}
	w.last = b
	if w.below >= w.windows {
		w.target.Close()
		return
	}
	w.timer.Reset(w.interval)
}
