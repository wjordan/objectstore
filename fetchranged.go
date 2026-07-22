package blobstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
)

// FetchOpts configures FetchRangedAt / FetchReader. Zero values take
// defaults.
type FetchOpts struct {
	Threshold   int64 // objects smaller than this use one GET; default 16MB
	PartSize    int64 // ranged part size; default 16MB
	Concurrency int   // concurrent part fetches; default 6
}

func (o FetchOpts) withDefaults() FetchOpts {
	if o.Threshold == 0 {
		o.Threshold = 16 << 20
	}
	if o.PartSize == 0 {
		o.PartSize = 16 << 20
	}
	if o.Concurrency == 0 {
		o.Concurrency = 6
	}
	return o
}

// FetchRangedAt downloads key into dst. Objects at or above the
// threshold are fetched as concurrent byte ranges written at their
// offsets (bounded memory: parts stream straight to dst), spreading the
// transfer across the S3 client's striped transports so a single
// path-pinned TCP connection cannot cap throughput; smaller objects use
// one GET. If the ranged path fails for any reason other than the
// object being missing or the context ending, it degrades to the
// single-stream path rather than failing the download.
//
// Integrity checking stays with the caller (blobstore keys carry a sha
// the caller verifies before an atomic rename).
func FetchRangedAt(ctx context.Context, b Bucket, key string, dst io.WriterAt, opts FetchOpts) (ObjectInfo, error) {
	opts = opts.withDefaults()
	info, err := b.Stat(ctx, key)
	if err != nil {
		return ObjectInfo{}, err
	}
	if info.Size < opts.Threshold {
		return info, fetchSingle(ctx, b, key, dst)
	}
	if err := fetchParts(ctx, b, key, info.Size, dst, opts); err != nil {
		if errors.Is(err, ErrNotFound) || ctx.Err() != nil {
			return info, err
		}
		slog.Warn("blobstore: ranged fetch failed; degrading to single stream", "key", key, "err", err)
		return info, fetchSingle(ctx, b, key, dst)
	}
	return info, nil
}

func fetchSingle(ctx context.Context, b Bucket, key string, dst io.WriterAt) error {
	body, _, err := b.Get(ctx, key)
	if err != nil {
		return err
	}
	defer body.Close()
	if _, err := io.Copy(io.NewOffsetWriter(dst, 0), body); err != nil {
		return fmt.Errorf("blobstore: fetch %q: %w", key, err)
	}
	return nil
}

func fetchParts(ctx context.Context, b Bucket, key string, size int64, dst io.WriterAt, opts FetchOpts) error {
	fctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sem := make(chan struct{}, opts.Concurrency)
	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	fail := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
		cancel()
	}
	for off := int64(0); off < size; off += opts.PartSize {
		select {
		case sem <- struct{}{}:
		case <-fctx.Done():
		}
		if fctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(off, ln int64) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fetchOnePart(fctx, b, key, off, ln, dst); err != nil {
				fail(err)
			}
		}(off, min(opts.PartSize, size-off))
	}
	wg.Wait()
	errMu.Lock()
	defer errMu.Unlock()
	return firstErr
}

func fetchOnePart(ctx context.Context, b Bucket, key string, off, ln int64, dst io.WriterAt) error {
	body, err := b.GetRange(ctx, key, off, ln)
	if err != nil {
		return err
	}
	defer body.Close()
	n, err := io.Copy(io.NewOffsetWriter(dst, off), body)
	if err != nil {
		return fmt.Errorf("blobstore: fetch %q part @%d: %w", key, off, err)
	}
	if n != ln {
		return fmt.Errorf("blobstore: fetch %q part @%d: got %d of %d bytes", key, off, n, ln)
	}
	return nil
}

// FetchReader is FetchRangedAt for streaming consumers: it returns a
// sequential reader over the object while parts are fetched
// concurrently ahead of the read position. Memory is bounded by
// (Concurrency+1) × PartSize of in-flight part buffers. Objects below
// the threshold return the plain (healed) Get body.
//
// If the very first ranged part fails (e.g. a backend without Range
// support), the reader degrades to a single-stream Get. Later part
// failures surface as read errors, exactly like a broken plain body.
func FetchReader(ctx context.Context, b Bucket, key string, opts FetchOpts) (io.ReadCloser, ObjectInfo, error) {
	opts = opts.withDefaults()
	info, err := b.Stat(ctx, key)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	if info.Size < opts.Threshold {
		body, _, err := b.Get(ctx, key)
		return body, info, err
	}
	rctx, cancel := context.WithCancel(ctx)
	r := &orderedReader{parent: ctx, ctx: rctx, cancel: cancel, b: b, key: key,
		slots: make(chan chan partResult, opts.Concurrency)}
	go r.produce(info.Size, opts)
	return r, info, nil
}

type partResult struct {
	off int64
	buf []byte
	err error
}

// orderedReader delivers concurrently-fetched parts in order: the
// producer emits one single-use channel per part into slots (the
// ordering), and fills each from a bounded pool of fetch goroutines.
type orderedReader struct {
	parent context.Context // outlives cancel; used for the fallback Get
	ctx    context.Context
	cancel context.CancelFunc
	b      Bucket
	key    string
	slots  chan chan partResult

	cur      bytes.Reader
	consumed int64
	fellBack io.ReadCloser
	closed   bool
}

func (r *orderedReader) produce(size int64, opts FetchOpts) {
	sem := make(chan struct{}, opts.Concurrency)
	defer close(r.slots)
	for off := int64(0); off < size; off += opts.PartSize {
		slot := make(chan partResult, 1)
		select {
		case r.slots <- slot:
		case <-r.ctx.Done():
			return
		}
		select {
		case sem <- struct{}{}:
		case <-r.ctx.Done():
			return
		}
		go func(off, ln int64) {
			defer func() { <-sem }()
			buf, err := r.fetchPart(off, ln)
			slot <- partResult{off: off, buf: buf, err: err}
		}(off, min(opts.PartSize, size-off))
	}
}

func (r *orderedReader) fetchPart(off, ln int64) ([]byte, error) {
	body, err := r.b.GetRange(r.ctx, r.key, off, ln)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	buf := make([]byte, ln)
	if _, err := io.ReadFull(body, buf); err != nil {
		return nil, fmt.Errorf("blobstore: fetch %q part @%d: %w", r.key, off, err)
	}
	return buf, nil
}

func (r *orderedReader) Read(p []byte) (int, error) {
	if r.fellBack != nil {
		return r.fellBack.Read(p)
	}
	for r.cur.Len() == 0 {
		slot, ok := <-r.slots
		if !ok {
			return 0, io.EOF
		}
		var res partResult
		select {
		case res = <-slot:
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
		if res.err != nil {
			if r.ctx.Err() != nil {
				return 0, r.ctx.Err()
			}
			if r.consumed == 0 && !errors.Is(res.err, ErrNotFound) {
				// Nothing delivered yet: degrade to a single stream.
				slog.Warn("blobstore: ranged read failed; degrading to single stream", "key", r.key, "err", res.err)
				r.cancel()
				body, _, err := r.b.Get(r.parent, r.key)
				if err != nil {
					return 0, err
				}
				r.fellBack = body
				return body.Read(p)
			}
			r.cancel()
			return 0, res.err
		}
		r.cur.Reset(res.buf)
	}
	n, err := r.cur.Read(p)
	r.consumed += int64(n)
	if err == io.EOF && r.cur.Len() == 0 {
		err = nil // next Read pulls the next part or real EOF
	}
	return n, err
}

func (r *orderedReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.cancel()
	if r.fellBack != nil {
		return r.fellBack.Close()
	}
	return nil
}
