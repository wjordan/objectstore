package blobstore_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/wjordan/blobstore"
)

// memWriterAt is a bounds-checked in-memory io.WriterAt.
type memWriterAt struct {
	mu  sync.Mutex
	buf []byte
}

func (w *memWriterAt) WriteAt(p []byte, off int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if need := int(off) + len(p); need > len(w.buf) {
		w.buf = append(w.buf, make([]byte, need-len(w.buf))...)
	}
	return copy(w.buf[off:], p), nil
}

func putRandom(t *testing.T, size int) (blobstore.Bucket, []byte) {
	t.Helper()
	fs, err := blobstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := make([]byte, size)
	rand.Read(content)
	if _, err := fs.Put(context.Background(), "k", bytes.NewReader(content), int64(size), nil); err != nil {
		t.Fatal(err)
	}
	return fs, content
}

// smallParts exercises the ranged path on tiny objects: unaligned part
// size so the last part is short.
var smallParts = blobstore.FetchOpts{Threshold: 1 << 10, PartSize: 100, Concurrency: 4}

func TestFetchRangedAtReassembles(t *testing.T) {
	b, content := putRandom(t, 100*100+37) // unaligned tail part
	var dst memWriterAt
	info, err := blobstore.FetchRangedAt(context.Background(), b, "k", &dst, smallParts)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len(content)) {
		t.Fatalf("info.Size = %d, want %d", info.Size, len(content))
	}
	if !bytes.Equal(dst.buf, content) {
		t.Fatalf("reassembled content mismatch (%d bytes)", len(dst.buf))
	}
}

func TestFetchRangedAtBelowThresholdSingleGet(t *testing.T) {
	b, content := putRandom(t, 500)
	sb := &scriptedBucket{Bucket: b}
	var dst memWriterAt
	if _, err := blobstore.FetchRangedAt(context.Background(), sb, "k", &dst, smallParts); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(dst.buf, content) {
		t.Fatal("content mismatch")
	}
	if n := len(sb.recorded()); n != 0 {
		t.Fatalf("expected no ranged reads below threshold, got %d", n)
	}
}

func TestFetchRangedAtMissing(t *testing.T) {
	b, _ := putRandom(t, 10)
	var dst memWriterAt
	_, err := blobstore.FetchRangedAt(context.Background(), b, "missing", &dst, smallParts)
	if !errors.Is(err, blobstore.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// rangeFailBucket fails every GetRange (a backend without Range support).
type rangeFailBucket struct{ blobstore.Bucket }

func (r *rangeFailBucket) GetRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	return nil, errors.New("range not supported")
}

func TestFetchRangedAtDegradesToSingleStream(t *testing.T) {
	b, content := putRandom(t, 4<<10)
	var dst memWriterAt
	if _, err := blobstore.FetchRangedAt(context.Background(), &rangeFailBucket{b}, "k", &dst,
		blobstore.FetchOpts{Threshold: 1 << 10, PartSize: 1 << 10, Concurrency: 2}); err != nil {
		t.Fatalf("expected single-stream degrade, got %v", err)
	}
	if !bytes.Equal(dst.buf, content) {
		t.Fatal("content mismatch after degrade")
	}
}

func TestFetchReaderOrdered(t *testing.T) {
	b, content := putRandom(t, 100*7+13)
	body, info, err := blobstore.FetchReader(context.Background(), b, "k",
		blobstore.FetchOpts{Threshold: 100, PartSize: 100, Concurrency: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len(content)) || !bytes.Equal(got, content) {
		t.Fatalf("ordered content mismatch: got %d bytes, want %d", len(got), len(content))
	}
}

func TestFetchReaderBelowThreshold(t *testing.T) {
	b, content := putRandom(t, 50)
	body, _, err := blobstore.FetchReader(context.Background(), b, "k", smallParts)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	got, _ := io.ReadAll(body)
	if !bytes.Equal(got, content) {
		t.Fatal("content mismatch")
	}
}

func TestFetchReaderDegradesToSingleStream(t *testing.T) {
	b, content := putRandom(t, 4<<10)
	body, _, err := blobstore.FetchReader(context.Background(), &rangeFailBucket{b}, "k",
		blobstore.FetchOpts{Threshold: 1 << 10, PartSize: 1 << 10, Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("expected degrade to single stream, got %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("content mismatch after degrade")
	}
}

// failAtPart fails GetRange calls for a specific offset, persistently.
type failAtPart struct {
	blobstore.Bucket
	off int64
}

func (f *failAtPart) GetRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	if off == f.off {
		return nil, errors.New("persistent part failure")
	}
	return f.Bucket.GetRange(ctx, key, off, length)
}

func TestFetchReaderMidStreamFailureSurfaces(t *testing.T) {
	b, content := putRandom(t, 1000)
	body, _, err := blobstore.FetchReader(context.Background(), &failAtPart{b, 500}, "k",
		blobstore.FetchOpts{Threshold: 100, PartSize: 100, Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err == nil {
		t.Fatal("expected mid-stream error to surface")
	}
	// Everything before the failed part must have been delivered in order.
	if len(got) < 400 || !bytes.Equal(got[:400], content[:400]) {
		t.Fatalf("prefix mismatch: got %d bytes", len(got))
	}
}

func TestFetchRangedAtWithHealingBucket(t *testing.T) {
	// The intended production composition: multipart over a healing
	// bucket; a part's connection dies mid-stream and heals invisibly.
	b, content := putRandom(t, 1000)
	sb := &scriptedBucket{Bucket: b}
	sb.onRange = func(call int, body io.ReadCloser) io.ReadCloser {
		if call == 2 {
			return errAfter(10, body)
		}
		return body
	}
	h := blobstore.NewHealing(sb, fastHeal)
	var dst memWriterAt
	if _, err := blobstore.FetchRangedAt(context.Background(), h, "k", &dst,
		blobstore.FetchOpts{Threshold: 100, PartSize: 100, Concurrency: 2}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(dst.buf, content) {
		t.Fatal("content mismatch with healed part")
	}
}
