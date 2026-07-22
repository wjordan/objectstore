package objectstore

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
)

// LabelStats holds per-label byte and request counters.
type LabelStats struct {
	PutBytes        uint64 `json:"put_bytes"`
	PutCount        uint64 `json:"put_count"`
	GetBytes        uint64 `json:"get_bytes"`
	GetCount        uint64 `json:"get_count"`
	ListCount       uint64 `json:"list_count"`
	ListObjects     uint64 `json:"list_objects"`
	ListObjectBytes uint64 `json:"list_object_bytes"`
}

// Stats is a snapshot of one Metered's counters, broken out by label
// plus separate Get, Put, and List totals across all labels.
type Stats struct {
	ByLabel   map[string]LabelStats `json:"by_label"`
	TotalGet  LabelStats            `json:"total_get"`
	TotalPut  LabelStats            `json:"total_put"`
	TotalList LabelStats            `json:"total_list"`
}

// Metered wraps a Bucket and counts bytes/requests, bucketing them by
// the label returned from the Classify function. The label set is
// open: any string returned by Classify becomes a row in Stats.
type Metered struct {
	inner    Bucket
	classify func(key string) string

	mu     sync.Mutex
	labels map[string]*labelCounters
}

type labelCounters struct {
	putBytes        atomic.Uint64
	putCount        atomic.Uint64
	getBytes        atomic.Uint64
	getCount        atomic.Uint64
	listCount       atomic.Uint64
	listObjects     atomic.Uint64
	listObjectBytes atomic.Uint64
}

// NewMetered wraps b with byte/request counters labelled by classify.
// Pass classify=nil for a single "all" label. The returned Bucket is
// safe for concurrent use.
func NewMetered(b Bucket, classify func(key string) string) *Metered {
	if classify == nil {
		classify = func(string) string { return "all" }
	}
	return &Metered{
		inner:    b,
		classify: classify,
		labels:   map[string]*labelCounters{},
	}
}

func (m *Metered) bucket(key string) *labelCounters {
	label := m.classify(key)
	m.mu.Lock()
	c, ok := m.labels[label]
	if !ok {
		c = &labelCounters{}
		m.labels[label] = c
	}
	m.mu.Unlock()
	return c
}

// Stats returns a snapshot of counters with totals summed.
func (m *Metered) Stats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := Stats{ByLabel: make(map[string]LabelStats, len(m.labels))}
	for label, c := range m.labels {
		s := LabelStats{
			PutBytes:        c.putBytes.Load(),
			PutCount:        c.putCount.Load(),
			GetBytes:        c.getBytes.Load(),
			GetCount:        c.getCount.Load(),
			ListCount:       c.listCount.Load(),
			ListObjects:     c.listObjects.Load(),
			ListObjectBytes: c.listObjectBytes.Load(),
		}
		out.ByLabel[label] = s
		out.TotalGet.GetBytes += s.GetBytes
		out.TotalGet.GetCount += s.GetCount
		out.TotalPut.PutBytes += s.PutBytes
		out.TotalPut.PutCount += s.PutCount
		out.TotalList.ListCount += s.ListCount
		out.TotalList.ListObjects += s.ListObjects
		out.TotalList.ListObjectBytes += s.ListObjectBytes
	}
	return out
}

func (m *Metered) Put(ctx context.Context, key string, body io.Reader, length int64, ifMatch *string) (string, error) {
	c := m.bucket(key)
	c.putCount.Add(1)
	etag, err := m.inner.Put(ctx, key, body, length, ifMatch)
	if err == nil && length > 0 {
		c.putBytes.Add(uint64(length))
	}
	return etag, err
}

func (m *Metered) PutStream(ctx context.Context, key string, body io.Reader, ifMatch *string) (string, error) {
	c := m.bucket(key)
	c.putCount.Add(1)
	return m.inner.PutStream(ctx, key, &countingReader{r: body, counter: &c.putBytes}, ifMatch)
}

func (m *Metered) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	c := m.bucket(key)
	c.getCount.Add(1)
	rc, etag, err := m.inner.Get(ctx, key)
	if err != nil {
		return rc, etag, err
	}
	return &countingReadCloser{rc: rc, counter: &c.getBytes}, etag, nil
}

func (m *Metered) GetRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	c := m.bucket(key)
	c.getCount.Add(1)
	rc, err := m.inner.GetRange(ctx, key, off, length)
	if err != nil {
		return rc, err
	}
	return &countingReadCloser{rc: rc, counter: &c.getBytes}, nil
}

func (m *Metered) List(ctx context.Context, prefix, startAfter string) ([]ObjectInfo, error) {
	c := m.bucket(prefix)
	c.listCount.Add(1)
	objs, err := m.inner.List(ctx, prefix, startAfter)
	if err != nil {
		return nil, err
	}
	c.listObjects.Add(uint64(len(objs)))
	var size uint64
	for _, obj := range objs {
		if obj.Size > 0 {
			size += uint64(obj.Size)
		}
	}
	c.listObjectBytes.Add(size)
	return objs, nil
}

func (m *Metered) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	c := m.bucket(key)
	c.getCount.Add(1)
	return m.inner.Stat(ctx, key)
}

func (m *Metered) Delete(ctx context.Context, key string) error {
	return m.inner.Delete(ctx, key)
}

type countingReadCloser struct {
	rc      io.ReadCloser
	counter *atomic.Uint64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.rc.Read(p)
	if n > 0 {
		c.counter.Add(uint64(n))
	}
	return n, err
}

func (c *countingReadCloser) Close() error { return c.rc.Close() }

type countingReader struct {
	r       io.Reader
	counter *atomic.Uint64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.counter.Add(uint64(n))
	}
	return n, err
}
