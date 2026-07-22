package blobstore

import (
	"context"
	"io"
	"strings"
)

// Prefixed returns a Bucket view that prepends prefix to every key.
// A trailing "/" on prefix is normalized; an empty prefix returns the
// inner bucket unchanged.
//
// Useful for topic-scoping a shared bucket across multiple workloads:
// `blobstore.Prefixed(bucket, "images/")` and
// `blobstore.Prefixed(bucket, "documents/")` give two isolated views
// over one underlying connection.
func Prefixed(inner Bucket, prefix string) Bucket {
	if prefix == "" {
		return inner
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	// Flatten nested wraps so chained prefixing pays one
	// concatenation per key, not one per layer.
	if pb, ok := inner.(*prefixedBucket); ok {
		return &prefixedBucket{inner: pb.inner, prefix: pb.prefix + prefix}
	}
	return &prefixedBucket{inner: inner, prefix: prefix}
}

type prefixedBucket struct {
	inner  Bucket
	prefix string
}

func (p *prefixedBucket) Put(ctx context.Context, key string, body io.Reader, length int64, ifMatch *string) (string, error) {
	return p.inner.Put(ctx, p.prefix+key, body, length, ifMatch)
}

func (p *prefixedBucket) PutStream(ctx context.Context, key string, body io.Reader, ifMatch *string) (string, error) {
	return p.inner.PutStream(ctx, p.prefix+key, body, ifMatch)
}

func (p *prefixedBucket) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	return p.inner.Get(ctx, p.prefix+key)
}

func (p *prefixedBucket) GetRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	return p.inner.GetRange(ctx, p.prefix+key, off, length)
}

func (p *prefixedBucket) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	info, err := p.inner.Stat(ctx, p.prefix+key)
	if err != nil {
		return ObjectInfo{}, err
	}
	info.Key = strings.TrimPrefix(info.Key, p.prefix)
	return info, nil
}

func (p *prefixedBucket) List(ctx context.Context, prefix, startAfter string) ([]ObjectInfo, error) {
	full := p.prefix + prefix
	startKey := startAfter
	if startAfter != "" {
		startKey = p.prefix + startAfter
	}
	objs, err := p.inner.List(ctx, full, startKey)
	if err != nil {
		return nil, err
	}
	for i := range objs {
		objs[i].Key = strings.TrimPrefix(objs[i].Key, p.prefix)
	}
	return objs, nil
}

func (p *prefixedBucket) Delete(ctx context.Context, key string) error {
	return p.inner.Delete(ctx, p.prefix+key)
}
