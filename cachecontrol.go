package blobstore

import "context"

type cacheControlKey struct{}

// WithCacheControl returns a context that requests the given Cache-Control
// value on objects written via Put or PutStream while it is in scope. An empty
// value is a no-op. Cache-aware backends persist it with the object; backends
// without a response cache ignore it.
func WithCacheControl(ctx context.Context, cacheControl string) context.Context {
	if cacheControl == "" {
		return ctx
	}
	return context.WithValue(ctx, cacheControlKey{}, cacheControl)
}

func cacheControlFromContext(ctx context.Context) string {
	cc, _ := ctx.Value(cacheControlKey{}).(string)
	return cc
}
