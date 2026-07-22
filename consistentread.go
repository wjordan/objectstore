package objectstore

import "context"

// consistentReadKey is the context key requesting a linearizable read.
type consistentReadKey struct{}

// WithConsistentRead returns a context that requests a strongly-consistent
// read on objects fetched via Get while it is in scope. On Tigris
// Global/Dual-region buckets an ordinary Get is served from the REGIONAL
// replica, which can lag the global leader — a reader in a trailing region
// then observes a stale object. For coordination objects that feed a
// compare-and-swap decision this is not merely slow but wrong: the stale read
// drives a doomed CAS that fails against the leader's current state.
// X-Tigris-Consistent routes the read to the global leader, pairing the read
// with the consistent CAS write (see conditionalOpts) so the whole
// read-modify-write is linearizable. Backends without regional replication
// (FS) ignore it. Carried on the context so the hint adds no parameter to the
// Bucket interface and passes transparently through the Prefixed/Metered
// wrappers.
func WithConsistentRead(ctx context.Context) context.Context {
	return context.WithValue(ctx, consistentReadKey{}, true)
}

// consistentReadFromContext reports whether WithConsistentRead is in scope.
func consistentReadFromContext(ctx context.Context) bool {
	v, _ := ctx.Value(consistentReadKey{}).(bool)
	return v
}

// IsConsistentRead is the exported form of consistentReadFromContext, for
// backends and test doubles outside this package that need to honor (or
// assert) the WithConsistentRead hint.
func IsConsistentRead(ctx context.Context) bool {
	return consistentReadFromContext(ctx)
}
