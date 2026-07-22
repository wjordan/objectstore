// Package objectstore provides a small object-storage abstraction with S3 and
// filesystem implementations. Bucket mirrors common object-store semantics so
// implementations remain thin and callers can use conditional writes, ranged
// reads, and ordered listings without depending on a provider SDK.
//
// Keys are byte-equivalent across calls; List returns lex-sorted keys;
// range reads use byte offsets; conditional writes use opaque ETags.
// Anything S3-specific that doesn't generalize (SSE, storage classes,
// lifecycle) lives behind constructor options on the S3 impl, not on
// the interface.
//
// Errors: every method returns ErrNotFound for a missing key and
// ErrPreconditionFailed for a conditional write whose precondition did
// not hold. Other errors are wrapped with operation context.
package objectstore

import (
	"context"
	"errors"
	"io"
	"time"
)

// ErrNotFound is returned by Get, GetRange, and Delete when the key
// does not exist. Callers can use errors.Is.
var ErrNotFound = errors.New("objectstore: object not found")

// ErrPreconditionFailed is returned by Put / PutStream when the
// supplied ifMatch precondition did not hold. Callers can use errors.Is.
var ErrPreconditionFailed = errors.New("objectstore: precondition failed")

// ObjectInfo describes one object in a List response.
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
}

// Bucket is the object-storage abstraction. Every implementation
// guarantees:
//
//   - Put with ifMatch is atomic and linearizable. nil ifMatch
//     overwrites; ifMatch=&"" requires the key not exist (returns
//     ErrPreconditionFailed if it does); ifMatch=&etag requires the
//     current ETag equals etag.
//   - PutStream is identical to Put but accepts a body of unknown
//     length; bodies are always fully consumed before preconditions
//     are evaluated, so streaming producers (e.g. an io.Pipe-fed
//     compressor) cannot deadlock on early-return.
//   - WithCacheControl scopes the stored response-cache policy for Put and
//     PutStream without changing this interface. Cache-aware backends persist
//     it with the object; other backends ignore it.
//   - Get and GetRange return io.ReadCloser bodies that the caller
//     must Close. Closing before EOF is permitted.
//   - List returns keys in lexicographic order. startAfter is
//     exclusive: results have Key > startAfter. Pages are bounded;
//     iterate with startAfter = lastKey to enumerate.
//   - Delete on a missing key returns ErrNotFound; idempotent callers
//     should ignore that.
type Bucket interface {
	// Put writes body of length bytes to key. ifMatch sets the
	// precondition (see package doc). Returns the new ETag.
	Put(ctx context.Context, key string, body io.Reader, length int64, ifMatch *string) (etag string, err error)

	// PutStream is Put for unknown-length bodies. The body is fully
	// consumed before the precondition is evaluated.
	PutStream(ctx context.Context, key string, body io.Reader, ifMatch *string) (etag string, err error)

	// Get returns the object's body and current ETag. The caller must
	// Close the reader. Returns ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) (body io.ReadCloser, etag string, err error)

	// GetRange returns length bytes starting at offset off. length == 0
	// means "to end of object." A negative off addresses from the end
	// (e.g., off=-4096, length=0 fetches the last 4 KiB). Returns
	// ErrNotFound if the key does not exist.
	GetRange(ctx context.Context, key string, off, length int64) (body io.ReadCloser, err error)

	// Stat returns object metadata (size, etag, last-modified) without
	// fetching the body. S3-backed impls use HeadObject; FS uses
	// os.Stat + a deferred-content-hash etag. Returns ErrNotFound if
	// the key does not exist.
	Stat(ctx context.Context, key string) (ObjectInfo, error)

	// List returns up to a backend-defined maximum (typically 1000)
	// objects whose keys begin with prefix and are lexicographically
	// greater than startAfter, sorted ascending.
	List(ctx context.Context, prefix, startAfter string) ([]ObjectInfo, error)

	// Delete removes the object at key. Returns ErrNotFound if absent.
	Delete(ctx context.Context, key string) error
}

// IfAbsent is the ifMatch sentinel meaning "succeed iff the key does
// not currently exist."
func IfAbsent() *string { e := ""; return &e }
