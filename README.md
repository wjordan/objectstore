# Blobstore

Blobstore is a small Go abstraction over key-addressed object storage. It
provides one `Bucket` interface with filesystem and S3-compatible
implementations, plus wrappers for key prefixes, metering, resilient reads,
and concurrent ranged downloads.

The interface deliberately follows object-store semantics rather than
filesystem semantics. Writes support atomic create and compare-and-swap
preconditions, reads return opaque entity tags, listings are ordered and
paginated, and callers retain responsibility for closing response bodies.

```go
bucket, err := blobstore.Open(ctx, "s3://example/data?region=us-west-2")
if err != nil {
	return err
}

etag, err := bucket.Put(
	ctx,
	"state/current",
	strings.NewReader("value"),
	int64(len("value")),
	blobstore.IfAbsent(),
)
```

See [Storage semantics](docs/SEMANTICS.md) for the contract implemented by
every backend.

## Backends

- `OpenFS` stores objects beneath a local filesystem directory. It is useful
  for development, tests, and single-host deployments.
- `OpenS3` supports Amazon S3 and services implementing the S3 API.
- `Open` constructs either backend from a `file://` or `s3://` URL.

Blobstore is pre-1.0. Public APIs and operational behavior may change between
minor releases.

## Development

Run the standalone checks with:

```console
go test -timeout=2s ./...
go vet ./...
```

Blobstore is licensed under the [Apache License 2.0](LICENSE).
