# Contributing

Changes to externally visible storage behavior begin in
[`docs/SEMANTICS.md`](docs/SEMANTICS.md). Update the contract first, then make
the implementation and conformance tests match it.

Keep backend-specific behavior behind constructors or wrappers rather than
expanding `Bucket` unless every implementation can provide the same guarantee.
New implementations should run the shared conformance suite.

Before submitting a change, run:

```console
gofmt -w .
go vet ./...
go test -timeout=2s ./...
git diff --check
```
