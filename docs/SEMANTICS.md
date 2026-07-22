# Storage semantics

This document defines the behavior every `Bucket` implementation must expose.
These guarantees are part of the public API; backend-specific optimizations do
not weaken them.

## Keys and metadata

Keys are opaque strings and remain byte-equivalent across operations.
`ObjectInfo` reports the key, byte size, last-modified time, and an opaque
entity tag. Callers may compare an entity tag for equality or supply it as a
write precondition, but must not interpret its contents.

## Conditional writes

`Put` and `PutStream` atomically evaluate an optional precondition and replace
the object when that precondition succeeds:

- a nil precondition permits an unconditional replacement;
- `IfAbsent()` succeeds only when the key does not exist; and
- an existing entity tag succeeds only when it identifies the current object.

A failed precondition returns `ErrPreconditionFailed` without changing the
object. Backends must evaluate each conditional write against the same
linearizable view used to commit the replacement.

`PutStream` has the same storage behavior as `Put`, but accepts an unknown
length. It consumes the complete input before returning, including when a
precondition ultimately fails, so a producer feeding the reader cannot be
left blocked.

## Reads and ranges

`Get` returns the complete object and its entity tag. `GetRange` returns
`length` bytes beginning at `off`; a zero length means through the end of the
object, and a negative offset is measured from the end. Returned readers must
be closed by the caller and may be closed before reaching EOF.

`Stat` returns metadata without transferring the body. Missing objects return
`ErrNotFound` from `Get`, `GetRange`, `Stat`, and `Delete`.

`WithConsistentRead` requests a strongly consistent read from backends that
offer multiple consistency levels. Backends with only one read consistency
level may ignore the hint.

## Listing

`List` returns a backend-sized page of objects whose keys begin with `prefix`
and are lexicographically greater than the exclusive `startAfter` value.
Results are sorted in ascending key order. To continue, pass the last returned
key as the next `startAfter` value.

## Scoped behavior

`Prefixed` creates a view that prepends a fixed prefix to all operations while
presenting keys relative to that view. `WithCacheControl` attaches a response
cache policy to writes; backends without response-cache metadata may ignore
it. Wrappers must preserve all other `Bucket` guarantees.
