package blobstore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// FS stores objects under a root directory. Keys map directly onto
// paths: "a/b/c.bin" lives at <root>/a/b/c.bin. Conditional writes use
// atomic Link or Rename through per-key temp files. ETags are sha256
// of the body.
//
// Intended for tests, single-host development, and as a minimum-
// viable backend without object storage. Cross-process correctness
// relies on filesystem rename-into-existing-target atomicity, which
// POSIX guarantees.
type FS struct {
	root string

	// mu serializes writes against the same key from within one
	// process. Cross-process correctness still relies on filesystem
	// atomicity; this mutex prevents racy reads of the etag during a
	// CAS window in single-process tests.
	mu sync.Mutex
}

// OpenFS opens (or creates) an FS rooted at root. The root is created
// with 0o755 if it does not exist.
func OpenFS(root string) (*FS, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("blobstore/fs: mkdir root %q: %w", root, err)
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("blobstore/fs: abs %q: %w", root, err)
	}
	return &FS{root: abs}, nil
}

func (f *FS) keyPath(key string) (string, error) {
	if key == "" {
		return "", errors.New("blobstore/fs: empty key")
	}
	if strings.Contains(key, "..") {
		return "", fmt.Errorf("blobstore/fs: refusing key with %q: %q", "..", key)
	}
	parts := strings.Split(key, "/")
	for _, p := range parts {
		if p == "" {
			return "", fmt.Errorf("blobstore/fs: empty key segment in %q", key)
		}
	}
	return filepath.Join(append([]string{f.root}, parts...)...), nil
}

// Put writes body to key with precondition ifMatch. See Bucket.Put.
func (f *FS) Put(ctx context.Context, key string, body io.Reader, length int64, ifMatch *string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	path, err := f.keyPath(key)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", fmt.Errorf("blobstore/fs: mkdir parent: %w", err)
	}
	tmp := path + ".tmp." + randSuffix()
	etag, err := writeAndSync(tmp, body, length)
	if err != nil {
		_ = os.Remove(tmp)
		return "", err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if err := checkIfMatch(path, ifMatch); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	if err := commit(tmp, path, ifAbsent(ifMatch)); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	return etag, nil
}

// PutStream writes a body of unknown length. See Bucket.PutStream.
func (f *FS) PutStream(ctx context.Context, key string, body io.Reader, ifMatch *string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	path, err := f.keyPath(key)
	if err != nil {
		return "", err
	}

	// Spool body fully BEFORE locking and BEFORE evaluating the
	// precondition, so a streaming producer (e.g., io.Pipe-fed
	// compressor) finishes without deadlocking even when the put will
	// ultimately fail.
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", fmt.Errorf("blobstore/fs: mkdir parent: %w", err)
	}
	tmp := path + ".tmp." + randSuffix()
	etag, err := writeAndSync(tmp, body, -1)
	if err != nil {
		_ = os.Remove(tmp)
		return "", err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if err := checkIfMatch(path, ifMatch); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	if err := commit(tmp, path, ifAbsent(ifMatch)); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	return etag, nil
}

// Get returns the object's body and ETag.
func (f *FS) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	path, err := f.keyPath(key)
	if err != nil {
		return nil, "", err
	}
	etag, err := fileETag(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, "", ErrNotFound
		}
		return nil, "", fmt.Errorf("blobstore/fs: etag %q: %w", key, err)
	}
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, "", ErrNotFound
		}
		return nil, "", fmt.Errorf("blobstore/fs: open %q: %w", key, err)
	}
	return file, etag, nil
}

// GetRange returns length bytes starting at off.
func (f *FS) GetRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := f.keyPath(key)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("blobstore/fs: open %q: %w", key, err)
	}
	st, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("blobstore/fs: stat %q: %w", key, err)
	}
	size := st.Size()
	start := off
	if start < 0 {
		start = size + start
		if start < 0 {
			start = 0
		}
	}
	if start > size {
		start = size
	}
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("blobstore/fs: seek %q: %w", key, err)
	}
	if length <= 0 {
		return file, nil
	}
	end := start + length
	if end > size {
		end = size
	}
	return &limitedFile{File: file, n: end - start}, nil
}

// List returns objects with key beginning with prefix and key > startAfter.
func (f *FS) List(ctx context.Context, prefix, startAfter string) ([]ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []ObjectInfo
	err := filepath.WalkDir(f.root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		// Skip non-regular files: callers may co-locate Unix sockets
		// or FIFOs in the same root (e.g. the daemon's gossip
		// listeners under peers/). Reading them as objects would hang
		// or fail with EOPNOTSUPP.
		if !d.Type().IsRegular() {
			return nil
		}
		// Skip temp files left by interrupted writes.
		if strings.Contains(d.Name(), ".tmp.") {
			return nil
		}
		rel, err := filepath.Rel(f.root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if !strings.HasPrefix(key, prefix) {
			return nil
		}
		if startAfter != "" && key <= startAfter {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		etag, err := fileETag(path)
		if err != nil {
			return err
		}
		out = append(out, ObjectInfo{
			Key:          key,
			Size:         info.Size(),
			LastModified: info.ModTime(),
			ETag:         etag,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("blobstore/fs: walk: %w", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	const maxKeys = 1000
	if len(out) > maxKeys {
		out = out[:maxKeys]
	}
	return out, nil
}

// Stat returns object metadata without opening the body. The etag is
// computed from the file's contents (sha256 of bytes), matching the
// etag returned by Get / Put.
func (f *FS) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	path, err := f.keyPath(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ObjectInfo{}, ErrNotFound
		}
		return ObjectInfo{}, fmt.Errorf("blobstore/fs: stat %q: %w", key, err)
	}
	etag, err := fileETag(path)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("blobstore/fs: etag %q: %w", key, err)
	}
	return ObjectInfo{
		Key:          key,
		Size:         info.Size(),
		LastModified: info.ModTime(),
		ETag:         etag,
	}, nil
}

// Delete removes key. Returns ErrNotFound if missing.
func (f *FS) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := f.keyPath(key)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrNotFound
		}
		return fmt.Errorf("blobstore/fs: remove %q: %w", key, err)
	}
	// Best-effort prune of empty parent dirs up to root.
	dir := filepath.Dir(path)
	for dir != f.root && dir != filepath.Dir(dir) {
		if err := os.Remove(dir); err != nil {
			break
		}
		dir = filepath.Dir(dir)
	}
	return nil
}

// checkIfMatch evaluates the precondition against the current state
// of path. Returns ErrPreconditionFailed when it doesn't hold.
//   - nil:    no precondition.
//   - &"":    file must not exist.
//   - &etag:  file's current ETag must equal etag.
func checkIfMatch(path string, ifMatch *string) error {
	if ifMatch == nil {
		return nil
	}
	if *ifMatch == "" {
		if _, err := os.Stat(path); err == nil {
			return ErrPreconditionFailed
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("blobstore/fs: stat: %w", err)
		}
		return nil
	}
	cur, err := fileETag(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrPreconditionFailed
		}
		return fmt.Errorf("blobstore/fs: etag: %w", err)
	}
	if cur != *ifMatch {
		return ErrPreconditionFailed
	}
	return nil
}

// commit installs tmp at path. When mustBeAbsent is true, uses Link so
// concurrent creation is detected (EEXIST → ErrPreconditionFailed);
// otherwise Rename, which atomically replaces any existing file.
func commit(tmp, path string, mustBeAbsent bool) error {
	if mustBeAbsent {
		if err := os.Link(tmp, path); err != nil {
			if errors.Is(err, os.ErrExist) {
				return ErrPreconditionFailed
			}
			return fmt.Errorf("blobstore/fs: link: %w", err)
		}
		_ = os.Remove(tmp)
		return nil
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("blobstore/fs: rename: %w", err)
	}
	return nil
}

func ifAbsent(ifMatch *string) bool { return ifMatch != nil && *ifMatch == "" }

type limitedFile struct {
	*os.File
	n int64
}

func (l *limitedFile) Read(p []byte) (int, error) {
	if l.n <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > l.n {
		p = p[:l.n]
	}
	n, err := l.File.Read(p)
	l.n -= int64(n)
	return n, err
}

// writeAndSync streams body to a fresh file at path, fsync's, and
// returns the sha256 ETag. length==-1 means "unknown"; otherwise the
// actual byte count must match length or the file is left in place
// for the caller to remove and an error is returned.
func writeAndSync(path string, body io.Reader, length int64) (string, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return "", fmt.Errorf("blobstore/fs: create %q: %w", path, err)
	}
	h := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(file, h), body)
	if copyErr != nil {
		_ = file.Close()
		return "", fmt.Errorf("blobstore/fs: write %q: %w", path, copyErr)
	}
	if length >= 0 && written != length {
		_ = file.Close()
		return "", fmt.Errorf("blobstore/fs: wrote %d != declared %d", written, length)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return "", fmt.Errorf("blobstore/fs: fsync %q: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("blobstore/fs: close %q: %w", path, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// fileETag returns the sha256-hex ETag of path's contents. Lifted out
// so List, Get, GetRange, and the CAS path share one definition.
func fileETag(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	var h hash.Hash = sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func randSuffix() string {
	var b [8]byte
	_, _ = io.ReadFull(rand.Reader, b[:])
	return hex.EncodeToString(b[:])
}
