package blobstore

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// Open constructs a Bucket from a URL string. Recognized schemes:
//
//   - file:///abs/path
//     An FS rooted at the given absolute path. No query options.
//
//   - s3://bucket/prefix?region=…&endpoint=…&path-style=true|false
//     An S3 over the named bucket. The path component (after the
//     bucket) is the Prefix. Query parameters override defaults:
//     region      — bucket region; falls back to AWS_REGION /
//     AWS_DEFAULT_REGION env vars when unset.
//     endpoint    — base URL for S3-compatible services (R2, MinIO,
//     Tigris, GCS S3 API). Implies path-style=true
//     unless overridden.
//     path-style  — force path-style addressing (true|false).
//     ip-version  — 4 to dial the endpoint over IPv4 only (some
//     anycast endpoints route IPv6 to a far PoP).
//     clients     — striped transport count (distinct TCP conns to
//     spread ECMP path risk); 0/unset = default (4).
//     heal        — false disables self-healing reads (watchdog +
//     resume-on-fresh-connection); default true.
//     Credentials come from the SDK default chain. The URL must not
//     carry credentials.
func Open(ctx context.Context, raw string) (Bucket, error) {
	if raw == "" {
		return nil, errors.New("blobstore: empty URL")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("blobstore: parse %q: %w", raw, err)
	}
	switch u.Scheme {
	case "file":
		return openFileURL(u)
	case "s3":
		return openS3URL(ctx, u)
	default:
		return nil, fmt.Errorf("blobstore: unsupported scheme %q in %q", u.Scheme, raw)
	}
}

func openFileURL(u *url.URL) (Bucket, error) {
	if u.Host != "" && u.Host != "localhost" {
		return nil, fmt.Errorf("blobstore: file:// host must be empty or localhost, got %q", u.Host)
	}
	if u.Path == "" {
		return nil, errors.New("blobstore: file:// URL missing path")
	}
	return OpenFS(u.Path)
}

func openS3URL(ctx context.Context, u *url.URL) (Bucket, error) {
	cfg, err := s3ConfigFromURL(u)
	if err != nil {
		return nil, err
	}
	b, err := OpenS3(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if v := u.Query().Get("heal"); v != "" {
		if on, err := strconv.ParseBool(v); err != nil {
			return nil, fmt.Errorf("blobstore: invalid heal=%q: %w", v, err)
		} else if !on {
			return b, nil
		}
	}
	return NewHealing(b, HealOpts{}), nil
}

// s3ConfigFromURL maps an s3:// URL to an S3Config (no network I/O), so URL
// parsing is unit-testable apart from SDK client construction.
func s3ConfigFromURL(u *url.URL) (S3Config, error) {
	if u.User != nil {
		return S3Config{}, errors.New("blobstore: s3:// URL must not embed credentials; use the SDK credential chain")
	}
	bucket := u.Host
	if bucket == "" {
		return S3Config{}, errors.New("blobstore: s3:// URL missing bucket")
	}
	q := u.Query()
	cfg := S3Config{
		Bucket:      bucket,
		Prefix:      strings.TrimPrefix(u.Path, "/"),
		Region:      q.Get("region"),
		EndpointURL: q.Get("endpoint"),
	}
	if cfg.Region == "" {
		for _, k := range []string{"AWS_REGION", "AWS_DEFAULT_REGION"} {
			if v := os.Getenv(k); v != "" {
				cfg.Region = v
				break
			}
		}
	}
	if cfg.EndpointURL != "" {
		cfg.UsePathStyle = true
	}
	if v := q.Get("path-style"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return S3Config{}, fmt.Errorf("blobstore: invalid path-style=%q: %w", v, err)
		}
		cfg.UsePathStyle = b
	}
	if v := q.Get("ip-version"); v != "" {
		switch v {
		case "4":
			cfg.ForceIPv4 = true
		case "6":
			// default dual-stack already prefers IPv6 when available
		default:
			return S3Config{}, fmt.Errorf("blobstore: invalid ip-version=%q (want 4 or 6)", v)
		}
	}
	if v := q.Get("clients"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return S3Config{}, fmt.Errorf("blobstore: invalid clients=%q (want a positive integer)", v)
		}
		cfg.Clients = n
	}
	return cfg, nil
}
