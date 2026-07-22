package blobstore

import (
	"net/url"
	"testing"
)

func mustParseS3(t *testing.T, raw string) S3Config {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	cfg, err := s3ConfigFromURL(u)
	if err != nil {
		t.Fatalf("s3ConfigFromURL(%q): %v", raw, err)
	}
	return cfg
}

func TestS3ConfigFromURL(t *testing.T) {
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")

	cfg := mustParseS3(t, "s3://bkt/p/q?region=eu&endpoint=https://x.dev")
	if cfg.Bucket != "bkt" || cfg.Prefix != "p/q" {
		t.Fatalf("bucket/prefix: %q %q", cfg.Bucket, cfg.Prefix)
	}
	if cfg.Region != "eu" || cfg.EndpointURL != "https://x.dev" {
		t.Fatalf("region/endpoint: %q %q", cfg.Region, cfg.EndpointURL)
	}
	if !cfg.UsePathStyle { // endpoint implies path-style
		t.Fatal("endpoint should imply path-style")
	}
	if cfg.ForceIPv4 {
		t.Fatal("ForceIPv4 should default false")
	}

	if cfg := mustParseS3(t, "s3://b?ip-version=4"); !cfg.ForceIPv4 {
		t.Fatal("ip-version=4 should set ForceIPv4")
	}
	if cfg := mustParseS3(t, "s3://b?ip-version=6"); cfg.ForceIPv4 {
		t.Fatal("ip-version=6 should leave ForceIPv4 false")
	}
	if cfg := mustParseS3(t, "s3://b?path-style=false&endpoint=https://x"); cfg.UsePathStyle {
		t.Fatal("explicit path-style=false should override the endpoint default")
	}
}

func TestS3ConfigFromURLErrors(t *testing.T) {
	for _, raw := range []string{
		"s3://b?ip-version=9",
		"s3://b?path-style=maybe",
		"s3://user:pw@b",
		"s3://",
	} {
		u, err := url.Parse(raw)
		if err != nil {
			continue // a parse failure is also a rejection
		}
		if _, err := s3ConfigFromURL(u); err == nil {
			t.Errorf("expected error for %q", raw)
		}
	}
}
