package objectstore

import (
	"context"
	"testing"
)

func TestWithCacheControl(t *testing.T) {
	base := context.Background()
	if got := cacheControlFromContext(base); got != "" {
		t.Fatalf("bare context cache control = %q", got)
	}
	if got := cacheControlFromContext(WithCacheControl(base, "no-store")); got != "no-store" {
		t.Fatalf("scoped cache control = %q, want no-store", got)
	}
	if got := WithCacheControl(base, ""); got != base {
		t.Fatal("empty cache control should preserve the original context")
	}
}
