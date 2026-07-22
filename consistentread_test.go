package blobstore

import (
	"context"
	"testing"
)

func TestConsistentReadContext(t *testing.T) {
	if consistentReadFromContext(context.Background()) {
		t.Fatal("bare context must not request a consistent read")
	}
	if !consistentReadFromContext(WithConsistentRead(context.Background())) {
		t.Fatal("WithConsistentRead must request a consistent read")
	}
}
