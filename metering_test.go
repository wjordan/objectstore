package objectstore_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/wjordan/objectstore"
)

func TestMeteredCountsListWork(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fs, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	m := objectstore.NewMetered(fs, func(key string) string {
		if len(key) >= 3 && key[:3] == "db/" {
			return "db"
		}
		return "other"
	})
	if _, err := m.Put(ctx, "db/a", bytes.NewReader([]byte("abc")), 3, objectstore.IfAbsent()); err != nil {
		t.Fatalf("Put db/a: %v", err)
	}
	if _, err := m.Put(ctx, "other/b", bytes.NewReader([]byte("zz")), 2, objectstore.IfAbsent()); err != nil {
		t.Fatalf("Put other/b: %v", err)
	}
	if _, err := m.List(ctx, "db/", ""); err != nil {
		t.Fatalf("List db/: %v", err)
	}

	stats := m.Stats()
	db := stats.ByLabel["db"]
	if db.ListCount != 1 || db.ListObjects != 1 || db.ListObjectBytes != 3 {
		t.Fatalf("db list stats = count:%d objects:%d bytes:%d, want 1/1/3",
			db.ListCount, db.ListObjects, db.ListObjectBytes)
	}
	if stats.TotalList.ListCount != 1 || stats.TotalList.ListObjects != 1 || stats.TotalList.ListObjectBytes != 3 {
		t.Fatalf("total list stats = count:%d objects:%d bytes:%d, want 1/1/3",
			stats.TotalList.ListCount, stats.TotalList.ListObjects, stats.TotalList.ListObjectBytes)
	}
}
