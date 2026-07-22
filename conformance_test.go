package objectstore_test

// Conformance suite for any Bucket impl. Exported as runConformance
// so an S3 integration suite can run the same cases against a live
// (or mocked) bucket.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/wjordan/objectstore"
)

func runConformance(t *testing.T, name string, factory func(t *testing.T) objectstore.Bucket) {
	t.Helper()
	cases := []struct {
		name string
		fn   func(t *testing.T, b objectstore.Bucket)
	}{
		{"Put_FreshKey", testPutFresh},
		{"Put_IfAbsent_RejectsDuplicate", testPutIfAbsentDuplicate},
		{"Put_LengthMismatch", testPutLengthMismatch},
		{"Put_NoIfMatch_Overwrites", testPutNoIfMatch},
		{"Put_IfMatchEtag", testPutIfMatch},
		{"Put_IfMatchOnMissing", testPutIfMatchMissing},
		{"PutStream_FreshKey", testPutStreamFresh},
		{"PutStream_IfAbsent_RejectsDuplicate", testPutStreamIfAbsentDuplicate},
		{"Get_Missing", testGetMissing},
		{"Get_Roundtrip", testGetRoundtrip},
		{"GetRange_FullObject", testGetRangeFullObject},
		{"GetRange_Mid", testGetRangeMid},
		{"GetRange_Suffix", testGetRangeSuffix},
		{"GetRange_BeyondEnd", testGetRangeBeyondEnd},
		{"GetRange_Missing", testGetRangeMissing},
		{"List_Empty", testListEmpty},
		{"List_PrefixSorted", testListPrefixSorted},
		{"List_StartAfter", testListStartAfter},
		{"Stat_Existing", testStatExisting},
		{"Stat_Missing", testStatMissing},
		{"Delete_Existing", testDeleteExisting},
		{"Delete_Missing", testDeleteMissing},
		{"Concurrent_Put_IfAbsent_OneWinner", testConcurrentIfAbsent},
	}
	for _, c := range cases {
		t.Run(name+"/"+c.name, func(t *testing.T) {
			b := factory(t)
			c.fn(t, b)
		})
	}
}

func TestFS(t *testing.T) {
	runConformance(t, "FS", func(t *testing.T) objectstore.Bucket {
		fb, err := objectstore.OpenFS(t.TempDir())
		if err != nil {
			t.Fatalf("OpenFS: %v", err)
		}
		return fb
	})
}

// --- helpers -----------------------------------------------------------------

func putBytes(t *testing.T, b objectstore.Bucket, key string, body []byte) {
	t.Helper()
	if _, err := b.Put(context.Background(), key, bytes.NewReader(body), int64(len(body)), objectstore.IfAbsent()); err != nil {
		t.Fatalf("Put %q: %v", key, err)
	}
}

func getAll(t *testing.T, b objectstore.Bucket, key string) ([]byte, string) {
	t.Helper()
	rc, etag, err := b.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get %q: %v", key, err)
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %q: %v", key, err)
	}
	return body, etag
}

// --- cases -------------------------------------------------------------------

func testPutFresh(t *testing.T, b objectstore.Bucket) {
	putBytes(t, b, "a/b/c.bin", []byte("hello"))
	body, _ := getAll(t, b, "a/b/c.bin")
	if !bytes.Equal(body, []byte("hello")) {
		t.Fatalf("body = %q, want %q", body, "hello")
	}
}

func testPutIfAbsentDuplicate(t *testing.T, b objectstore.Bucket) {
	putBytes(t, b, "k", []byte("first"))
	_, err := b.Put(context.Background(), "k", bytes.NewReader([]byte("second")), 6, objectstore.IfAbsent())
	if !errors.Is(err, objectstore.ErrPreconditionFailed) {
		t.Fatalf("Put duplicate err = %v, want ErrPreconditionFailed", err)
	}
	body, _ := getAll(t, b, "k")
	if !bytes.Equal(body, []byte("first")) {
		t.Fatalf("body changed under failed Put: %q", body)
	}
}

func testPutLengthMismatch(t *testing.T, b objectstore.Bucket) {
	_, err := b.Put(context.Background(), "k", bytes.NewReader([]byte("hello")), 99, objectstore.IfAbsent())
	if err == nil {
		t.Fatalf("expected error on length mismatch, got nil")
	}
	_, _, err = b.Get(context.Background(), "k")
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("after length-mismatch put, Get = %v, want ErrNotFound", err)
	}
}

func testPutNoIfMatch(t *testing.T, b objectstore.Bucket) {
	ctx := context.Background()
	etag1, err := b.Put(ctx, "k", bytes.NewReader([]byte("v1")), 2, nil)
	if err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	etag2, err := b.Put(ctx, "k", bytes.NewReader([]byte("v2-longer")), 9, nil)
	if err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	if etag1 == etag2 {
		t.Fatalf("etags should differ across content: %q == %q", etag1, etag2)
	}
	body, etag := getAll(t, b, "k")
	if !bytes.Equal(body, []byte("v2-longer")) {
		t.Fatalf("body = %q, want v2-longer", body)
	}
	if etag != etag2 {
		t.Fatalf("read etag %q != put etag %q", etag, etag2)
	}
}

func testPutIfMatch(t *testing.T, b objectstore.Bucket) {
	ctx := context.Background()
	etag1, err := b.Put(ctx, "k", bytes.NewReader([]byte("v1")), 2, nil)
	if err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	wrong := "deadbeef"
	if _, err := b.Put(ctx, "k", bytes.NewReader([]byte("v2")), 2, &wrong); !errors.Is(err, objectstore.ErrPreconditionFailed) {
		t.Fatalf("Put wrong etag: err = %v, want ErrPreconditionFailed", err)
	}
	etag2, err := b.Put(ctx, "k", bytes.NewReader([]byte("v2")), 2, &etag1)
	if err != nil {
		t.Fatalf("Put right etag: %v", err)
	}
	if etag2 == etag1 {
		t.Fatalf("etag did not change after CAS update")
	}
}

func testPutIfMatchMissing(t *testing.T, b objectstore.Bucket) {
	some := "anything"
	_, err := b.Put(context.Background(), "k", bytes.NewReader([]byte("v")), 1, &some)
	if !errors.Is(err, objectstore.ErrPreconditionFailed) {
		t.Fatalf("Put IfMatch on missing: err = %v, want ErrPreconditionFailed", err)
	}
}

func testPutStreamFresh(t *testing.T, b objectstore.Bucket) {
	if _, err := b.PutStream(context.Background(), "stream/k", bytes.NewReader([]byte("streamed")), objectstore.IfAbsent()); err != nil {
		t.Fatalf("PutStream: %v", err)
	}
	body, _ := getAll(t, b, "stream/k")
	if !bytes.Equal(body, []byte("streamed")) {
		t.Fatalf("body = %q, want %q", body, "streamed")
	}
}

func testPutStreamIfAbsentDuplicate(t *testing.T, b objectstore.Bucket) {
	ctx := context.Background()
	if _, err := b.PutStream(ctx, "k", bytes.NewReader([]byte("v1")), objectstore.IfAbsent()); err != nil {
		t.Fatalf("first PutStream: %v", err)
	}
	if _, err := b.PutStream(ctx, "k", bytes.NewReader([]byte("v2")), objectstore.IfAbsent()); !errors.Is(err, objectstore.ErrPreconditionFailed) {
		t.Fatalf("second PutStream: err = %v, want ErrPreconditionFailed", err)
	}
}

func testGetMissing(t *testing.T, b objectstore.Bucket) {
	_, _, err := b.Get(context.Background(), "missing")
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Get missing: err = %v, want ErrNotFound", err)
	}
}

func testGetRoundtrip(t *testing.T, b objectstore.Bucket) {
	body := bytes.Repeat([]byte("abc"), 1000)
	putBytes(t, b, "k", body)
	got, _ := getAll(t, b, "k")
	if !bytes.Equal(got, body) {
		t.Fatalf("body mismatch: %d vs %d", len(got), len(body))
	}
}

func testGetRangeFullObject(t *testing.T, b objectstore.Bucket) {
	body := []byte("0123456789")
	putBytes(t, b, "k", body)
	rc, err := b.GetRange(context.Background(), "k", 0, 0)
	if err != nil {
		t.Fatalf("GetRange full: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, body) {
		t.Fatalf("range full = %q, want %q", got, body)
	}
}

func testGetRangeMid(t *testing.T, b objectstore.Bucket) {
	body := []byte("0123456789")
	putBytes(t, b, "k", body)
	rc, err := b.GetRange(context.Background(), "k", 3, 4)
	if err != nil {
		t.Fatalf("GetRange mid: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, []byte("3456")) {
		t.Fatalf("range mid = %q, want %q", got, "3456")
	}
}

func testGetRangeSuffix(t *testing.T, b objectstore.Bucket) {
	body := []byte("0123456789")
	putBytes(t, b, "k", body)
	rc, err := b.GetRange(context.Background(), "k", -3, 0)
	if err != nil {
		t.Fatalf("GetRange suffix: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, []byte("789")) {
		t.Fatalf("range suffix = %q, want %q", got, "789")
	}
}

func testGetRangeBeyondEnd(t *testing.T, b objectstore.Bucket) {
	body := []byte("0123456789")
	putBytes(t, b, "k", body)
	rc, err := b.GetRange(context.Background(), "k", 8, 1000)
	if err != nil {
		t.Fatalf("GetRange beyond end: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, []byte("89")) {
		t.Fatalf("range beyond end = %q, want %q", got, "89")
	}
}

func testGetRangeMissing(t *testing.T, b objectstore.Bucket) {
	_, err := b.GetRange(context.Background(), "missing", 0, 100)
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("GetRange missing: err = %v, want ErrNotFound", err)
	}
}

func testListEmpty(t *testing.T, b objectstore.Bucket) {
	objs, err := b.List(context.Background(), "anything/", "")
	if err != nil {
		t.Fatalf("List empty: %v", err)
	}
	if len(objs) != 0 {
		t.Fatalf("expected empty list, got %v", objs)
	}
}

func testListPrefixSorted(t *testing.T, b objectstore.Bucket) {
	for _, k := range []string{"a/0001", "a/0003", "a/0002", "b/zzz", "c/foo"} {
		putBytes(t, b, k, []byte(k))
	}
	objs, err := b.List(context.Background(), "a/", "")
	if err != nil {
		t.Fatalf("List a/: %v", err)
	}
	want := []string{"a/0001", "a/0002", "a/0003"}
	if len(objs) != len(want) {
		t.Fatalf("list a/: got %d, want %d (%v)", len(objs), len(want), objs)
	}
	for i, o := range objs {
		if o.Key != want[i] {
			t.Fatalf("list a/ [%d]: got %q want %q", i, o.Key, want[i])
		}
		if o.Size != int64(len(o.Key)) {
			t.Fatalf("list a/ [%d] size: got %d want %d", i, o.Size, len(o.Key))
		}
		if o.ETag == "" {
			t.Fatalf("list a/ [%d] etag empty", i)
		}
	}
}

func testListStartAfter(t *testing.T, b objectstore.Bucket) {
	for _, k := range []string{"p/01", "p/02", "p/03", "p/04"} {
		putBytes(t, b, k, []byte("x"))
	}
	objs, err := b.List(context.Background(), "p/", "p/02")
	if err != nil {
		t.Fatalf("List startAfter: %v", err)
	}
	want := []string{"p/03", "p/04"}
	if len(objs) != len(want) {
		t.Fatalf("startAfter: got %d, want %d", len(objs), len(want))
	}
	for i, o := range objs {
		if o.Key != want[i] {
			t.Fatalf("startAfter [%d]: got %q want %q", i, o.Key, want[i])
		}
	}
}

func testStatExisting(t *testing.T, b objectstore.Bucket) {
	body := []byte("hello-stat")
	putBytes(t, b, "stat/key", body)
	info, err := b.Stat(context.Background(), "stat/key")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size != int64(len(body)) {
		t.Fatalf("Stat size = %d, want %d", info.Size, len(body))
	}
	if info.ETag == "" {
		t.Fatalf("Stat etag empty")
	}
}

func testStatMissing(t *testing.T, b objectstore.Bucket) {
	_, err := b.Stat(context.Background(), "definitely/missing")
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Stat missing: err = %v, want ErrNotFound", err)
	}
}

func testDeleteExisting(t *testing.T, b objectstore.Bucket) {
	putBytes(t, b, "k", []byte("x"))
	if err := b.Delete(context.Background(), "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, _, err := b.Get(context.Background(), "k")
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("after Delete: Get = %v, want ErrNotFound", err)
	}
}

func testDeleteMissing(t *testing.T, b objectstore.Bucket) {
	err := b.Delete(context.Background(), "missing")
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Delete missing: err = %v, want ErrNotFound", err)
	}
}

func testConcurrentIfAbsent(t *testing.T, b objectstore.Bucket) {
	const N = 16
	ctx := context.Background()
	var wg sync.WaitGroup
	winners := make(chan int, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := []byte(fmt.Sprintf("worker-%d", i))
			_, err := b.Put(ctx, "shared", bytes.NewReader(body), int64(len(body)), objectstore.IfAbsent())
			if err == nil {
				winners <- i
			} else if !errors.Is(err, objectstore.ErrPreconditionFailed) {
				t.Errorf("worker %d: unexpected err %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	close(winners)
	count := 0
	for range winners {
		count++
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 winner, got %d", count)
	}
}
