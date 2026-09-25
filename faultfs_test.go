package afero

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
)

var errInjected = errors.New("injected fault")

// copyChunks copies src to dst on fs using small writes so that tests can
// target individual Write calls.
func copyChunks(fs Fs, src, dst string, chunk int) error {
	in, err := fs.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := fs.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	buf := make([]byte, chunk)
	for {
		n, rerr := in.Read(buf)
		if n > 0 {
			if _, werr := out.Write(buf[:n]); werr != nil {
				return werr
			}
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return rerr
		}
	}
}

// Acceptance: copying a file on MemMapFs where the second Write only writes
// a 3-byte prefix and then fails. The real destination content must match
// exactly what was actually written.
func TestFaultFsShortWriteCopy(t *testing.T) {
	mem := NewMemMapFs()
	if err := WriteFile(mem, "src.txt", []byte("hello world"), 0o644); err != nil {
		t.Fatal(err)
	}

	fs := NewFaultFs(mem, FaultRule{
		Op:          FaultOpWrite,
		Path:        "dst.txt",
		N:           2,
		Err:         errInjected,
		WritePrefix: 3,
	})

	err := copyChunks(fs, "src.txt", "dst.txt", 4)
	if err == nil {
		t.Fatal("expected copy to fail")
	}
	if !errors.Is(err, errInjected) {
		t.Fatalf("expected errors.Is(err, errInjected), got %v", err)
	}

	// First write stored "hell", the faulty second write stored its 3-byte
	// prefix "o w" before failing.
	content, rerr := ReadFile(mem, "dst.txt")
	if rerr != nil {
		t.Fatal(rerr)
	}
	if want := "hello w"; string(content) != want {
		t.Fatalf("dst content = %q, want %q", content, want)
	}
}

func TestFaultFsRename(t *testing.T) {
	mem := NewMemMapFs()
	if err := WriteFile(mem, "old.txt", []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	fs := NewFaultFs(mem, FaultRule{
		Op:   FaultOpRename,
		Path: "old.txt",
		N:    1,
		Err:  errInjected,
	})

	err := fs.Rename("old.txt", "new.txt")
	if !errors.Is(err, errInjected) {
		t.Fatalf("expected injected rename error, got %v", err)
	}
	if _, serr := mem.Stat("old.txt"); serr != nil {
		t.Fatalf("old.txt should still exist: %v", serr)
	}
	if _, serr := mem.Stat("new.txt"); !errors.Is(serr, ErrFileNotFound) {
		t.Fatalf("new.txt should not exist, got %v", serr)
	}

	// The rule fires only once: the second rename succeeds.
	if err := fs.Rename("old.txt", "new.txt"); err != nil {
		t.Fatalf("second rename should succeed, got %v", err)
	}
	if _, serr := mem.Stat("new.txt"); serr != nil {
		t.Fatalf("new.txt should exist after rename: %v", serr)
	}
}

func TestFaultFsClose(t *testing.T) {
	mem := NewMemMapFs()
	fs := NewFaultFs(mem, FaultRule{
		Op:   FaultOpClose,
		Path: "file.txt",
		N:    1,
		Err:  errInjected,
	})

	f, err := fs.Create("file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("data"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); !errors.Is(err, errInjected) {
		t.Fatalf("expected injected close error, got %v", err)
	}

	// Fires only once: closing again reaches the underlying file.
	if err := f.Close(); err != nil {
		t.Fatalf("second close should succeed, got %v", err)
	}
}

func TestFaultFsRuleFiresOnce(t *testing.T) {
	mem := NewMemMapFs()
	fs := NewFaultFs(mem, FaultRule{
		Op:   FaultOpWrite,
		Path: "once.txt",
		N:    2,
		Err:  errInjected,
	})

	f, err := fs.Create("once.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if _, err := f.WriteString("a"); err != nil {
		t.Fatalf("first write should succeed, got %v", err)
	}
	if _, err := f.WriteString("b"); !errors.Is(err, errInjected) {
		t.Fatalf("second write should fail, got %v", err)
	}
	if _, err := f.WriteString("c"); err != nil {
		t.Fatalf("third write should succeed, got %v", err)
	}

	content, err := ReadFile(mem, "once.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "ac" {
		t.Fatalf("content = %q, want %q", content, "ac")
	}
}

func TestFaultFsRepeatRule(t *testing.T) {
	mem := NewMemMapFs()
	fs := NewFaultFs(mem, FaultRule{
		Op:     FaultOpWrite,
		Path:   "repeat.txt",
		N:      2,
		Err:    errInjected,
		Repeat: true,
	})

	f, err := fs.Create("repeat.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if _, err := f.WriteString("a"); err != nil {
		t.Fatalf("first write should succeed, got %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := f.WriteString("x"); !errors.Is(err, errInjected) {
			t.Fatalf("write %d should keep failing with Repeat, got %v", i+2, err)
		}
	}
}

func TestFaultFsPathCleaning(t *testing.T) {
	mem := NewMemMapFs()
	if err := mem.MkdirAll("dir", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(mem, "dir/file.txt", []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	fs := NewFaultFs(mem, FaultRule{
		Op:   FaultOpOpen,
		Path: "dir/file.txt",
		N:    1,
		Err:  errInjected,
	})

	// The unclean path must be normalized before matching the rule.
	if _, err := fs.Open("./dir//sub/../file.txt"); !errors.Is(err, errInjected) {
		t.Fatalf("expected injected open error for unclean path, got %v", err)
	}
	// The rule fired once; the cleaned path now opens normally.
	if _, err := fs.Open("dir/file.txt"); err != nil {
		t.Fatalf("second open should succeed, got %v", err)
	}
}

func TestFaultFsConcurrentCounting(t *testing.T) {
	mem := NewMemMapFs()
	if err := WriteFile(mem, "shared.txt", []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	const goroutines = 8
	const readsEach = 25
	const total = goroutines * readsEach

	var fired int32
	fs := NewFaultFs(mem, FaultRule{
		Op:   FaultOpRead,
		Path: "shared.txt",
		N:    total,
		Err:  errInjected,
		Callback: func(info FaultInfo) {
			atomic.AddInt32(&fired, 1)
		},
	})

	var wg sync.WaitGroup
	errs := make(chan error, total)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f, err := fs.Open("shared.txt")
			if err != nil {
				errs <- err
				return
			}
			defer f.Close()
			buf := make([]byte, 4)
			for i := 0; i < readsEach; i++ {
				if _, err := f.Read(buf); err != nil && !errors.Is(err, errInjected) && err != io.EOF {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := atomic.LoadInt32(&fired); got != 1 {
		t.Fatalf("rule fired %d times, want exactly 1", got)
	}
	if got := fs.CallCount(FaultOpRead, "shared.txt"); got != total {
		t.Fatalf("read count = %d, want %d", got, total)
	}
}

func TestFaultFsCountersIsolatedPerInstance(t *testing.T) {
	mem := NewMemMapFs()
	fs1 := NewFaultFs(mem)
	fs2 := NewFaultFs(mem)

	for i := 0; i < 3; i++ {
		f, err := fs1.Create("iso.txt")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString("x"); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}

	if got := fs1.CallCount(FaultOpWrite, "iso.txt"); got != 3 {
		t.Fatalf("fs1 write count = %d, want 3", got)
	}
	if got := fs2.CallCount(FaultOpWrite, "iso.txt"); got != 0 {
		t.Fatalf("fs2 write count = %d, want 0", got)
	}
}

func TestFaultFsDisabledMatchesUnderlying(t *testing.T) {
	mem := NewMemMapFs()
	fs := NewFaultFs(mem, FaultRule{
		Op:     FaultOpWrite,
		Path:   "file.txt",
		N:      1,
		Err:    errInjected,
		Repeat: true,
	}, FaultRule{
		Op:     FaultOpRename,
		Path:   "file.txt",
		N:      1,
		Err:    errInjected,
		Repeat: true,
	})

	// Sanity check: faults fire while enabled.
	f, err := fs.Create("file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("x"); !errors.Is(err, errInjected) {
		t.Fatalf("expected injected error while enabled, got %v", err)
	}
	f.Close()

	fs.SetEnabled(false)
	if fs.Enabled() {
		t.Fatal("fault injection should be disabled")
	}

	// With injection off, the wrapper behaves like the underlying Fs.
	if err := WriteFile(fs, "file.txt", []byte("payload"), 0o644); err != nil {
		t.Fatalf("write while disabled should succeed, got %v", err)
	}
	if err := fs.Rename("file.txt", "renamed.txt"); err != nil {
		t.Fatalf("rename while disabled should succeed, got %v", err)
	}
	content, err := ReadFile(fs, "renamed.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "payload" {
		t.Fatalf("content = %q, want %q", content, "payload")
	}
	direct, err := ReadFile(mem, "renamed.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(direct) != string(content) {
		t.Fatalf("wrapper and underlying Fs disagree: %q vs %q", content, direct)
	}

	// No calls are counted while disabled.
	if got := fs.CallCount(FaultOpWrite, "file.txt"); got != 1 {
		t.Fatalf("write count = %d, want 1 (only the enabled call)", got)
	}
}

func TestFaultFsCallbackRunsOutsideLock(t *testing.T) {
	mem := NewMemMapFs()
	if err := WriteFile(mem, "cb.txt", []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	var fs *FaultFs
	fs = NewFaultFs(mem, FaultRule{
		Op:   FaultOpOpen,
		Path: "cb.txt",
		N:    1,
		Err:  errInjected,
		Callback: func(info FaultInfo) {
			defer close(done)
			// Calling back into the same FaultFs would deadlock if the
			// callback ran while the internal lock was held.
			if got := fs.CallCount(FaultOpOpen, "cb.txt"); got != 1 {
				t.Errorf("CallCount inside callback = %d, want 1", got)
			}
			if _, err := fs.Open("other.txt"); err == nil {
				t.Error("expected error opening missing file")
			}
		},
	})

	if _, err := fs.Open("cb.txt"); !errors.Is(err, errInjected) {
		t.Fatalf("expected injected open error, got %v", err)
	}
	<-done
}

func TestFaultFsUnderlyingErrorsStillMatch(t *testing.T) {
	mem := NewMemMapFs()
	fs := NewFaultFs(mem)

	_, err := fs.Open("missing.txt")
	if !errors.Is(err, ErrFileNotFound) {
		t.Fatalf("expected ErrFileNotFound, got %v", err)
	}
}

// ExampleFaultFs demonstrates a normal copy and a copy that fails with a
// short write on the second Write call.
func ExampleFaultFs() {
	mem := NewMemMapFs()
	_ = WriteFile(mem, "src.txt", []byte("hello world"), 0o644)

	// Normal copy: no rules, the FaultFs behaves like the underlying Fs.
	normal := NewFaultFs(mem)
	if err := copyChunks(normal, "src.txt", "ok.txt", 4); err != nil {
		fmt.Println("normal copy failed:", err)
		return
	}
	ok, _ := ReadFile(mem, "ok.txt")
	fmt.Printf("normal copy: %q\n", ok)

	// Faulty copy: the second Write stores only a 3-byte prefix and then
	// reports an error, like a full disk would.
	faulty := NewFaultFs(mem, FaultRule{
		Op:          FaultOpWrite,
		Path:        "dst.txt",
		N:           2,
		Err:         errInjected,
		WritePrefix: 3,
	})
	err := copyChunks(faulty, "src.txt", "dst.txt", 4)
	partial, _ := ReadFile(mem, "dst.txt")
	fmt.Printf("faulty copy failed: %v\n", errors.Is(err, errInjected))
	fmt.Printf("partial content: %q\n", partial)

	// Output:
	// normal copy: "hello world"
	// faulty copy failed: true
	// partial content: "hello w"
}
