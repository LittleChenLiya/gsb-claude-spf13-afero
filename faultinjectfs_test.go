package afero

import (
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

var (
	errFaultDiskFull = errors.New("simulated disk full")
	errFaultRename   = errors.New("simulated rename failure")
	errFaultClose    = errors.New("simulated close failure")
	errFaultOpen     = errors.New("simulated open failure")
	errFaultRead     = errors.New("simulated read failure")
)

// copyWithBuffer copies from src to dst using a fixed-size buffer so the
// sequence of Write calls is deterministic and easy to target with rules.
func copyWithBuffer(dst File, src File, size int) error {
	buf := make([]byte, size)
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[:nr])
			if ew != nil {
				return ew
			}
			if nw != nr {
				return io.ErrShortWrite
			}
		}
		if er == io.EOF {
			return nil
		}
		if er != nil {
			return er
		}
	}
}

func readFileString(t *testing.T, fs Fs, name string) string {
	t.Helper()
	f, err := fs.Open(name)
	if err != nil {
		t.Fatalf("open %q: %v", name, err)
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read %q: %v", name, err)
	}
	return string(b)
}

// Acceptance: copying a file on MemMapFs, the second Write writes a 3 byte
// prefix and then fails; the bytes that really landed match that prefix.
func TestFaultInjectingFsShortWritePrefix(t *testing.T) {
	base := NewMemMapFs()
	const content = "hello world"
	if err := WriteFile(base, "src", []byte(content), 0o644); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	fs := NewFaultInjectingFs(base, FaultRule{
		Op:          InjectWrite,
		Path:        "dst",
		N:           2,
		WritePrefix: 3,
		Err:         errFaultDiskFull,
	})

	src, err := fs.Open("src")
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	dst, err := fs.Create("dst")
	if err != nil {
		t.Fatalf("create dst: %v", err)
	}

	err = copyWithBuffer(dst, src, 4)
	if !errors.Is(err, errFaultDiskFull) {
		t.Fatalf("expected injected error, got %v", err)
	}

	// First Write landed "hell", the second Write landed its 3 byte prefix
	// "o w" before failing.
	got := readFileString(t, base, "dst")
	want := "hello w"
	if got != want {
		t.Fatalf("partial write content = %q, want %q", got, want)
	}
	if got != content[:len(got)] {
		t.Fatalf("partial content %q is not the prefix of the source", got)
	}
	if n := fs.Calls(InjectWrite, "dst"); n != 2 {
		t.Fatalf("write calls = %d, want 2", n)
	}
	if err := dst.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestFaultInjectingFsOpenFailure(t *testing.T) {
	base := NewMemMapFs()
	if err := WriteFile(base, "f", []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	fs := NewFaultInjectingFs(base, FaultRule{
		Op:     InjectOpen,
		Path:   "f",
		N:      1,
		Repeat: true,
		Err:    errFaultOpen,
	})

	if _, err := fs.Open("f"); !errors.Is(err, errFaultOpen) {
		t.Fatalf("Open error = %v", err)
	}
	if _, err := fs.OpenFile("f", os.O_RDONLY, 0); !errors.Is(err, errFaultOpen) {
		t.Fatalf("OpenFile error = %v", err)
	}
	if _, err := fs.Create("f"); !errors.Is(err, errFaultOpen) {
		t.Fatalf("Create error = %v", err)
	}
}

func TestFaultInjectingFsReadFailure(t *testing.T) {
	base := NewMemMapFs()
	if err := WriteFile(base, "f", []byte("abc"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	fs := NewFaultInjectingFs(base, FaultRule{
		Op:   InjectRead,
		Path: "f",
		N:    1,
		Err:  errFaultRead,
	})

	f, err := fs.Open("f")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	n, err := f.Read(make([]byte, 3))
	if n != 0 || !errors.Is(err, errFaultRead) {
		t.Fatalf("Read = (%d, %v), want (0, %v)", n, err, errFaultRead)
	}
}

func TestFaultInjectingFsRenameFailure(t *testing.T) {
	base := NewMemMapFs()
	if err := WriteFile(base, "old", []byte("data"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	fs := NewFaultInjectingFs(base, FaultRule{
		Op:   InjectRename,
		Path: "old",
		N:    1,
		Err:  errFaultRename,
	})

	if err := fs.Rename("old", "new"); !errors.Is(err, errFaultRename) {
		t.Fatalf("Rename error = %v", err)
	}
	if _, err := fs.Stat("old"); err != nil {
		t.Fatalf("old name should still exist: %v", err)
	}
	if _, err := fs.Stat("new"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("new name should not exist, got %v", err)
	}
	if n := fs.Calls(InjectRename, "old"); n != 1 {
		t.Fatalf("rename calls = %d, want 1", n)
	}
}

func TestFaultInjectingFsCloseFailure(t *testing.T) {
	base := NewMemMapFs()
	fs := NewFaultInjectingFs(base, FaultRule{
		Op:   InjectClose,
		Path: "f",
		N:    1,
		Err:  errFaultClose,
	})

	f, err := fs.Create("f")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := f.Close(); !errors.Is(err, errFaultClose) {
		t.Fatalf("Close error = %v, want %v", err, errFaultClose)
	}
	// The underlying handle really was closed despite the injected error.
	if _, err := f.Write([]byte("x")); err == nil || err.Error() != ErrFileClosed.Error() {
		t.Fatalf("write after close error = %v, want %v", err, ErrFileClosed)
	}
}

func TestFaultInjectingFsRuleFiresOnceAndRepeat(t *testing.T) {
	fired := 0
	fs := NewFaultInjectingFs(NewMemMapFs(), FaultRule{
		Op:      InjectWrite,
		Path:    "once",
		N:       2,
		Repeat:  false,
		Err:     errFaultDiskFull,
		OnFired: func(FaultRule) { fired++ },
	})

	f, err := fs.Create("once")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()

	// 1st call: before N, succeeds. 2nd call: fires once. 3rd call: passes.
	if _, err := f.Write([]byte("a")); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if _, err := f.Write([]byte("b")); !errors.Is(err, errFaultDiskFull) {
		t.Fatalf("second write error = %v", err)
	}
	if _, err := f.Write([]byte("c")); err != nil {
		t.Fatalf("third write should pass, got %v", err)
	}
	if fired != 1 {
		t.Fatalf("rule fired %d times, want 1", fired)
	}

	fs.SetRules(FaultRule{
		Op:     InjectWrite,
		Path:   "rep",
		N:      1,
		Repeat: true,
		Err:    errFaultDiskFull,
	})
	g, err := fs.Create("rep")
	if err != nil {
		t.Fatalf("create rep: %v", err)
	}
	defer g.Close()
	for i := 0; i < 3; i++ {
		if _, err := g.Write([]byte("x")); !errors.Is(err, errFaultDiskFull) {
			t.Fatalf("repeating write %d error = %v", i, err)
		}
	}
}

func TestFaultInjectingFsFirstMatchingRuleWins(t *testing.T) {
	errSecond := errors.New("never reached")
	fs := NewFaultInjectingFs(NewMemMapFs(),
		FaultRule{Op: InjectWrite, Path: "f", N: 1, Err: errFaultDiskFull},
		FaultRule{Op: InjectWrite, Path: "f", N: 1, Err: errSecond},
	)
	f, err := fs.Create("f")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()
	if _, err := f.Write(nil); !errors.Is(err, errFaultDiskFull) || errors.Is(err, errSecond) {
		t.Fatalf("expected first rule only, got %v", err)
	}
}

func TestFaultInjectingFsPathCleaning(t *testing.T) {
	fs := NewFaultInjectingFs(NewMemMapFs(), FaultRule{
		Op:   InjectWrite,
		Path: "dir/../f",
		N:    1,
		Err:  errFaultDiskFull,
	})
	f, err := fs.Create("./f")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()
	if _, err := f.Write([]byte("x")); !errors.Is(err, errFaultDiskFull) {
		t.Fatalf("path cleaning match error = %v", err)
	}
}

// Files opened through the wrapper share the wrapper's counters even when
// multiple handles to the same path are in use.
func TestFaultInjectingFsFilesShareWrapperCounters(t *testing.T) {
	fs := NewFaultInjectingFs(NewMemMapFs(), FaultRule{
		Op:   InjectWrite,
		Path: "f",
		N:    2,
		Err:  errFaultDiskFull,
	})
	a, err := fs.Create("f")
	if err != nil {
		t.Fatalf("create a: %v", err)
	}
	b, err := fs.OpenFile("f", os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open b: %v", err)
	}
	defer a.Close()
	defer b.Close()

	if _, err := a.Write([]byte("1")); err != nil {
		t.Fatalf("write via a: %v", err)
	}
	if _, err := b.Write([]byte("2")); !errors.Is(err, errFaultDiskFull) {
		t.Fatalf("write via b should hit the shared counter, got %v", err)
	}
}

func TestFaultInjectingFsConcurrentCounting(t *testing.T) {
	const goroutines = 100
	const fireAt = 50
	fs := NewFaultInjectingFs(NewMemMapFs(), FaultRule{
		Op:   InjectWrite,
		Path: "f",
		N:    fireAt,
		Err:  errFaultDiskFull,
	})

	f, err := fs.Create("f")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures int
	start := make(chan struct{})
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			if _, err := f.Write([]byte("x")); err != nil {
				mu.Lock()
				failures++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if failures != 1 {
		t.Fatalf("failures = %d, want exactly 1", failures)
	}
	if n := fs.Calls(InjectWrite, "f"); n != goroutines {
		t.Fatalf("write calls = %d, want %d", n, goroutines)
	}
}

func TestFaultInjectingFsCountersAreInstanceScoped(t *testing.T) {
	rules := []FaultRule{{Op: InjectWrite, Path: "f", N: 2, Err: errFaultDiskFull}}
	fsA := NewFaultInjectingFs(NewMemMapFs(), rules...)
	fsB := NewFaultInjectingFs(NewMemMapFs(), rules...)

	fa, err := fsA.Create("f")
	if err != nil {
		t.Fatalf("create a: %v", err)
	}
	fb, err := fsB.Create("f")
	if err != nil {
		t.Fatalf("create b: %v", err)
	}

	// First write succeeds on each wrapper; the second write fails on each
	// wrapper because counters are isolated per instance.
	if _, err := fa.Write([]byte("x")); err != nil {
		t.Fatalf("first write a: %v", err)
	}
	if _, err := fb.Write([]byte("x")); err != nil {
		t.Fatalf("first write b: %v", err)
	}
	if _, err := fa.Write([]byte("x")); !errors.Is(err, errFaultDiskFull) {
		t.Fatalf("second write a error = %v, want %v", err, errFaultDiskFull)
	}
	if _, err := fb.Write([]byte("x")); !errors.Is(err, errFaultDiskFull) {
		t.Fatalf("second write b error = %v, want %v", err, errFaultDiskFull)
	}
	if n := fsA.Calls(InjectWrite, "f"); n != 2 {
		t.Fatalf("fsA calls = %d, want 2", n)
	}
	if n := fsB.Calls(InjectWrite, "f"); n != 2 {
		t.Fatalf("fsB calls = %d, want 2", n)
	}
	if err := errors.Join(fa.Close(), fb.Close()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// OnFired must run without the wrapper lock held: a callback that calls back
// into the wrapper must not deadlock.
func TestFaultInjectingFsCallbackRunsUnlocked(t *testing.T) {
	base := NewMemMapFs()
	if err := WriteFile(base, "f", []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var fs *FaultInjectingFs
	fs = NewFaultInjectingFs(base, FaultRule{
		Op:   InjectRead,
		Path: "f",
		N:    1,
		Err:  errFaultRead,
		OnFired: func(FaultRule) {
			fs.Calls(InjectRead, "f")
			fs.Enabled()
		},
	})
	f, err := fs.Open("f")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	done := make(chan struct{})
	go func() {
		_, _ = f.Read(make([]byte, 1))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("OnFired appears to run while the wrapper lock is held")
	}
}

// With fault injection disabled the wrapper behaves exactly like the
// underlying Fs.
func TestFaultInjectingFsDisabledMatchesSource(t *testing.T) {
	base := NewMemMapFs()
	fs := NewFaultInjectingFs(base,
		FaultRule{Op: InjectOpen, Path: "f", N: 1, Err: errFaultOpen},
		FaultRule{Op: InjectOpen, Path: "g", N: 1, Err: errFaultOpen},
		FaultRule{Op: InjectWrite, Path: "f", N: 1, Err: errFaultDiskFull},
		FaultRule{Op: InjectRead, Path: "f", N: 1, Err: errFaultRead},
		FaultRule{Op: InjectClose, Path: "f", N: 1, Err: errFaultClose},
		FaultRule{Op: InjectRename, Path: "f", N: 1, Err: errFaultRename},
	)
	fs.SetEnabled(false)

	f, err := fs.Create("f")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := f.Write([]byte("payload")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := fs.Rename("f", "g"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if got := readFileString(t, fs, "g"); got != "payload" {
		t.Fatalf("content = %q, want %q", got, "payload")
	}
	// Nothing is counted while disabled.
	if n := fs.Calls(InjectWrite, "f"); n != 0 {
		t.Fatalf("calls while disabled = %d, want 0", n)
	}

	// Re-enabling restores injection.
	fs.SetEnabled(true)
	if _, err := fs.Open("g"); !errors.Is(err, errFaultOpen) {
		t.Fatalf("expected fault after re-enable, got %v", err)
	}
}
