package afero

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
)

// newTestConsistentFs builds the adapter over an in-memory backend and, for the
// "os" case, a native temp-directory backend. Each case gets its own scratch
// directory on local (native) storage; no Docker or network fixtures are used.
func newTestConsistentFs(t *testing.T, backend string) (Fs, func()) {
	t.Helper()
	switch backend {
	case "mem":
		return NewConsistentWriteFs(NewMemMapFs()), func() {}
	case "os":
		dir := t.TempDir()
		base := NewBasePathFs(NewOsFs(), dir)
		return NewConsistentWriteFs(base), func() {}
	default:
		t.Fatalf("unknown backend %q", backend)
		return nil, nil
	}
}

// readFileAll opens the file through the plain wrapped backend (not the
// adapter) to verify what actually landed in storage.
func readFilePlain(t *testing.T, cw *ConsistentWriteFs, name string) []byte {
	t.Helper()
	f, err := cw.source.Open(name)
	if err != nil {
		t.Fatalf("plain open %s: %v", name, err)
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("plain read %s: %v", name, err)
	}
	return b
}

// TestConsistentWriteSeekHole is the acceptance example: write "abc", seek to
// offset 5 (past the 3-byte end), write "X", and expect the six bytes
// "abc\x00\x00X" — the two-byte hole reads back as zeros. It runs against both
// the in-memory backend and the native small-file (OsFs) backend.
func TestConsistentWriteSeekHole(t *testing.T) {
	for _, backend := range []string{"mem", "os"} {
		t.Run(backend, func(t *testing.T) {
			fs, cleanup := newTestConsistentFs(t, backend)
			defer cleanup()
			cw := fs.(*ConsistentWriteFs)
			const name = "/hole.txt"

			f, err := cw.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
			if err != nil {
				t.Fatalf("OpenFile: %v", err)
			}
			if n, err := f.Write([]byte("abc")); err != nil || n != 3 {
				t.Fatalf("Write abc = %d, %v", n, err)
			}
			if off, err := f.Seek(5, io.SeekStart); err != nil || off != 5 {
				t.Fatalf("Seek(5) = %d, %v", off, err)
			}
			if n, err := f.Write([]byte("X")); err != nil || n != 1 {
				t.Fatalf("Write X = %d, %v", n, err)
			}
			if err := f.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			want := []byte{'a', 'b', 'c', 0, 0, 'X'}
			if got := readFilePlain(t, cw, name); !bytes.Equal(got, want) {
				t.Fatalf("stored bytes = %q, want %q", got, want)
			}

			// Read back through the adapter from the start.
			rf, err := cw.Open(name)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			got, err := io.ReadAll(rf)
			rf.Close()
			if err != nil {
				t.Fatalf("readback: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("readback = %q, want %q (six-byte example: abc+00+X)", got, want)
			}

			fi, err := cw.Stat(name)
			if err != nil {
				t.Fatalf("Stat: %v", err)
			}
			if fi.Size() != 6 {
				t.Fatalf("size = %d, want 6", fi.Size())
			}
		})
	}
}

// TestConsistentWriteAtHole checks the absolute-offset path as well.
func TestConsistentWriteAtHole(t *testing.T) {
	for _, backend := range []string{"mem", "os"} {
		t.Run(backend, func(t *testing.T) {
			fs, cleanup := newTestConsistentFs(t, backend)
			defer cleanup()
			cw := fs.(*ConsistentWriteFs)
			const name = "/at.bin"

			f, err := cw.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
			if err != nil {
				t.Fatalf("OpenFile: %v", err)
			}
			if n, err := f.Write([]byte("abc")); err != nil || n != 3 {
				t.Fatalf("Write abc = %d, %v", n, err)
			}
			if n, err := f.WriteAt([]byte("X"), 5); err != nil || n != 1 {
				t.Fatalf("WriteAt(5,X) = %d, %v", n, err)
			}
			if err := f.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			want := []byte{'a', 'b', 'c', 0, 0, 'X'}
			if got := readFilePlain(t, cw, name); !bytes.Equal(got, want) {
				t.Fatalf("stored bytes = %q, want %q", got, want)
			}
		})
	}
}

// failingSecondStepFiller is a chunked filler whose first chunk lands fully
// and whose second chunk fails (after landing the bytes it managed). It is a
// faithful version of the production filler with an injected mid-fill failure,
// used to prove honest progress reporting.
type failingSecondStepFiller struct {
	step   int64 // bytes materialized per step
	inject error
}

func (fl *failingSecondStepFiller) fill(f File, start, n int64) (int64, error) {
	var written int64
	step := fl.step
	if step <= 0 {
		step = 1
	}
	for written < n {
		todo := n - written
		if todo > step {
			todo = step
		}
		if written > 0 {
			// We have been asked for a second step: write one zero byte of it,
			// then report the injected failure with real progress.
			m, _ := f.WriteAt([]byte{0}, start+written)
			return written + int64(m), fl.inject
		}
		m, err := f.WriteAt(zeroFillBuf[:todo], start+written)
		written += int64(m)
		if err != nil {
			return written, err
		}
		if int64(m) < todo {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

// TestConsistentWriteFillFailureProgress is the second acceptance case: when
// filling the hole fails on the second step after partial progress, the Write
// must report 0 payload bytes and a *FillError whose Filled count is exactly
// the bytes actually materialized — never an atomic "success".
func TestConsistentWriteFillFailureProgress(t *testing.T) {
	for _, backend := range []string{"mem", "os"} {
		t.Run(backend, func(t *testing.T) {
			fs, cleanup := newTestConsistentFs(t, backend)
			defer cleanup()
			cw := fs.(*ConsistentWriteFs)
			const name = "/failfill.txt"

			h, err := cw.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
			if err != nil {
				t.Fatalf("OpenFile: %v", err)
			}
			cf := h.(*consistentWriteFile)
			defer cf.Close()
			fl := &failingSecondStepFiller{inject: errors.New("injected fill failure")}
			cf.fill = fl.fill

			if _, err := cf.Write([]byte("ab")); err != nil {
				t.Fatalf("seed Write: %v", err)
			}
			// Seek to offset 6: the hole spans offsets 2..5 (4 bytes). The
			// injected filler lays one zero in step one and one zero in step
			// two before failing.
			if _, err := cf.Seek(6, io.SeekStart); err != nil {
				t.Fatalf("Seek: %v", err)
			}
			n, err := cf.Write([]byte("Z"))
			if n != 0 {
				t.Fatalf("payload Write returned n=%d, want 0 (payload must not land)", n)
			}
			var fe *FillError
			if !errors.As(err, &fe) {
				t.Fatalf("want *FillError, got %T: %v", err, err)
			}
			if fe.Filled != 2 {
				t.Fatalf("Filled = %d, want 2 (real progress across two steps)", fe.Filled)
			}
			if fe.Op != "write" || fe.Path != cf.Name() {
				t.Fatalf("FillError meta = %q %q, want path %q", fe.Op, fe.Path, cf.Name())
			}
			if !errors.Is(fe, fl.inject) {
				t.Fatalf("FillError should wrap injected error, got %v", fe.Err)
			}

			// Exactly the seed prefix plus the two real zero bytes must exist;
			// the payload "Z" must not have been written.
			got := readFilePlain(t, cw, name)
			want := []byte{'a', 'b', 0, 0}
			if !bytes.Equal(got, want) {
				t.Fatalf("stored after failed fill = %q, want %q", got, want)
			}
		})
	}
}

// TestConsistentWriteTruncateGrowKeepsPrefix verifies that an extending
// Truncate is itself explicit and consistent: it appends real zero bytes and
// this growth does not change the original prefix.
func TestConsistentWriteTruncateGrowKeepsPrefix(t *testing.T) {
	for _, backend := range []string{"mem", "os"} {
		t.Run(backend, func(t *testing.T) {
			fs, cleanup := newTestConsistentFs(t, backend)
			defer cleanup()
			cw := fs.(*ConsistentWriteFs)
			const name = "/grow.txt"

			f, err := cw.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
			if err != nil {
				t.Fatalf("OpenFile: %v", err)
			}
			if _, err := f.Write([]byte("abc")); err != nil {
				t.Fatalf("seed Write: %v", err)
			}
			if err := f.Truncate(6); err != nil {
				t.Fatalf("Truncate grow: %v", err)
			}
			if err := f.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			want := []byte{'a', 'b', 'c', 0, 0, 0}
			if got := readFilePlain(t, cw, name); !bytes.Equal(got, want) {
				t.Fatalf("after grow = %q, want %q", got, want)
			}

			// Shrinking must also behave normally and preserve the kept prefix.
			g, err := cw.OpenFile(name, os.O_RDWR, 0o644)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			if err := g.Truncate(2); err != nil {
				t.Fatalf("Truncate shrink: %v", err)
			}
			if err := g.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if got := readFilePlain(t, cw, name); !bytes.Equal(got, []byte{'a', 'b'}) {
				t.Fatalf("after shrink = %q, want %q", got, []byte{'a', 'b'})
			}
		})
	}
}

// TestConsistentWriteTruncateGrowFillFailure verifies honest progress on a
// failing extending truncation.
func TestConsistentWriteTruncateGrowFillFailure(t *testing.T) {
	fs, cleanup := newTestConsistentFs(t, "mem")
	defer cleanup()
	cw := fs.(*ConsistentWriteFs)

	h, err := cw.OpenFile("/gt.bin", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	cf := h.(*consistentWriteFile)
	if _, err := cf.Write([]byte("ab")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	injected := errors.New("boom")
	fl := &failingSecondStepFiller{inject: injected}
	cf.fill = fl.fill

	err = cf.Truncate(8) // grow by 6; step one writes 1, step two writes 1 then fails
	var fe *FillError
	if !errors.As(err, &fe) {
		t.Fatalf("want *FillError, got %T: %v", err, err)
	}
	if fe.Op != "truncate" || fe.Filled != 2 {
		t.Fatalf("FillError = op %q filled %d, want truncate/2", fe.Op, fe.Filled)
	}
	if !errors.Is(fe, injected) {
		t.Fatalf("wrap: %v", fe.Err)
	}
	if err := cf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := readFilePlain(t, cw, "/gt.bin"); !bytes.Equal(got, []byte{'a', 'b', 0, 0}) {
		t.Fatalf("stored = %q, want ab+2 zeros", got)
	}
}

// shortWriteFs wraps an Fs and forces write handles to short-write, so the
// adapter's honest-length path can be exercised without a flaky real disk.
type shortWriteFs struct {
	Fs
	// zerosOnly restricts short writes to all-zero (hole-filling) buffers, so
	// payload writes can still complete fully.
	zerosOnly bool
}

func (s shortWriteFs) Create(name string) (File, error) {
	f, err := s.Fs.Create(name)
	if err != nil {
		return f, err
	}
	return &shortWriteFile{File: f, zerosOnly: s.zerosOnly}, nil
}

func (s shortWriteFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f, err := s.Fs.OpenFile(name, flag, perm)
	if err != nil {
		return f, err
	}
	if flag&(os.O_WRONLY|os.O_RDWR) == 0 {
		return f, nil
	}
	return &shortWriteFile{File: f, zerosOnly: s.zerosOnly}, nil
}

// shortWriteFile simulates a backend that accepts only the first len(p)-1
// bytes of qualifying buffers, returning that short count without an error.
type shortWriteFile struct {
	File
	zerosOnly bool
}

func (w *shortWriteFile) shouldShort(p []byte) bool {
	if len(p) <= 1 {
		return false
	}
	if !w.zerosOnly {
		return true
	}
	for _, b := range p {
		if b != 0 {
			return false
		}
	}
	return true
}

func (w *shortWriteFile) Write(p []byte) (int, error) {
	if w.shouldShort(p) {
		p = p[:len(p)-1]
	}
	return w.File.Write(p)
}

func (w *shortWriteFile) WriteAt(p []byte, off int64) (int, error) {
	if w.shouldShort(p) {
		p = p[:len(p)-1]
	}
	return w.File.WriteAt(p, off)
}

// TestConsistentWriteShortPayloadWrite ensures a short payload write returns
// the real completed length plus io.ErrShortWrite rather than claiming full
// atomic success.
func TestConsistentWriteShortPayloadWrite(t *testing.T) {
	base := shortWriteFs{Fs: NewMemMapFs()}
	cw := NewConsistentWriteFs(base)

	f, err := cw.OpenFile("/short.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	n, err := f.Write([]byte("abcd"))
	if err != io.ErrShortWrite {
		t.Fatalf("err = %v, want io.ErrShortWrite", err)
	}
	if n != 3 {
		t.Fatalf("short Write n = %d, want 3 (real completed length)", n)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := readFilePlain(t, cw, "/short.txt"); !bytes.Equal(got, []byte{'a', 'b', 'c'}) {
		t.Fatalf("stored = %q, want abc", got)
	}
}

// TestConsistentWriteShortFill ensures a short write while materializing the
// hole is reported as fill progress, not success.
func TestConsistentWriteShortFill(t *testing.T) {
	// Backend short-writes only on large zero-filling buffers; seed payload is
	// 3 bytes (<4), holes are larger.
	base := shortWriteFs{Fs: NewMemMapFs(), zerosOnly: true}
	cw := NewConsistentWriteFs(base)

	f, err := cw.OpenFile("/shortfill.bin", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte("abc")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := f.Seek(8, io.SeekStart); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	n, err := f.Write([]byte("Z"))
	if n != 0 {
		t.Fatalf("payload n = %d, want 0", n)
	}
	var fe *FillError
	if !errors.As(err, &fe) {
		t.Fatalf("want *FillError, got %T: %v", err, err)
	}
	// The 5-byte hole is short-written by one byte on the first attempt.
	if fe.Filled != 4 {
		t.Fatalf("Filled = %d, want 4", fe.Filled)
	}
	if !errors.Is(fe, io.ErrShortWrite) {
		t.Fatalf("want io.ErrShortWrite wrapped, got %v", fe.Err)
	}
}

// TestConsistentWriteOffsetOverflow checks that offset arithmetic is validated
// before touching the backend.
func TestConsistentWriteOffsetOverflow(t *testing.T) {
	cw := NewConsistentWriteFs(NewMemMapFs())
	f, err := cw.OpenFile("/ovf.bin", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()

	if _, err := f.Seek(-1, io.SeekStart); err == nil {
		t.Fatal("Seek(-1) should fail")
	}
	if _, err := f.Seek(math.MaxInt64, io.SeekStart); err != nil {
		t.Fatalf("Seek(MaxInt64) should position: %v", err)
	}
	if _, err := f.Seek(1, io.SeekCurrent); !errors.Is(err, errOffsetOverflow) {
		t.Fatalf("overflow seek err = %v, want errOffsetOverflow", err)
	}
	// A payload that would overflow from the max position is rejected.
	if n, err := f.Write([]byte("x")); n != 0 || !errors.Is(err, errOffsetOverflow) {
		t.Fatalf("overflow Write = %d, %v", n, err)
	}
	if n, err := f.WriteAt([]byte("x"), -1); n != 0 || !errors.Is(err, errNegativeOffset) {
		t.Fatalf("negative WriteAt = %d, %v", n, err)
	}
	if n, err := f.WriteAt([]byte("x"), math.MaxInt64); n != 0 || !errors.Is(err, errOffsetOverflow) {
		t.Fatalf("overflow WriteAt = %d, %v", n, err)
	}
	if err := f.Truncate(-1); !errors.Is(err, errNegativeOffset) {
		t.Fatalf("negative Truncate = %v", err)
	}
}

// TestConsistentWriteBorrowedHandleNotClosed verifies the ownership rule: a
// handle built via NewConsistentWriteFile borrows the underlying File and must
// not close it, leaving it usable by its owner.
func TestConsistentWriteBorrowedHandleNotClosed(t *testing.T) {
	base := NewMemMapFs()
	owner, err := base.Create("/borrow.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	borrowed := NewConsistentWriteFile(owner)
	if _, err := borrowed.Write([]byte("abc")); err != nil {
		t.Fatalf("write via adapter: %v", err)
	}
	if err := borrowed.Close(); err != nil {
		t.Fatalf("borrowed Close: %v", err)
	}
	// Owner's handle must still be usable (adapter did not close it).
	if _, err := owner.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("owner Seek after borrowed close: %v", err)
	}
	got := make([]byte, 3)
	if n, err := owner.Read(got); err != nil || n != 3 {
		t.Fatalf("owner Read = %d, %v", n, err)
	}
	if string(got) != "abc" {
		t.Fatalf("owner read = %q", got)
	}
	if err := owner.Close(); err != nil {
		t.Fatalf("real owner Close: %v", err)
	}
}

// TestConsistentWriteBorrowedOsHandleNotClosed repeats the ownership check on
// a real os.File: the adapter must not close a handle it did not create.
func TestConsistentWriteBorrowedOsHandleNotClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "borrow-os.txt")
	owner, err := NewOsFs().Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	borrowed := NewConsistentWriteFile(owner)
	if _, err := borrowed.Write([]byte("abc")); err != nil {
		t.Fatalf("write via adapter: %v", err)
	}
	if err := borrowed.Close(); err != nil {
		t.Fatalf("borrowed Close: %v", err)
	}
	if _, err := owner.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("owner still usable after borrowed close: %v", err)
	}
	got, err := io.ReadAll(owner)
	if err != nil {
		t.Fatalf("owner ReadAll: %v", err)
	}
	if string(got) != "abc" {
		t.Fatalf("owner read = %q", got)
	}
	if err := owner.Close(); err != nil {
		t.Fatalf("real owner Close: %v", err)
	}
}

// TestConsistentWriteOwnedHandleClosesBackend verifies handles produced by the
// Fs own and close the underlying backend handle.
func TestConsistentWriteOwnedHandleClosesBackend(t *testing.T) {
	for _, backend := range []string{"mem", "os"} {
		t.Run(backend, func(t *testing.T) {
			fs, cleanup := newTestConsistentFs(t, backend)
			defer cleanup()
			cw := fs.(*ConsistentWriteFs)

			w, err := cw.OpenFile("/owned.txt", os.O_RDWR|os.O_CREATE, 0o644)
			if err != nil {
				t.Fatalf("OpenFile: %v", err)
			}
			if _, err := w.Write([]byte("data")); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close owned handle: %v", err)
			}
			// The backend handle must really be closed: a further use fails.
			if _, err := w.Read(make([]byte, 1)); err == nil {
				t.Fatal("Read after Close unexpectedly succeeded")
			}

			if got := readFilePlain(t, cw, "/owned.txt"); string(got) != "data" {
				t.Fatalf("stored = %q, want data", got)
			}
		})
	}
}

// TestConsistentWriteSequentialCursor checks interleaved writes and reads keep
// a coherent sequential cursor, including a write that fills a hole.
func TestConsistentWriteSequentialCursor(t *testing.T) {
	for _, backend := range []string{"mem", "os"} {
		t.Run(backend, func(t *testing.T) {
			fs, cleanup := newTestConsistentFs(t, backend)
			defer cleanup()
			cw := fs.(*ConsistentWriteFs)

			f, err := cw.OpenFile("/seq.bin", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
			if err != nil {
				t.Fatalf("OpenFile: %v", err)
			}
			defer f.Close()
			if _, err := f.Write([]byte("ab")); err != nil {
				t.Fatalf("Write ab: %v", err)
			}
			if off, err := f.Seek(4, io.SeekStart); err != nil || off != 4 {
				t.Fatalf("Seek = %d, %v", off, err)
			}
			if _, err := f.Write([]byte("YZ")); err != nil {
				t.Fatalf("Write past end: %v", err)
			}
			// Back at the start, sequential read returns the six bytes.
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				t.Fatalf("rewind: %v", err)
			}
			got, err := io.ReadAll(f)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			want := []byte{'a', 'b', 0, 0, 'Y', 'Z'}
			if !bytes.Equal(got, want) {
				t.Fatalf("sequential read = %q, want %q", got, want)
			}
		})
	}
}

// TestConsistentWriteNameAndPassThrough sanity-checks metadata passthrough and
// the adapter name.
func TestConsistentWriteNameAndPassThrough(t *testing.T) {
	cw := NewConsistentWriteFs(NewMemMapFs())
	if cw.Name() != "ConsistentWriteFs(MemMapFS)" {
		t.Fatalf("Name = %q", cw.Name())
	}
	f, err := cw.Create("/meta.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if f.Name() != filepath.Clean("/meta.txt") {
		t.Fatalf("file Name = %q", f.Name())
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	fi, err := f.Stat()
	if err != nil || fi.Size() != 0 {
		t.Fatalf("Stat: %v size=%d", err, fi.Size())
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// ExampleConsistentWriteFs demonstrates the six-byte acceptance case on the
// in-memory backend: write "abc", seek to offset 5 past the end, write "X",
// and read back "abc" + two zero bytes + "X".
func ExampleConsistentWriteFs() {
	fs := NewConsistentWriteFs(NewMemMapFs())

	f, _ := fs.OpenFile("/example.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	f.Write([]byte("abc"))
	f.Seek(5, io.SeekStart) // hole at offsets 3 and 4
	f.Write([]byte("X"))
	f.Close()

	r, _ := fs.Open("/example.txt")
	b, _ := io.ReadAll(r)
	r.Close()
	fmt.Printf("%q len=%d\n", string(b), len(b))
	// Output: "abc\x00\x00X" len=6
}
