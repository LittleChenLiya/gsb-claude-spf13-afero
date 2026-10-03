// Copyright © 2026 Afero authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package afero

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Fault-injection fixture
// ---------------------------------------------------------------------------

// faultyFs wraps an Fs and returns handles whose writes can be made short or
// can fail. It is used to exercise the reconcile-after-write path.
type faultyFs struct {
	Fs
	mu           sync.Mutex
	shortWriteAt int64 // max bytes actually written per Write/WriteAt; 0 = unlimited
	writeErr     error // error returned after a (possibly short) write
	removeErr    error // error returned by Remove
}

func (f *faultyFs) setShortWrite(n int64) {
	f.mu.Lock()
	f.shortWriteAt = n
	f.mu.Unlock()
}

func (f *faultyFs) setWriteErr(err error) {
	f.mu.Lock()
	f.writeErr = err
	f.mu.Unlock()
}

func (f *faultyFs) setRemoveErr(err error) {
	f.mu.Lock()
	f.removeErr = err
	f.mu.Unlock()
}

type faultyFile struct {
	File
	fs *faultyFs
}

func (f *faultyFile) limit(p []byte) []byte {
	f.fs.mu.Lock()
	limit := f.fs.shortWriteAt
	f.fs.mu.Unlock()
	if limit > 0 && int64(len(p)) > limit {
		return p[:limit]
	}
	return p
}

func (f *faultyFile) writeError() error {
	f.fs.mu.Lock()
	err := f.fs.writeErr
	f.fs.mu.Unlock()
	return err
}

func (f *faultyFile) Write(p []byte) (int, error) {
	q := f.limit(p)
	n, err := f.File.Write(q)
	if err == nil {
		err = f.writeError()
	}
	return n, err
}

func (f *faultyFile) WriteAt(p []byte, off int64) (int, error) {
	q := f.limit(p)
	n, err := f.File.WriteAt(q, off)
	if err == nil {
		err = f.writeError()
	}
	return n, err
}

func (f *faultyFs) Create(name string) (File, error) {
	sf, err := f.Fs.Create(name)
	if err != nil {
		return nil, err
	}
	return &faultyFile{File: sf, fs: f}, nil
}

func (f *faultyFs) Open(name string) (File, error) {
	sf, err := f.Fs.Open(name)
	if err != nil {
		return nil, err
	}
	return &faultyFile{File: sf, fs: f}, nil
}

func (f *faultyFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	sf, err := f.Fs.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &faultyFile{File: sf, fs: f}, nil
}

func (f *faultyFs) Remove(name string) error {
	f.mu.Lock()
	err := f.removeErr
	f.mu.Unlock()
	if err != nil {
		return err
	}
	return f.Fs.Remove(name)
}

// ---------------------------------------------------------------------------
// Symlink fixture
// ---------------------------------------------------------------------------

type symlinkInfo struct{ name string }

func (s symlinkInfo) Name() string       { return s.name }
func (s symlinkInfo) Size() int64        { return 0 }
func (s symlinkInfo) Mode() os.FileMode  { return os.ModeSymlink | 0o777 }
func (s symlinkInfo) ModTime() time.Time { return time.Time{} }
func (s symlinkInfo) IsDir() bool        { return false }
func (s symlinkInfo) Sys() interface{}   { return nil }

// symlinkFs reports a configurable set of names as symbolic links through
// LstatIfPossible.
type symlinkFs struct {
	Fs
	links map[string]bool
}

func (s *symlinkFs) LstatIfPossible(name string) (os.FileInfo, bool, error) {
	if s.links[name] {
		return symlinkInfo{name: filepath.Base(name)}, true, nil
	}
	fi, err := s.Fs.Stat(name)
	return fi, false, err
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newQuotaMem(t *testing.T, quota int64) (*QuotaFs, Fs) {
	t.Helper()
	src := NewMemMapFs()
	q, err := NewQuotaFs(src, quota)
	if err != nil {
		t.Fatalf("NewQuotaFs: %v", err)
	}
	return q, src
}

func fileSize(t *testing.T, f File) int64 {
	t.Helper()
	fi, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	return fi.Size()
}

func fsSize(t *testing.T, fs Fs, name string) int64 {
	t.Helper()
	fi, err := fs.Stat(name)
	if err != nil {
		t.Fatalf("Stat(%s): %v", name, err)
	}
	return fi.Size()
}

// ---------------------------------------------------------------------------
// acceptance tests
// ---------------------------------------------------------------------------

// Total quota 8: overwrites by two handles do not charge again, and a write of
// 2 bytes at offset 7 is rejected first.
func TestQuotaEightByteLimit(t *testing.T) {
	q, _ := newQuotaMem(t, 8)

	h1, err := q.Create("/f")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := h1.Write([]byte("abcd")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := q.Used(); got != 4 {
		t.Fatalf("after 4 bytes, used = %d, want 4", got)
	}

	// A second handle overwrites the same bytes: no extra charge.
	h2, err := q.OpenFile("/f", os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := h2.Write([]byte("efgh")); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if got := q.Used(); got != 4 {
		t.Fatalf("after overwrite, used = %d, want 4", got)
	}
	if got := string(mustRead(t, h2)); got != "efgh" {
		t.Fatalf("content = %q, want efgh", got)
	}

	// Writing 2 bytes at offset 7 would make the file 9 bytes long: rejected
	// before touching the underlying file.
	if _, err := h2.WriteAt([]byte("ij"), 7); err != ErrQuotaExceeded {
		t.Fatalf("WriteAt@7: got %v, want ErrQuotaExceeded", err)
	}
	if got := q.Used(); got != 4 {
		t.Fatalf("after rejected write, used = %d, want 4", got)
	}
	if got := fileSize(t, h2); got != 4 {
		t.Fatalf("file size = %d, want 4 (underlying file untouched)", got)
	}
}

// A short write only charges for the bytes that actually landed.
func TestQuotaShortWrite(t *testing.T) {
	src := NewMemMapFs()
	ff := &faultyFs{Fs: src}
	ff.setShortWrite(1)
	q, err := NewQuotaFs(ff, 8)
	if err != nil {
		t.Fatalf("NewQuotaFs: %v", err)
	}

	h, err := q.Create("/f")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Ask for 2 bytes at offset 0, only 1 lands.
	if n, err := h.Write([]byte("ab")); err != nil || n != 1 {
		t.Fatalf("Write: n=%d err=%v, want 1,nil", n, err)
	}
	if got := q.Used(); got != 1 {
		t.Fatalf("used = %d, want 1 (actual growth only)", got)
	}
	if got := fileSize(t, h); got != 1 {
		t.Fatalf("file size = %d, want 1", got)
	}

	// Ask for 2 more bytes at offset 1, only 1 lands.
	if n, err := h.Write([]byte("cd")); err != nil || n != 1 {
		t.Fatalf("Write: n=%d err=%v, want 1,nil", n, err)
	}
	if got := q.Used(); got != 2 {
		t.Fatalf("used = %d, want 2", got)
	}
	if got := fileSize(t, h); got != 2 {
		t.Fatalf("file size = %d, want 2", got)
	}
}

// A failed write only charges for the bytes that actually landed.
func TestQuotaFailedWrite(t *testing.T) {
	src := NewMemMapFs()
	ff := &faultyFs{Fs: src}
	ff.setShortWrite(1)
	ff.setWriteErr(errors.New("boom"))
	q, err := NewQuotaFs(ff, 8)
	if err != nil {
		t.Fatalf("NewQuotaFs: %v", err)
	}

	h, err := q.Create("/f")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := h.Write([]byte("ab")); err == nil || err.Error() != "boom" {
		t.Fatalf("Write: got %v, want boom", err)
	}
	if got := q.Used(); got != 1 {
		t.Fatalf("used = %d, want 1 (actual growth only)", got)
	}
	if got := fileSize(t, h); got != 1 {
		t.Fatalf("file size = %d, want 1", got)
	}
}

// Sparse WriteAt is charged by the resulting file length.
func TestQuotaSparseWriteAt(t *testing.T) {
	q, _ := newQuotaMem(t, 8)
	h, err := q.Create("/f")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// 2 bytes at offset 6: the file becomes 8 bytes (6 sparse + 2 data).
	if _, err := h.WriteAt([]byte("xy"), 6); err != nil {
		t.Fatalf("WriteAt@6: %v", err)
	}
	if got := q.Used(); got != 8 {
		t.Fatalf("used = %d, want 8", got)
	}
	if got := fileSize(t, h); got != 8 {
		t.Fatalf("file size = %d, want 8", got)
	}
	// 2 more bytes at offset 8 would make the file 10 bytes: rejected.
	if _, err := h.WriteAt([]byte("z!"), 8); err != ErrQuotaExceeded {
		t.Fatalf("WriteAt@8: got %v, want ErrQuotaExceeded", err)
	}
	if got := q.Used(); got != 8 {
		t.Fatalf("used = %d, want 8", got)
	}
}

// Truncate shrink refunds immediately; a failed remove does not refund.
func TestQuotaTruncateVsDeleteFailure(t *testing.T) {
	src := NewMemMapFs()
	ff := &faultyFs{Fs: src}
	q, err := NewQuotaFs(ff, 8)
	if err != nil {
		t.Fatalf("NewQuotaFs: %v", err)
	}

	h, err := q.Create("/f")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := h.Write([]byte("abcdef")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := q.Used(); got != 6 {
		t.Fatalf("used = %d, want 6", got)
	}

	// Truncate shrink: refund now.
	if err := h.Truncate(3); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if got := q.Used(); got != 3 {
		t.Fatalf("after truncate shrink, used = %d, want 3", got)
	}
	if got := fileSize(t, h); got != 3 {
		t.Fatalf("file size = %d, want 3", got)
	}

	// A failed remove must not refund.
	ff.setRemoveErr(errors.New("remove boom"))
	if err := q.Remove("/f"); err == nil || err.Error() != "remove boom" {
		t.Fatalf("Remove: got %v, want remove boom", err)
	}
	if got := q.Used(); got != 3 {
		t.Fatalf("after failed remove, used = %d, want 3 (no refund)", got)
	}

	// Once the remove succeeds the bytes are refunded. The handle is still
	// open, so the refund is deferred until it closes.
	ff.setRemoveErr(nil)
	if err := q.Remove("/f"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if got := q.Used(); got != 3 {
		t.Fatalf("after successful remove (handle open), used = %d, want 3", got)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := q.Used(); got != 0 {
		t.Fatalf("after handle close, used = %d, want 0", got)
	}
}

// After a successful remove with open handles, the refund waits for the last
// Close.
func TestQuotaDeleteDeferredRefund(t *testing.T) {
	q, _ := newQuotaMem(t, 8)

	h1, err := q.Create("/f")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := h1.Write([]byte("abcde")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	h2, err := q.OpenFile("/f", os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	if err := q.Remove("/f"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if got := q.Used(); got != 5 {
		t.Fatalf("after remove (handles open), used = %d, want 5", got)
	}

	// Closing the second handle is not the last one: no refund yet.
	if err := h2.Close(); err != nil {
		t.Fatalf("h2.Close: %v", err)
	}
	if got := q.Used(); got != 5 {
		t.Fatalf("after h2 close, used = %d, want 5", got)
	}

	// Closing the last handle refunds.
	if err := h1.Close(); err != nil {
		t.Fatalf("h1.Close: %v", err)
	}
	if got := q.Used(); got != 0 {
		t.Fatalf("after last close, used = %d, want 0", got)
	}
}

// Existing regular files are counted at wrapper creation.
func TestQuotaInitCountsExisting(t *testing.T) {
	src := NewMemMapFs()
	mustWrite(t, src, "/a", []byte("abc"))   // 3 bytes
	mustWrite(t, src, "/b", []byte("abcde")) // 5 bytes
	if err := src.Mkdir("/d", 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	mustWrite(t, src, "/d/c", []byte("ab")) // 2 bytes

	q, err := NewQuotaFs(src, 100)
	if err != nil {
		t.Fatalf("NewQuotaFs: %v", err)
	}
	if got := q.Used(); got != 10 {
		t.Fatalf("used = %d, want 10", got)
	}
}

// Symbolic links are rejected at this entry point.
func TestQuotaSymlinkRejected(t *testing.T) {
	src := NewMemMapFs()
	mustWrite(t, src, "/f", []byte("abc"))
	ff := &symlinkFs{Fs: src, links: map[string]bool{"/link": true}}
	q, err := NewQuotaFs(ff, 8)
	if err != nil {
		t.Fatalf("NewQuotaFs: %v", err)
	}

	for _, tc := range []struct {
		name string
		fn   func() error
	}{
		{"Open", func() error { _, e := q.Open("/link"); return e }},
		{"Create", func() error { _, e := q.Create("/link"); return e }},
		{"OpenFile", func() error { _, e := q.OpenFile("/link", os.O_RDWR|os.O_CREATE, 0o666); return e }},
	} {
		if err := tc.fn(); !errors.Is(err, ErrSymlinkNotAllowed) {
			t.Errorf("%s: got %v, want ErrSymlinkNotAllowed", tc.name, err)
		}
	}

	// A regular file is still accessible.
	h, err := q.Open("/f")
	if err != nil {
		t.Fatalf("Open(/f): %v", err)
	}
	if got := fileSize(t, h); got != 3 {
		t.Fatalf("size = %d, want 3", got)
	}
}

// WriteFile (OpenFile O_WRONLY|O_CREATE|O_TRUNC + Write) is accounted for.
func TestQuotaWriteFileHelper(t *testing.T) {
	q, _ := newQuotaMem(t, 8)
	if err := WriteFile(q, "/f", []byte("abcdef"), 0o666); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if got := q.Used(); got != 6 {
		t.Fatalf("used = %d, want 6", got)
	}
	// Overwrite with a shorter file: shrink is refunded.
	if err := WriteFile(q, "/f", []byte("ab"), 0o666); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if got := q.Used(); got != 2 {
		t.Fatalf("used = %d, want 2", got)
	}
}

// ---------------------------------------------------------------------------
// race tests
// ---------------------------------------------------------------------------

// Many goroutines grow their own files; after joining, used equals the sum of
// the file sizes.
func TestQuotaRaceOwnFiles(t *testing.T) {
	q, _ := newQuotaMem(t, 1<<30)
	const n = 16
	const chunk = 64

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("/f%d", i)
			h, err := q.Create(name)
			if err != nil {
				t.Errorf("Create: %v", err)
				return
			}
			defer h.Close()
			for j := 0; j < 10; j++ {
				if _, err := h.Write(make([]byte, chunk)); err != nil {
					t.Errorf("Write: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	var sum int64
	for i := 0; i < n; i++ {
		sum += fsSize(t, q, fmt.Sprintf("/f%d", i))
	}
	if got := q.Used(); got != sum {
		t.Fatalf("used = %d, sum of file sizes = %d", got, sum)
	}
	if sum != n*chunk*10 {
		t.Fatalf("sum = %d, want %d", sum, n*chunk*10)
	}
}

// Many goroutines overwrite the same file through separate handles: the file
// grows at most once and used stays at the file size.
func TestQuotaRaceSameFile(t *testing.T) {
	q, _ := newQuotaMem(t, 1<<30)
	h0, err := q.Create("/shared")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := h0.Write(make([]byte, 128)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	h0.Close()

	const n = 16
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, err := q.OpenFile("/shared", os.O_RDWR, 0)
			if err != nil {
				t.Errorf("OpenFile: %v", err)
				return
			}
			defer h.Close()
			for j := 0; j < 10; j++ {
				if _, err := h.Write([]byte("abcdefgh")); err != nil {
					t.Errorf("Write: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if got := q.Used(); got != 128 {
		t.Fatalf("used = %d, want 128 (overwrites do not charge again)", got)
	}
	if got := fsSize(t, q, "/shared"); got != 128 {
		t.Fatalf("file size = %d, want 128", got)
	}
}

// Race with the faulty fs: many goroutines grow their own files through short
// writes; used equals the sum of the actual file sizes.
func TestQuotaRaceFaultyShortWrites(t *testing.T) {
	src := NewMemMapFs()
	ff := &faultyFs{Fs: src}
	ff.setShortWrite(3)
	q, err := NewQuotaFs(ff, 1<<30)
	if err != nil {
		t.Fatalf("NewQuotaFs: %v", err)
	}
	const n = 16

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("/f%d", i)
			h, err := q.Create(name)
			if err != nil {
				t.Errorf("Create: %v", err)
				return
			}
			defer h.Close()
			for j := 0; j < 10; j++ {
				if _, err := h.Write(make([]byte, 8)); err != nil {
					t.Errorf("Write: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	var sum int64
	for i := 0; i < n; i++ {
		sum += fsSize(t, q, fmt.Sprintf("/f%d", i))
	}
	if got := q.Used(); got != sum {
		t.Fatalf("used = %d, sum of file sizes = %d", got, sum)
	}
}

// Mixed workload: writes, truncates and removes racing together; the invariant
// used == sum of live file sizes must hold at the end.
func TestQuotaRaceMixed(t *testing.T) {
	q, _ := newQuotaMem(t, 1<<30)
	const n = 8

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("/f%d", i)
			h, err := q.Create(name)
			if err != nil {
				t.Errorf("Create: %v", err)
				return
			}
			for j := 0; j < 20; j++ {
				switch j % 4 {
				case 0:
					h.Write(make([]byte, 16))
				case 1:
					h.Truncate(int64((j % 5) * 4))
				case 2:
					h.WriteAt(make([]byte, 4), int64(j%3))
				case 3:
					h.Seek(0, io.SeekStart)
					h.Write(make([]byte, 8))
				}
			}
			h.Close()
			if i%2 == 0 {
				q.Remove(name)
			}
		}(i)
	}
	wg.Wait()

	var sum int64
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("/f%d", i)
		if fi, err := q.Stat(name); err == nil {
			sum += fi.Size()
		}
	}
	if got := q.Used(); got != sum {
		t.Fatalf("used = %d, sum of live file sizes = %d", got, sum)
	}
}

// ---------------------------------------------------------------------------
// demo
// ---------------------------------------------------------------------------

// A short, narrated demonstration of the quota wrapper.
func TestQuotaDemo(t *testing.T) {
	fs := NewMemMapFs()
	q, err := NewQuotaFs(fs, 8)
	if err != nil {
		t.Fatalf("NewQuotaFs: %v", err)
	}

	h, _ := q.Create("/demo")
	h.Write([]byte("hello")) // 5 bytes
	t.Logf("after writing 'hello': used=%d available=%d", q.Used(), q.Available())

	h.WriteAt([]byte("!!"), 6) // 2 bytes at offset 6 -> file length 8
	t.Logf("after WriteAt(\"!!\", 6): used=%d available=%d", q.Used(), q.Available())

	_, err = h.WriteAt([]byte("!"), 8) // would make the file 9 bytes
	t.Logf("write of 1 byte at offset 8: err=%v (used=%d)", err, q.Used())

	h.Truncate(3) // shrink: refund
	t.Logf("after Truncate(3): used=%d available=%d", q.Used(), q.Available())

	q.Remove("/demo")
	h.Close()
	t.Logf("after remove + close: used=%d available=%d", q.Used(), q.Available())
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

func mustRead(t *testing.T, f File) []byte {
	t.Helper()
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return b
}

func mustWrite(t *testing.T, fs Fs, name string, data []byte) {
	t.Helper()
	if err := WriteFile(fs, name, data, 0o666); err != nil {
		t.Fatalf("WriteFile(%s): %v", name, err)
	}
}
