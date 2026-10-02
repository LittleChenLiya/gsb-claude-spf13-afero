// Copyright © 2026 Steve Francia <spf@spf13.com>.
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
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- injected fixtures -----------------------------------------------------

// shortWriteFs injects short writes: every underlying Write/WriteAt/
// WriteString persists at most max bytes and reports a short count without
// an error, emulating a filesystem that writes less than requested.
type shortWriteFs struct {
	Fs
	max int
}

func (s *shortWriteFs) wrap(f File, err error) (File, error) {
	if err != nil {
		return nil, err
	}
	return &shortWriteFile{File: f, max: s.max}, nil
}

func (s *shortWriteFs) Create(name string) (File, error) { return s.wrap(s.Fs.Create(name)) }
func (s *shortWriteFs) Open(name string) (File, error)   { return s.wrap(s.Fs.Open(name)) }
func (s *shortWriteFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	return s.wrap(s.Fs.OpenFile(name, flag, perm))
}

type shortWriteFile struct {
	File
	max int
}

func (f *shortWriteFile) Write(p []byte) (int, error) {
	if len(p) > f.max {
		p = p[:f.max]
	}
	return f.File.Write(p)
}

func (f *shortWriteFile) WriteAt(p []byte, off int64) (int, error) {
	if len(p) > f.max {
		p = p[:f.max]
	}
	return f.File.WriteAt(p, off)
}

// failRemoveFs injects Remove failures for selected paths.
type failRemoveFs struct {
	Fs
	fail map[string]bool
}

func (f failRemoveFs) Remove(name string) error {
	if f.fail[quotaKey(name)] {
		return &os.PathError{Op: "remove", Path: name, Err: errors.New("injected remove failure")}
	}
	return f.Fs.Remove(name)
}

// writeCountFs counts how many write calls reach the underlying filesystem,
// to prove that quota rejections never touch it.
type writeCountFs struct {
	Fs
	writes *int64
}

func (c *writeCountFs) wrap(f File, err error) (File, error) {
	if err != nil {
		return nil, err
	}
	return &writeCountFile{File: f, writes: c.writes}, nil
}

func (c *writeCountFs) Create(name string) (File, error) { return c.wrap(c.Fs.Create(name)) }
func (c *writeCountFs) Open(name string) (File, error)   { return c.wrap(c.Fs.Open(name)) }
func (c *writeCountFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	return c.wrap(c.Fs.OpenFile(name, flag, perm))
}

type writeCountFile struct {
	File
	writes *int64
}

func (f *writeCountFile) Write(p []byte) (int, error) {
	atomic.AddInt64(f.writes, 1)
	return f.File.Write(p)
}

func (f *writeCountFile) WriteAt(p []byte, off int64) (int, error) {
	atomic.AddInt64(f.writes, 1)
	return f.File.WriteAt(p, off)
}

// linkInjectFs injects a synthetic symlink entry "/link" into a MemMapFs
// (which cannot create real symlinks) while inject is set.
type linkInjectFs struct {
	Fs
	inject *atomic.Bool
}

type fakeLinkInfo struct{}

func (fakeLinkInfo) Name() string       { return "link" }
func (fakeLinkInfo) Size() int64        { return 0 }
func (fakeLinkInfo) Mode() os.FileMode  { return os.ModeSymlink | 0o777 }
func (fakeLinkInfo) ModTime() time.Time { return time.Time{} }
func (fakeLinkInfo) IsDir() bool        { return false }
func (fakeLinkInfo) Sys() interface{}   { return nil }

func (l *linkInjectFs) LstatIfPossible(name string) (os.FileInfo, bool, error) {
	if l.inject.Load() && quotaKey(name) == "/link" {
		return fakeLinkInfo{}, true, nil
	}
	if ls, ok := l.Fs.(Lstater); ok {
		return ls.LstatIfPossible(name)
	}
	fi, err := l.Fs.Stat(name)
	return fi, false, err
}

func (l *linkInjectFs) Open(name string) (File, error) {
	f, err := l.Fs.Open(name)
	if err != nil {
		return nil, err
	}
	if l.inject.Load() && quotaKey(name) == "/" {
		return &linkDirFile{File: f}, nil
	}
	return f, nil
}

type linkDirFile struct{ File }

func (d *linkDirFile) Readdirnames(n int) ([]string, error) {
	names, err := d.File.Readdirnames(n)
	if n < 0 {
		names = append(names, "link")
	}
	return names, err
}

// --- helpers ---------------------------------------------------------------

func mustQuotaFs(t *testing.T, source Fs, quota int64) *QuotaFs {
	t.Helper()
	fs, err := NewQuotaFs(source, quota)
	if err != nil {
		t.Fatalf("NewQuotaFs: %v", err)
	}
	return fs
}

func mustWrite(t *testing.T, f File, p []byte) {
	t.Helper()
	if _, err := f.Write(p); err != nil {
		t.Fatalf("write %q: %v", p, err)
	}
}

func fileSize(t *testing.T, fs Fs, name string) int64 {
	t.Helper()
	fi, err := fs.Stat(name)
	if err != nil {
		t.Fatalf("stat %s: %v", name, err)
	}
	return fi.Size()
}

// --- acceptance scenario ---------------------------------------------------

// The acceptance scenario: total quota 8, two handles on the same file,
// overwrites are free, a sparse write to offset 7 with 2 bytes is rejected
// before touching the underlying filesystem, and an injected short write of
// 1 byte only charges the actual growth.
func TestQuotaAcceptance(t *testing.T) {
	var underlyingWrites int64
	base := &writeCountFs{Fs: NewMemMapFs(), writes: &underlyingWrites}
	fs := mustQuotaFs(t, base, 8)

	h1, err := fs.Create("/f")
	if err != nil {
		t.Fatal(err)
	}
	h2, err := fs.OpenFile("/f", os.O_RDWR, 0o666)
	if err != nil {
		t.Fatal(err)
	}
	defer h1.Close()
	defer h2.Close()

	mustWrite(t, h1, []byte("abcd")) // file length 4
	if got := fs.Used(); got != 4 {
		t.Fatalf("used = %d, want 4", got)
	}

	// Second handle overwrites existing bytes: no extra charge.
	mustWrite(t, h2, []byte("WXYZ")) // h2 offset is 0
	if got := fs.Used(); got != 4 {
		t.Fatalf("overwrite charged: used = %d, want 4", got)
	}
	if _, err := h2.WriteAt([]byte("12"), 2); err != nil { // still within the 4 existing bytes
		t.Fatal(err)
	}
	if got := fs.Used(); got != 4 {
		t.Fatalf("overwrite charged: used = %d, want 4", got)
	}

	// Sparse write to offset 7 with 2 bytes would grow the file to 9 > 8.
	before := atomic.LoadInt64(&underlyingWrites)
	if _, err := h1.WriteAt([]byte("zz"), 7); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("WriteAt err = %v, want ErrQuotaExceeded", err)
	}
	if got := atomic.LoadInt64(&underlyingWrites); got != before {
		t.Fatal("rejected WriteAt touched the underlying filesystem")
	}
	if got := fileSize(t, fs, "/f"); got != 4 {
		t.Fatalf("file size = %d, want 4 (rejection must not write)", got)
	}
	if got := fs.Used(); got != 4 {
		t.Fatalf("used = %d after rejection, want 4", got)
	}

	// A write that fits exactly is accepted: offset 6, 2 bytes -> length 8.
	if _, err := h1.WriteAt([]byte("ok"), 6); err != nil {
		t.Fatalf("WriteAt within quota: %v", err)
	}
	if got := fs.Used(); got != 8 {
		t.Fatalf("used = %d, want 8", got)
	}
}

func TestQuotaShortWriteCommitsActualGrowth(t *testing.T) {
	base := &shortWriteFs{Fs: NewMemMapFs(), max: 1}
	fs := mustQuotaFs(t, base, 8)

	f, err := fs.Create("/g")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Ask for 2 bytes, the underlying filesystem persists only 1.
	n, err := f.Write([]byte("ab"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("n = %d, want 1 (injected short write)", n)
	}
	if got := fs.Used(); got != 1 {
		t.Fatalf("used = %d, want 1: only actual growth may be committed", got)
	}
	if got := fileSize(t, fs, "/g"); got != 1 {
		t.Fatalf("file size = %d, want 1", got)
	}

	// Same for WriteAt and for a failing underlying write.
	if _, err := f.WriteAt([]byte("cd"), 4); err != nil {
		t.Fatal(err)
	}
	if got := fs.Used(); got != 5 {
		t.Fatalf("used = %d, want 5 after sparse short write", got)
	}
}

func TestQuotaInitialScanCountsExistingFiles(t *testing.T) {
	base := NewMemMapFs()
	if err := WriteFile(base, "/a", []byte("12345"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := base.MkdirAll("/d", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(base, "/d/b", []byte("xy"), 0o644); err != nil {
		t.Fatal(err)
	}

	fs := mustQuotaFs(t, base, 8)
	if got := fs.Used(); got != 7 {
		t.Fatalf("used = %d, want 7 from initial scan", got)
	}
	// Only 1 byte of headroom left.
	f, err := fs.Create("/c")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write([]byte("ab")); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("err = %v, want ErrQuotaExceeded", err)
	}
	mustWrite(t, f, []byte("z"))
	if got := fs.Used(); got != 8 {
		t.Fatalf("used = %d, want 8", got)
	}
}

// --- links -----------------------------------------------------------------

func TestQuotaRejectsLinksAtConstruction(t *testing.T) {
	inject := &atomic.Bool{}
	inject.Store(true)
	base := &linkInjectFs{Fs: NewMemMapFs(), inject: inject}

	_, err := NewQuotaFs(base, 100)
	if !errors.Is(err, ErrQuotaLink) {
		t.Fatalf("NewQuotaFs err = %v, want ErrQuotaLink", err)
	}
}

func TestQuotaLinkCannotBypassWrapper(t *testing.T) {
	inject := &atomic.Bool{}
	base := &linkInjectFs{Fs: NewMemMapFs(), inject: inject}
	fs := mustQuotaFs(t, base, 100)

	// The wrapper never creates links.
	if err := fs.SymlinkIfPossible("/target", "/link"); !errors.Is(err, ErrQuotaLink) {
		t.Fatalf("SymlinkIfPossible err = %v, want ErrQuotaLink", err)
	}

	// A link appearing behind the wrapper's back cannot be opened for
	// writing through the wrapper.
	inject.Store(true)
	if _, err := fs.OpenFile("/link", os.O_WRONLY|os.O_CREATE, 0o666); !errors.Is(err, ErrQuotaLink) {
		t.Fatalf("OpenFile err = %v, want ErrQuotaLink", err)
	}
	if _, err := fs.Create("/link"); !errors.Is(err, ErrQuotaLink) {
		t.Fatalf("Create err = %v, want ErrQuotaLink", err)
	}
	// Reads of unrelated files keep working.
	inject.Store(false)
	if err := WriteFile(fs, "/ok", []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := fs.Used(); got != 2 {
		t.Fatalf("used = %d, want 2", got)
	}
}

// --- truncate / remove refunds ---------------------------------------------

func TestQuotaTruncateShrinkRefundsImmediately(t *testing.T) {
	fs := mustQuotaFs(t, NewMemMapFs(), 16)
	f, err := fs.Create("/f")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := f.Truncate(10); err != nil {
		t.Fatal(err)
	}
	if got := fs.Used(); got != 10 {
		t.Fatalf("used = %d, want 10", got)
	}
	// Growing beyond the quota is rejected without touching the file.
	if err := f.Truncate(17); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("Truncate err = %v, want ErrQuotaExceeded", err)
	}
	if got := fileSize(t, fs, "/f"); got != 10 {
		t.Fatalf("file size = %d, want 10 (rejection must not truncate)", got)
	}
	// Shrinking refunds immediately.
	if err := f.Truncate(3); err != nil {
		t.Fatal(err)
	}
	if got := fs.Used(); got != 3 {
		t.Fatalf("used = %d, want 3 after shrink refund", got)
	}
}

func TestQuotaFailedRemoveRefundsNothing(t *testing.T) {
	base := failRemoveFs{Fs: NewMemMapFs(), fail: map[string]bool{"/f": true}}
	fs := mustQuotaFs(t, base, 16)
	if err := WriteFile(fs, "/f", []byte("123456"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := fs.Used(); got != 6 {
		t.Fatalf("used = %d, want 6", got)
	}
	if err := fs.Remove("/f"); err == nil {
		t.Fatal("expected injected remove failure")
	}
	// Unlike a Truncate shrink, a failed remove must not refund.
	if got := fs.Used(); got != 6 {
		t.Fatalf("used = %d, want 6: failed remove must not refund", got)
	}
}

func TestQuotaRemoveRefundDeferredToLastClose(t *testing.T) {
	fs := mustQuotaFs(t, NewMemMapFs(), 8)
	h1, err := fs.Create("/f")
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, h1, []byte("12345678")) // uses the whole quota of 8
	h2, err := fs.OpenFile("/f", os.O_RDWR, 0o666)
	if err != nil {
		t.Fatal(err)
	}

	if err := fs.Remove("/f"); err != nil {
		t.Fatal(err)
	}
	// Handles are still open: nothing is refunded yet.
	if got := fs.Used(); got != 8 {
		t.Fatalf("used = %d, want 8 while handles are open", got)
	}
	// Accounting happens at write time, not at Close: a write through the
	// surviving handle must still be bounded by the quota.
	if _, err := h2.WriteAt([]byte("!"), 8); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("write on removed file err = %v, want ErrQuotaExceeded", err)
	}
	// Overwriting existing bytes of the removed file is still free.
	if _, err := h2.WriteAt([]byte("?"), 0); err != nil {
		t.Fatal(err)
	}

	if err := h1.Close(); err != nil {
		t.Fatal(err)
	}
	if got := fs.Used(); got != 8 {
		t.Fatalf("used = %d, want 8: one handle still open", got)
	}
	if err := h2.Close(); err != nil {
		t.Fatal(err)
	}
	if got := fs.Used(); got != 0 {
		t.Fatalf("used = %d, want 0 after last close", got)
	}
}

func TestQuotaRemoveWithoutHandlesRefundsImmediately(t *testing.T) {
	fs := mustQuotaFs(t, NewMemMapFs(), 8)
	if err := WriteFile(fs, "/f", []byte("12345"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := fs.Used(); got != 5 {
		t.Fatalf("used = %d, want 5", got)
	}
	if err := fs.Remove("/f"); err != nil {
		t.Fatal(err)
	}
	if got := fs.Used(); got != 0 {
		t.Fatalf("used = %d, want 0", got)
	}
}

func TestQuotaConcurrentHandlesShareReservation(t *testing.T) {
	fs := mustQuotaFs(t, NewMemMapFs(), 8)
	h1, err := fs.Create("/f")
	if err != nil {
		t.Fatal(err)
	}
	defer h1.Close()
	h2, err := fs.OpenFile("/f", os.O_RDWR, 0o666)
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()

	if _, err := h1.WriteAt([]byte("aaaaaa"), 0); err != nil {
		t.Fatal(err)
	}
	// The second handle shares the same accounting record: growing the
	// file to 12 would exceed the quota of 8 and is rejected up front,
	// not deferred to Close where both handles could jointly overspend.
	if _, err := h2.WriteAt([]byte("bbbbbb"), 6); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("err = %v, want ErrQuotaExceeded", err)
	}
	if got := fs.Used(); got != 6 {
		t.Fatalf("used = %d, want 6", got)
	}
}

// --- race tests (run with -race) -------------------------------------------

// Concurrent handles on one shared file (MemMapFs): every goroutine rewrites
// the same 512-byte window through its own handle. Overwrites are free, so
// the final charge must equal the final file length exactly, and the used
// bytes must never leave [0, quota].
func TestQuotaRaceConcurrentHandles(t *testing.T) {
	const quota = 1 << 20
	fs := mustQuotaFs(t, NewMemMapFs(), quota)
	seed, err := fs.Create("/shared")
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}

	var samplerStop atomic.Bool
	var samplerErr atomic.Value
	go func() {
		for !samplerStop.Load() {
			u := fs.Used()
			if u < 0 || u > quota {
				samplerErr.Store(fmt.Sprintf("used out of bounds: %d", u))
				return
			}
		}
	}()

	const workers = 8
	const window = 512
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			h, err := fs.OpenFile("/shared", os.O_RDWR, 0o666)
			if err != nil {
				t.Error(err)
				return
			}
			defer h.Close()
			buf := []byte{byte('A' + w), byte('a' + w), byte('0' + w), byte('!')}
			for off := 0; off < window; off += len(buf) {
				if _, err := h.WriteAt(buf, int64(off)); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	samplerStop.Store(true)
	if v := samplerErr.Load(); v != nil {
		t.Fatal(v)
	}
	if got := fs.Used(); got != window {
		t.Fatalf("used = %d, want %d", got, window)
	}
	if got := fileSize(t, fs, "/shared"); got != window {
		t.Fatalf("file size = %d, want %d", got, window)
	}
}

// Remove racing with writes and closes on still-open handles: the refund
// must happen exactly once, at the last Close.
func TestQuotaRaceRemoveVsClose(t *testing.T) {
	const quota = 1 << 20
	fs := mustQuotaFs(t, NewMemMapFs(), quota)
	if err := WriteFile(fs, "/victim", []byte("0123456789abcdef"), 0o644); err != nil {
		t.Fatal(err)
	}

	const workers = 8
	handles := make([]File, workers)
	for i := range handles {
		h, err := fs.OpenFile("/victim", os.O_RDWR, 0o666)
		if err != nil {
			t.Fatal(err)
		}
		handles[i] = h
	}

	if err := fs.Remove("/victim"); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i, h := range handles {
		wg.Add(1)
		go func(i int, h File) {
			defer wg.Done()
			// Growth writes on the unlinked file are still metered.
			if _, err := h.WriteAt([]byte("xy"), int64(64+i)); err != nil {
				t.Error(err)
			}
			if err := h.Close(); err != nil {
				t.Error(err)
			}
		}(i, h)
	}
	wg.Wait()
	if got := fs.Used(); got != 0 {
		t.Fatalf("used = %d, want 0 after remove + last close", got)
	}
}

// Race test riding on the injected short-write fixture: short underlying
// writes must only ever commit real growth, per file, concurrently.
func TestQuotaRaceShortWriteFixture(t *testing.T) {
	const quota = 1 << 20
	base := &shortWriteFs{Fs: NewMemMapFs(), max: 3}
	fs := mustQuotaFs(t, base, quota)

	const workers = 8
	const writesPerWorker = 50
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			name := fmt.Sprintf("/g%d", w)
			h, err := fs.Create(name)
			if err != nil {
				t.Error(err)
				return
			}
			defer h.Close()
			for i := 0; i < writesPerWorker; i++ {
				// 8 bytes requested, only 3 persist per call.
				if _, err := h.Write([]byte("12345678")); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	want := int64(workers * writesPerWorker * 3)
	if got := fs.Used(); got != want {
		t.Fatalf("used = %d, want %d (only actual short-write growth)", got, want)
	}
	for w := 0; w < workers; w++ {
		if got := fileSize(t, fs, fmt.Sprintf("/g%d", w)); got != writesPerWorker*3 {
			t.Fatalf("file %d size = %d, want %d", w, got, writesPerWorker*3)
		}
	}
}

// --- demo ------------------------------------------------------------------

// A short demonstration of a tiny quota: 8 bytes total.
func ExampleQuotaFs() {
	fs, err := NewQuotaFs(NewMemMapFs(), 8)
	if err != nil {
		panic(err)
	}
	f, _ := fs.Create("/demo.txt")
	_, _ = f.Write([]byte("abcd")) // 4 of 8 bytes used
	_, _ = f.Write([]byte("efgh")) // 8 of 8 bytes used, quota full
	if _, err := f.Write([]byte("!")); err != nil {
		fmt.Println("grow:", err)
	}
	if _, err := f.WriteAt([]byte("xy"), 7); err != nil {
		fmt.Println("sparse:", err)
	}
	_ = f.Close()
	fmt.Println("used:", fs.Used())

	// Output:
	// grow: write /demo.txt: afero: quota exceeded
	// sparse: write /demo.txt: afero: quota exceeded
	// used: 8
}
