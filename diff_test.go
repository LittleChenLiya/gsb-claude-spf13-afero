package afero

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"testing"
	"time"
)

const fixedTime = "2026-10-04T12:00:00Z"

func mustTime(t *testing.T, v string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, v)
	if err != nil {
		t.Fatal(err)
	}
	return tm
}

// putFile writes a file and pins its modification time so equal-content files
// have equal mtimes (the comparator must not rely on mtime anyway).
func putFile(t *testing.T, fs Fs, name, content string) {
	t.Helper()
	if err := fs.MkdirAll(path.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(fs, name, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := fs.Chtimes(name, mustTime(t, fixedTime), mustTime(t, fixedTime)); err != nil {
		t.Fatal(err)
	}
}

func typesByRel(diffs []Diff) map[string]DiffType {
	m := map[string]DiffType{}
	for _, d := range diffs {
		m[d.RelPath] = d.Type
	}
	return m
}

// TestSameMtimeSizeDifferentContent verifies the core acceptance case: equal
// length and equal modification time do not imply equal contents.
func TestSameMtimeSizeDifferentContent(t *testing.T) {
	base := &MemMapFs{}
	nwe := &MemMapFs{}
	putFile(t, base, "/root/a.txt", "hello!")
	putFile(t, nwe, "/root/a.txt", "hello?")
	putFile(t, base, "/root/same.txt", "identical")
	putFile(t, nwe, "/root/same.txt", "identical")

	diffs, err := NewDirDiffer().DiffTrees(base, "/root", nwe, "/root")
	if err != nil {
		t.Fatal(err)
	}
	got := typesByRel(diffs)
	if got["a.txt"] != DiffContentChanged {
		t.Fatalf("expected content_changed for a.txt, got %v (diffs=%v)", got["a.txt"], diffs)
	}
	if _, ok := got["same.txt"]; ok {
		t.Fatalf("equal file must not be reported: %v", diffs)
	}
}

// TestEqualContentDifferentMtime verifies modification time alone never
// drives the result: identical bytes compare equal regardless of mtime.
func TestEqualContentDifferentMtime(t *testing.T) {
	base := &MemMapFs{}
	nwe := &MemMapFs{}
	putFile(t, base, "/root/a.txt", "same bytes")
	putFile(t, nwe, "/root/a.txt", "same bytes")
	// Give the new copy a clearly different modification time.
	if err := nwe.Chtimes("/root/a.txt",
		mustTime(t, "2020-01-01T00:00:00Z"), mustTime(t, "2020-01-01T00:00:00Z")); err != nil {
		t.Fatal(err)
	}

	diffs, err := NewDirDiffer().DiffTrees(base, "/root", nwe, "/root")
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 0 {
		t.Fatalf("identical content with different mtime must report no diff: %v", diffs)
	}
}

// TestTypeConflictFileDir verifies a file and a directory sharing a relative
// path are reported as a type_conflict, and directory contents are not walked
// into the conflict.
func TestTypeConflictFileDir(t *testing.T) {
	base := &MemMapFs{}
	nwe := &MemMapFs{}
	putFile(t, base, "/root/x", "i am a file")
	putFile(t, nwe, "/root/x/child.txt", "i live under a directory")

	diffs, err := NewDirDiffer().DiffTrees(base, "/root", nwe, "/root")
	if err != nil {
		t.Fatal(err)
	}
	got := typesByRel(diffs)
	if got["x"] != DiffTypeConflict {
		t.Fatalf("expected type_conflict at x, got %v (diffs=%v)", got["x"], diffs)
	}
	if d := findDiff(diffs, "x"); d != nil {
		if d.BaseType != NodeFile || d.NewType != NodeDir {
			t.Fatalf("conflict types = %s -> %s, want file -> dir", d.BaseType, d.NewType)
		}
	}
	// Children of a type-conflicting directory are not reported separately.
	if _, ok := got["x/child.txt"]; ok {
		t.Fatalf("child of type-conflicting path must not be reported separately: %v", diffs)
	}
}

func findDiff(diffs []Diff, rel string) *Diff {
	for i := range diffs {
		if diffs[i].RelPath == rel {
			return &diffs[i]
		}
	}
	return nil
}

// TestAddedMissingAndStableSort verifies presence diffs and the stable sort
// order (relative path, then type).
func TestAddedMissingAndStableSort(t *testing.T) {
	base := &MemMapFs{}
	nwe := &MemMapFs{}
	putFile(t, base, "/root/only-base.txt", "b")
	putFile(t, nwe, "/root/only-new.txt", "n")
	putFile(t, base, "/root/dir/old.txt", "o")
	putFile(t, nwe, "/root/dir/fresh.txt", "f")

	diffs, err := NewDirDiffer().DiffTrees(base, "/root", nwe, "/root")
	if err != nil {
		t.Fatal(err)
	}
	got := typesByRel(diffs)
	if got["only-base.txt"] != DiffMissing || got["only-new.txt"] != DiffAdded {
		t.Fatalf("unexpected presence diffs: %v", diffs)
	}
	if got["dir/old.txt"] != DiffMissing || got["dir/fresh.txt"] != DiffAdded {
		t.Fatalf("unexpected nested diffs: %v", diffs)
	}

	rels := make([]string, len(diffs))
	for i, d := range diffs {
		rels[i] = d.RelPath
	}
	if !sort.StringsAreSorted(rels) {
		t.Fatalf("diffs not sorted by relative path: %v", rels)
	}
}

// TestSimilarPrefixDirectories verifies that similarly prefixed directories
// ("a" and "ab") never get their relative paths merged or trimmed incorrectly.
func TestSimilarPrefixDirectories(t *testing.T) {
	base := &MemMapFs{}
	nwe := &MemMapFs{}
	putFile(t, base, "/root/a/file.txt", "base-a")
	putFile(t, nwe, "/root/a/file.txt", "new-a")
	putFile(t, base, "/root/ab/file.txt", "base-ab")
	putFile(t, nwe, "/root/ab/file.txt", "new-ab")

	diffs, err := NewDirDiffer().DiffTrees(base, "/root", nwe, "/root")
	if err != nil {
		t.Fatal(err)
	}
	got := typesByRel(diffs)
	for _, rel := range []string{"a/file.txt", "ab/file.txt"} {
		if got[rel] != DiffContentChanged {
			t.Fatalf("expected content_changed at %q, got %v; all=%v", rel, got[rel], diffs)
		}
	}
	if _, ok := got["ab"]; ok {
		t.Fatalf("prefix collision produced spurious entry ab: %v", diffs)
	}
}

// TestNoMutationOnEitherSide ensures the comparator only reads: file bytes,
// directory listings and metadata stay byte-for-byte unchanged.
func TestNoMutationOnEitherSide(t *testing.T) {
	base := &MemMapFs{}
	nwe := &MemMapFs{}
	payload := strings.Repeat("x", 130*1024)
	putFile(t, base, "/root/big.txt", payload)
	putFile(t, nwe, "/root/big.txt", payload)

	bi, _ := base.Stat("/root/big.txt")
	ni, _ := nwe.Stat("/root/big.txt")

	if _, err := NewDirDiffer(WithChunkSize(4096)).DiffTrees(base, "/root", nwe, "/root"); err != nil {
		t.Fatal(err)
	}

	bi2, _ := base.Stat("/root/big.txt")
	ni2, _ := nwe.Stat("/root/big.txt")
	if !sameMeta(bi, bi2) || !sameMeta(ni, ni2) {
		t.Fatal("comparator mutated file metadata")
	}
	got, err := ReadFile(base, "/root/big.txt")
	if err != nil || string(got) != payload {
		t.Fatal("comparator mutated base file contents")
	}
}

// mutateOnReadFs rewrites a file's data halfway through the first read,
// emulating another actor changing a fixture while it is being compared. The
// file size changes before the post-read metadata check, so the comparator
// must report unstable.
type mutateOnReadFs struct {
	Fs
	target      string
	replacement []byte // nil -> a shorter replacement that changes size
	done        bool
}

func (m *mutateOnReadFs) Open(name string) (File, error) {
	f, err := m.Fs.Open(name)
	if err != nil || path.Clean(name) != path.Clean(m.target) {
		return f, err
	}
	return &mutatingFile{File: f, fs: m.Fs, name: name, owner: m}, nil
}

func (m *mutateOnReadFs) LstatIfPossible(name string) (os.FileInfo, bool, error) {
	if ls, ok := m.Fs.(Lstater); ok {
		return ls.LstatIfPossible(name)
	}
	fi, err := m.Fs.Stat(name)
	return fi, false, err
}

type mutatingFile struct {
	File
	fs      Fs
	name    string
	owner   *mutateOnReadFs
	mutated bool
}

func (f *mutatingFile) Read(p []byte) (int, error) {
	// Let the first block be read normally, then mutate shared backing data
	// before the next block: a true mid-read change.
	if f.mutated || f.owner.done {
		return f.File.Read(p)
	}
	n, err := f.File.Read(p)
	if !f.owner.done && !f.mutated {
		f.mutated = true
		f.owner.done = true
		replacement := f.owner.replacement
		if replacement == nil {
			replacement = []byte("MUTATED-WHILE-READING-CONTENT")
		}
		_ = WriteFile(f.fs, f.name, replacement, 0o644)
	}
	return n, err
}

// TestUnstableWhenMutatedDuringRead verifies a file changed while the
// comparator reads it is flagged unstable and never reported equal.
func TestUnstableWhenMutatedDuringRead(t *testing.T) {
	base := &MemMapFs{}
	nwe := &MemMapFs{}
	// Larger than one chunk so the mutation happens mid-stream.
	putFile(t, base, "/root/changing.txt", strings.Repeat("a", 100*1024))
	putFile(t, nwe, "/root/changing.txt", strings.Repeat("a", 100*1024))

	mutating := &mutateOnReadFs{Fs: nwe, target: "/root/changing.txt"}
	diffs, err := NewDirDiffer(WithChunkSize(1024)).DiffTrees(base, "/root", mutating, "/root")
	if err != nil {
		t.Fatal(err)
	}
	d := findDiff(diffs, "changing.txt")
	if d == nil {
		t.Fatal("expected a diff for the mutating file")
	}
	if d.Type != DiffUnstable {
		t.Fatalf("expected unstable, got %s (%v)", d.Type, d)
	}
}

// TestUnstableWhenMutatedSameLength verifies a same-size content rewrite
// during reading is also flagged unstable via the post-read metadata check,
// rather than masquerading as equal or as a clean content change.
func TestUnstableWhenMutatedSameLength(t *testing.T) {
	base := &MemMapFs{}
	nwe := &MemMapFs{}
	original := strings.Repeat("a", 100*1024)
	replaced := strings.Repeat("b", 100*1024) // identical length
	putFile(t, base, "/root/changing.txt", original)
	putFile(t, nwe, "/root/changing.txt", original)

	mutating := &mutateOnReadFs{Fs: nwe, target: "/root/changing.txt", replacement: []byte(replaced)}
	diffs, err := NewDirDiffer(WithChunkSize(1024)).DiffTrees(base, "/root", mutating, "/root")
	if err != nil {
		t.Fatal(err)
	}
	d := findDiff(diffs, "changing.txt")
	if d == nil || d.Type != DiffUnstable {
		t.Fatalf("expected unstable for same-length rewrite, got %v", diffs)
	}
}

// countingReader wraps a reader and records the maximum requested read size
// and the number of reads, so chunk boundaries can be asserted deterministically.
type countingReader struct {
	r           io.Reader
	maxRead     int
	reads       int
	total       int64
	failAfter   int // inject an error after this many reads (0 = never)
	errToReturn error
}

func (c *countingReader) Read(p []byte) (int, error) {
	c.reads++
	if len(p) > c.maxRead {
		c.maxRead = len(p)
	}
	if c.failAfter > 0 && c.reads > c.failAfter {
		return 0, c.errToReturn
	}
	n, err := c.r.Read(p)
	c.total += int64(n)
	return n, err
}

// TestStreamEqualChunkBounds uses a small in-memory source and a counting
// reader to verify block size is honored for multi-chunk files.
func TestStreamEqualChunkBounds(t *testing.T) {
	d := NewDirDiffer(WithChunkSize(16))
	data := []byte("0123456789abcdef0123456789abcdef_trailing") // 39 bytes
	cr := &countingReader{r: bytes.NewReader(data)}
	ok, err := d.streamEqual(cr, bytes.NewReader(append([]byte{}, data...)))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("identical streams compared unequal")
	}
	if cr.maxRead != 16 {
		t.Fatalf("max read buffer = %d, want chunk size 16", cr.maxRead)
	}
	if cr.total != int64(len(data)) {
		t.Fatalf("read %d of %d bytes", cr.total, len(data))
	}
	if cr.reads < 3 {
		t.Fatalf("expected at least 3 chunk reads, got %d", cr.reads)
	}
}

// TestStreamEqualReaderError verifies reader errors propagate instead of
// being misreported as a content difference.
func TestStreamEqualReaderError(t *testing.T) {
	d := NewDirDiffer(WithChunkSize(8))
	boom := errors.New("simulated read failure")
	cr := &countingReader{r: bytes.NewReader([]byte("0123456789")), failAfter: 1, errToReturn: boom}
	_, err := d.streamEqual(cr, bytes.NewReader([]byte("0123456789")))
	if !errors.Is(err, boom) {
		t.Fatalf("expected injected error, got %v", err)
	}
}

// TestStreamEqualDetectsBlockDifference verifies a mismatch in any block is
// found without reading the rest.
func TestStreamEqualDetectsBlockDifference(t *testing.T) {
	d := NewDirDiffer(WithChunkSize(4))
	a := bytes.NewReader([]byte("same-same-DIFF-ignored-tail"))
	b := bytes.NewReader([]byte("same-same-XXXX-ignored-tail"))
	ok, err := d.streamEqual(a, b)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected unequal streams")
	}
}

// TestChunkSizeWithMemMapFs runs the full comparator against small MemMapFs
// trees with a tiny chunk size, exercising the chunk loop end to end.
func TestChunkSizeWithMemMapFs(t *testing.T) {
	base := &MemMapFs{}
	nwe := &MemMapFs{}
	content := strings.Repeat("z", 5*1024+7)
	putFile(t, base, "/root/f", content)
	putFile(t, nwe, "/root/f", content)

	diffs, err := NewDirDiffer(WithChunkSize(64)).DiffTrees(base, "/root", nwe, "/root")
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 0 {
		t.Fatalf("equal multi-chunk files produced diffs: %v", diffs)
	}

	// Change only the last byte: the comparator must still find it.
	putFile(t, nwe, "/root/f", strings.Repeat("z", 5*1024+6)+"q")
	diffs, err = NewDirDiffer(WithChunkSize(64)).DiffTrees(base, "/root", nwe, "/root")
	if err != nil {
		t.Fatal(err)
	}
	if typesByRel(diffs)["f"] != DiffContentChanged {
		t.Fatalf("expected content_changed, got %v", diffs)
	}
}

// fakeSymlinkFs is a small in-memory filesystem that models symlinks
// explicitly (MemMapFs cannot), so symlink comparison can be tested
// deterministically and natively on every platform.
type fakeSymlinkFs struct {
	links map[string]string // link path -> target
	infos map[string]os.FileInfo
	dirs  map[string]map[string]struct{}
	files map[string][]byte
}

func newFakeSymlinkFs() *fakeSymlinkFs {
	return &fakeSymlinkFs{
		links: map[string]string{},
		infos: map[string]os.FileInfo{},
		dirs:  map[string]map[string]struct{}{},
		files: map[string][]byte{},
	}
}

func (f *fakeSymlinkFs) addFile(p, content string) {
	f.files[p] = []byte(content)
	f.infos[p] = fakeInfo{name: path.Base(p), size: int64(len(content)), mode: 0o644, mtime: mustTimeP(fixedTime)}
	f.register(p)
}

func (f *fakeSymlinkFs) addLink(p, target string) {
	f.links[p] = target
	f.infos[p] = fakeInfo{name: path.Base(p), mode: os.ModeSymlink | 0o777, mtime: mustTimeP(fixedTime)}
	f.register(p)
}

func (f *fakeSymlinkFs) register(p string) {
	dir := path.Dir(p)
	if f.dirs[dir] == nil {
		f.dirs[dir] = map[string]struct{}{}
	}
	f.dirs[dir][path.Base(p)] = struct{}{}
}

type fakeInfo struct {
	name  string
	size  int64
	mode  os.FileMode
	mtime time.Time
}

func (i fakeInfo) Name() string       { return i.name }
func (i fakeInfo) Size() int64        { return i.size }
func (i fakeInfo) Mode() os.FileMode  { return i.mode }
func (i fakeInfo) ModTime() time.Time { return i.mtime }
func (i fakeInfo) IsDir() bool        { return i.mode.IsDir() }
func (i fakeInfo) Sys() any           { return nil }

func mustTimeP(v string) time.Time {
	tm, _ := time.Parse(time.RFC3339, v)
	return tm
}

func (f *fakeSymlinkFs) LstatIfPossible(p string) (os.FileInfo, bool, error) {
	if fi, ok := f.infos[p]; ok {
		return fi, true, nil
	}
	return nil, true, &os.PathError{Op: "stat", Path: p, Err: os.ErrNotExist}
}

func (f *fakeSymlinkFs) Stat(p string) (os.FileInfo, error) {
	fi, _, err := f.LstatIfPossible(p)
	return fi, err
}

func (f *fakeSymlinkFs) ReadlinkIfPossible(p string) (string, error) {
	if t, ok := f.links[p]; ok {
		return t, nil
	}
	return "", &os.PathError{Op: "readlink", Path: p, Err: ErrNoReadlink}
}

// Open returns a directory listing for directories or a regular file reader.
func (f *fakeSymlinkFs) Open(p string) (File, error) {
	if entries, ok := f.dirs[p]; ok {
		names := make([]string, 0, len(entries))
		for n := range entries {
			names = append(names, n)
		}
		sort.Strings(names)
		return &listingFile{names: names}, nil
	}
	if data, ok := f.files[p]; ok {
		return &fakeRegFile{Reader: bytes.NewReader(data), name: path.Base(p)}, nil
	}
	if _, statErr := f.Stat(p); statErr == nil {
		return nil, &os.PathError{Op: "open", Path: p, Err: os.ErrPermission}
	}
	return nil, &os.PathError{Op: "open", Path: p, Err: os.ErrNotExist}
}

// The remaining Fs methods are write operations and are intentionally
// rejected: the comparator must never invoke them.
func (f *fakeSymlinkFs) Create(string) (File, error) {
	return nil, os.ErrPermission
}
func (f *fakeSymlinkFs) Mkdir(string, os.FileMode) error    { return os.ErrPermission }
func (f *fakeSymlinkFs) MkdirAll(string, os.FileMode) error { return os.ErrPermission }
func (f *fakeSymlinkFs) OpenFile(string, int, os.FileMode) (File, error) {
	return nil, os.ErrPermission
}
func (f *fakeSymlinkFs) Remove(string) error             { return os.ErrPermission }
func (f *fakeSymlinkFs) RemoveAll(string) error          { return os.ErrPermission }
func (f *fakeSymlinkFs) Rename(string, string) error     { return os.ErrPermission }
func (f *fakeSymlinkFs) Name() string                    { return "fakeSymlinkFs" }
func (f *fakeSymlinkFs) Chmod(string, os.FileMode) error { return os.ErrPermission }
func (f *fakeSymlinkFs) Chown(string, int, int) error    { return os.ErrPermission }
func (f *fakeSymlinkFs) Chtimes(string, time.Time, time.Time) error {
	return os.ErrPermission
}

// TestSymlinkComparison verifies supported links are compared by target and
// reported as content_changed when targets differ.
func TestSymlinkComparison(t *testing.T) {
	base := newFakeSymlinkFs()
	nwe := newFakeSymlinkFs()
	base.infos["/root"] = fakeInfo{name: "root", mode: os.ModeDir | 0o755, mtime: mustTimeP(fixedTime)}
	nwe.infos["/root"] = base.infos["/root"]
	base.dirs["/root"] = map[string]struct{}{}
	nwe.dirs["/root"] = map[string]struct{}{}
	base.addLink("/root/link", "target-a")
	nwe.addLink("/root/link", "target-a")
	base.addLink("/root/other", "x")
	nwe.addLink("/root/other", "y")
	base.addFile("/root/data", "same")
	nwe.addFile("/root/data", "same")

	d := NewDirDiffer()
	diffs, err := d.DiffTrees(base, "/root", nwe, "/root")
	if err != nil {
		t.Fatal(err)
	}
	got := typesByRel(diffs)
	if _, ok := got["link"]; ok {
		t.Fatalf("equal links must not be reported: %v", diffs)
	}
	if got["other"] != DiffContentChanged {
		t.Fatalf("expected content_changed for differing link, got %v (%v)", got["other"], diffs)
	}
	if _, ok := got["data"]; ok {
		t.Fatalf("equal files must not be reported: %v", diffs)
	}
}

// TestUnsupportedSpecialNode verifies non-regular, non-dir, non-link nodes
// are surfaced explicitly instead of being silently skipped.
func TestUnsupportedSpecialNode(t *testing.T) {
	base := newFakeSymlinkFs()
	nwe := newFakeSymlinkFs()
	base.infos["/root"] = fakeInfo{name: "root", mode: os.ModeDir | 0o755, mtime: mustTimeP(fixedTime)}
	nwe.infos["/root"] = base.infos["/root"]
	base.dirs["/root"] = map[string]struct{}{}
	nwe.dirs["/root"] = map[string]struct{}{}
	base.infos["/root/fifo"] = fakeInfo{name: "fifo", mode: os.ModeNamedPipe | 0o644, mtime: mustTimeP(fixedTime)}
	nwe.infos["/root/fifo"] = base.infos["/root/fifo"]
	base.register("/root/fifo")
	nwe.register("/root/fifo")

	diffs, err := NewDirDiffer().DiffTrees(base, "/root", nwe, "/root")
	if err != nil {
		t.Fatal(err)
	}
	if got := typesByRel(diffs)["fifo"]; got != DiffUnsupported {
		t.Fatalf("expected unsupported for fifo, got %v (%v)", got, diffs)
	}
}

// listingFile provides just enough File behavior for snapshotTree: sorted
// Readdirnames and Close.
type listingFile struct {
	names []string
	pos   int
}

func (l *listingFile) Read(p []byte) (int, error)             { return 0, io.EOF }
func (l *listingFile) ReadAt(p []byte, o int64) (int, error)  { return 0, io.EOF }
func (l *listingFile) Seek(o int64, w int) (int64, error)     { return 0, nil }
func (l *listingFile) Write(p []byte) (int, error)            { return 0, os.ErrPermission }
func (l *listingFile) WriteAt(p []byte, o int64) (int, error) { return 0, os.ErrPermission }
func (l *listingFile) Name() string                           { return "" }
func (l *listingFile) Readdir(n int) ([]os.FileInfo, error)   { return nil, io.EOF }
func (l *listingFile) Readdirnames(n int) ([]string, error) {
	if l.pos >= len(l.names) {
		return nil, io.EOF
	}
	if n <= 0 || n > len(l.names)-l.pos {
		n = len(l.names) - l.pos
	}
	out := append([]string{}, l.names[l.pos:l.pos+n]...)
	l.pos += n
	return out, nil
}
func (l *listingFile) Stat() (os.FileInfo, error)        { return nil, os.ErrPermission }
func (l *listingFile) Sync() error                       { return nil }
func (l *listingFile) Truncate(n int64) error            { return os.ErrPermission }
func (l *listingFile) WriteString(s string) (int, error) { return 0, os.ErrPermission }
func (l *listingFile) Close() error                      { return nil }

// fakeRegFile is a read-only regular file handle backed by an in-memory
// buffer, enough for the comparator's streaming reads.
type fakeRegFile struct {
	*bytes.Reader
	name string
}

func (f *fakeRegFile) Name() string                       { return f.name }
func (f *fakeRegFile) Close() error                       { return nil }
func (f *fakeRegFile) Write([]byte) (int, error)          { return 0, os.ErrPermission }
func (f *fakeRegFile) WriteAt([]byte, int64) (int, error) { return 0, os.ErrPermission }
func (f *fakeRegFile) WriteString(string) (int, error)    { return 0, os.ErrPermission }
func (f *fakeRegFile) Truncate(int64) error               { return os.ErrPermission }
func (f *fakeRegFile) Sync() error                        { return nil }
func (f *fakeRegFile) Readdir(int) ([]os.FileInfo, error) {
	return nil, &os.PathError{Op: "readdir", Path: f.name, Err: errors.New("not a directory")}
}
func (f *fakeRegFile) Readdirnames(int) ([]string, error) {
	return nil, &os.PathError{Op: "readdirnames", Path: f.name, Err: errors.New("not a directory")}
}
func (f *fakeRegFile) Stat() (os.FileInfo, error) { return nil, os.ErrPermission }

// TestRootTypeConflict verifies non-directory roots are compared as a single
// node (and file/dir roots conflict).
func TestRootTypeConflict(t *testing.T) {
	base := &MemMapFs{}
	nwe := &MemMapFs{}
	putFile(t, base, "/base-root", "file")
	putFile(t, nwe, "/n-root/child", "dir")
	diffs, err := NewDirDiffer().DiffTrees(base, "/base-root", nwe, "/n-root")
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 1 || diffs[0].Type != DiffTypeConflict || diffs[0].RelPath != "" {
		t.Fatalf("expected one root type_conflict, got %v", diffs)
	}
}

// TestRootMissing verifies a missing root is returned as an error rather than
// a confusing diff.
func TestRootMissing(t *testing.T) {
	base := &MemMapFs{}
	nwe := &MemMapFs{}
	putFile(t, nwe, "/n-root/f", "x")
	if _, err := NewDirDiffer().DiffTrees(base, "/no-such-root", nwe, "/n-root"); err == nil {
		t.Fatal("expected error for missing base root")
	}
}

// TestHashOption verifies the digest function can be replaced and equal
// streams still compare equal chunk by chunk.
func TestHashOption(t *testing.T) {
	calls := 0
	d := NewDirDiffer(WithChunkSize(8), WithHash(func() hash.Hash {
		calls++
		return sha256.New()
	}))
	data := []byte("0123456789")
	ok, err := d.streamEqual(bytes.NewReader(data), bytes.NewReader(append([]byte{}, data...)))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || calls == 0 {
		t.Fatalf("custom hash not used: ok=%v calls=%d", ok, calls)
	}
}

// ExampleNewDirDiffer demonstrates printing the differences between two
// read-only Afero trees.
func ExampleNewDirDiffer() {
	base := &MemMapFs{}
	nwe := &MemMapFs{}
	putFileExample(base, "/base/keep.txt", "unchanged")
	putFileExample(base, "/base/removed.txt", "gone")
	putFileExample(nwe, "/new/keep.txt", "unchanged")
	putFileExample(nwe, "/new/added.txt", "fresh")
	putFileExample(base, "/base/edited.txt", "same-len!")
	putFileExample(nwe, "/new/edited.txt", "same-len?")
	putFileExample(base, "/base/x", "file here")
	putFileExample(nwe, "/new/x/inside.txt", "dir here")

	diffs, err := NewDirDiffer(WithChunkSize(4096)).DiffTrees(base, "/base", nwe, "/new")
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	for _, d := range diffs {
		fmt.Println(d.String())
	}
	// Output:
	// added            added.txt (file)
	// content_changed  edited.txt (file -> file, 9 -> 9 bytes)
	// missing          removed.txt (file)
	// type_conflict    x (file -> dir)
}

func putFileExample(fs Fs, name, content string) {
	_ = fs.MkdirAll(path.Dir(name), 0o755)
	_ = WriteFile(fs, name, []byte(content), 0o644)
}
